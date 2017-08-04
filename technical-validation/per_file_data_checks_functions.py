import os
import traceback
import sys

import h5py
import numpy
import numpy as np

from rq import Queue
from rq import get_current_job


def check_dataset_length(f):
    frequency = int(f.attrs['frequency'])
    if 'clear' in f.attrs['name'].decode():
        if frequency == 50000:
            # BLOND-50: CLEAR uses 5min @ 50kHz files
            expected_length = 5 * 60 * 50000
        elif frequency == 250000:
            # BLOND-250: CLEAR uses 2min @ 250kHz files
            expected_length = 2 * 60 * 250000
        else:
            raise ValueError('Dataset length unknown: {}'.format(frequency))
    else:
        if frequency == 6400:
            # BLOND-50: MEDAL uses 15min @ 6400Hz files
            expected_length = 15 * 60 * 6400
        elif frequency == 50000:
            # BLOND-250: MEDAL uses 2min @ 50000 files
            expected_length = 2 * 60 * 50000
        else:
            raise ValueError('Dataset length unknown: {}'.format(frequency))

    for name in list(f):
        if len(f[name]) != expected_length:
            raise ValueError('{}: Dataset length is {}, expected to be {}'.format(name, len(f[name]), expected_length))


def check_mains_frequency(f):
    dsets = [n for n in list(f) if 'voltage' in n]
    for name in dsets:
        frequency = int(f.attrs['frequency'])
        voltage_signal = f[name][:] * f[name].attrs['calibration_factor']
        w = numpy.hamming(len(voltage_signal))
        sp = numpy.fft.rfft((voltage_signal * w).reshape(-1, frequency * 10))
        freq_bins = np.fft.rfftfreq(frequency * 10, d=1 / frequency)
        mains_freqs = freq_bins[numpy.argmax(abs(sp), axis=1)]

        if not all(mains_freq >= 49.0 and mains_freq <= 51.0 for mains_freq in mains_freqs):
            raise ValueError('{}: Mains frequency is {} Hz, expected to be >= 49Hz and <= 51Hz'.format(name, np.mean(mains_freqs)))


def check_voltage_rms(f):
    dsets = [n for n in list(f) if 'voltage' in n]
    for name in dsets:
        s = f[name][:] * f[name].attrs['calibration_factor']
        rms = numpy.sqrt(numpy.mean(numpy.square(s)))
        mean = abs(numpy.mean(s))
        crest_factor = numpy.percentile(abs(s), 99) / rms
        if not (rms >= 210 and rms <= 240):
            raise ValueError('{}: RMS is {}, expected to be >= 210 and <= 240'.format(name, rms))
        if not (mean <= 5):
            raise ValueError('{}: mean is {}, expected to be <= 5'.format(name, mean))
        if not (crest_factor >= 1.2 and crest_factor <= 1.6):
            raise ValueError('{}: crest factor is {}, expected to be >= 1.2 and <= 1.6'.format(name, crest_factor))


def check_voltage_values(f):
    if 'clear' in f.attrs['name'].decode():
        # CLEAR uses 16-bit signed integers
        threshold = 50000
    else:
        # MEDAL uses 12-bit unsigned integers with DC-offset
        threshold = 2000

    dsets = [n for n in list(f) if 'voltage' in n]
    for name in dsets:
        s = f[name][:]
        used_values = len(numpy.unique(s))
        if not used_values >= threshold:
            raise ValueError('{}: used values is {}, expected to be >= {}'.format(name, used_values, threshold))


def check_voltage_bandwidth(f):
    if 'clear' in f.attrs['name'].decode():
        # CLEAR uses 16-bit signed integers
        threshold = 80
        bits = 16
    else:
        # MEDAL uses 12-bit unsigned integers with DC-offset
        threshold = 50
        bits = 12

    dsets = [n for n in list(f) if 'voltage' in n]
    for name in dsets:
        s = f[name][:]
        calibration_factor = f[name].attrs['calibration_factor']
        bandwidth = (abs(int(numpy.max(s))) + abs(int(numpy.min(s)))) / (2**bits - 1) * 100
        if not bandwidth >= threshold:
            raise ValueError('{}: bandwidth is {}%, expected to be >= {}%'.format(name, bandwidth, threshold))
        min_value = numpy.percentile(np.clip(s * calibration_factor, -999, 0), 1)
        if not (min_value < -300 and min_value > -355):
            raise ValueError('{}: min value is {}, expected to be between -355 and -300'.format(name, min_value))
        max_value = numpy.percentile(np.clip(s * calibration_factor, 0, 999), 99)
        if not (max_value > 300 and max_value < 355):
            raise ValueError('{}: max value is {}, expected to be between 300 and 355'.format(name, max_value))


def check_current_rms(f):
    if 'clear' in f.attrs['name'].decode():
        # CLEAR uses LEM HAL50-S current sensors
        threshold = 20
    else:
        # MEDAL uses ACS712-5B and ACS712-30A with a 16A mains fuse
        threshold = 16

    frequency = int(f.attrs['frequency'])
    dsets = [n for n in list(f) if 'current' in n]
    for name in dsets:
        s = f[name][:] * f[name].attrs['calibration_factor']
        rms = numpy.max(numpy.sqrt(numpy.mean(numpy.square(s).reshape(-1, frequency), axis=1)))
        mean = abs(numpy.mean(s))
        crest_factor = numpy.max(abs(s)) / rms
        if not (rms <= threshold):
            raise ValueError('{}: RMS is {}, expected to be <= {}'.format(name, rms, threshold))
        if not (mean <= 1):
            raise ValueError('{}: mean is {}, expected to be <= 1'.format(name, mean))
        if not (crest_factor >= 1.2):
            raise ValueError('{}: crest factor is {}, expected to be >= 1.2'.format(name, crest_factor))


def check_flat_regions(f):
    frequency = int(f.attrs['frequency'])
    for name in list(f):
        s = f[name][:]
        step = int(frequency / 50)
        for x in range(0, len(s), step):
            value = np.sum(np.abs(np.diff(s[x:x + step])))
            if value == 0:
                raise ValueError('{}: flat region found at index: {}'.format(name, step))


def check_file(file, path_prefix):
    fails = []
    checks = [
        check_dataset_length,
        check_mains_frequency,
        check_voltage_rms,
        check_voltage_values,
        check_voltage_bandwidth,
        check_current_rms,
        check_flat_regions,
    ]

    try:
        local_file = os.path.expanduser(os.path.join(path_prefix, file))
        with h5py.File(local_file, 'r', driver='core') as f:
            for check in checks:
                try:
                    check(f)
                except ValueError as e:
                    fails.append(e)
                except Exception as e:
                    fails.append('{} | {} | {}'.format(repr(e), traceback.format_exc(), traceback.format_stack()))
    except IOError as e:
        fails.append(ValueError(repr(e)))

    job = get_current_job()
    results_q = Queue(connection=job.connection, name='results')

    if fails:
        if job:
            results_q.enqueue(print, file, fails)
        else:
            for file, fail in fails:
                print(repr(fail), file=sys.stderr)
    else:
        results_q.enqueue(print, file, True)
