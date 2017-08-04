import glob
import os
import collections
import datetime

import matplotlib
matplotlib.use('Agg')
matplotlib.rcParams['figure.figsize'] = (10.0, 10.0)
import matplotlib.pyplot as plt
import matplotlib.mlab

import numpy as np
import scipy
import scipy.signal
import h5py

from rq import Queue
from rq import get_current_job


def calibrate_offset(f, average_frequency):
    if 'voltage' not in list(f):
        return np.zeros(f['voltage1'].shape), np.zeros(f['voltage1'].shape)

    length = len(f['voltage'])
    period_length = round(average_frequency / 50)

    remainder = divmod(length, period_length)[1]
    if remainder == 0:
        remainder = period_length
    offset = np.pad(f['voltage'][:], (0, period_length - remainder), 'constant', constant_values=0).reshape(-1, period_length).mean(axis=1)

    x = np.linspace(1, length, length // period_length, dtype=np.int)
    new_x = np.linspace(1, length, length - period_length, dtype=np.int)
    offset = scipy.interpolate.interp1d(x, offset)(new_x)
    offset = np.concatenate((np.repeat([offset[0]], period_length / 2), offset, np.repeat([offset[-1]], period_length / 2)))
    return offset, offset * 0.7


def compute_rms(f, j, seconds_per_file, average_frequency, offset_voltage, offset_current, name):
    """
    Root-Mean-Square'd values per second.

    First we square the quantity, then we calculate the mean and finally, the
    square-root of the mean of the squares.
    """

    rms = dict()
    for name in list(f):
        signal = f[name][:] * 1.0
        if name == 'voltage' and offset_voltage is not None:
            signal -= offset_voltage
        elif 'current' in name and offset_current is not None:
            signal -= offset_current
        signal = np.multiply(signal, f[name].attrs['calibration_factor'])
        signal -= np.mean(signal)
        signal = scipy.signal.medfilt(signal, 15)

        key = name.replace('current', 'current_rms').replace('voltage', 'voltage_rms')
        rms[key] = np.sqrt(np.mean(np.square(signal)[:seconds_per_file * int(average_frequency)].reshape(-1, int(average_frequency)), axis=1))
    return rms


def compute_real_power(f, j, seconds_per_file, average_frequency, offset_voltage, offset_current):
    """
    Real power is the average of instantaneous power.

    First we calculate the instantaneous power by multiplying the instantaneous
    voltage measurement by the instantaneous current measurement. We sum the
    instantaneous power measurement over a given number of samples and divide by
    that number of samples.
    """

    cs = [n for n in list(f) if 'current' in n]
    real_power = dict()

    for cs_i, _ in enumerate(cs):
        if 'voltage' in list(f):
            voltage_name = 'voltage'
        else:
            voltage_name = 'voltage{}'.format(cs_i + 1)
        voltage_signal = f[voltage_name][:] * 1.0
        if offset_voltage is not None:
            voltage_signal -= offset_voltage
        voltage_signal *= f[voltage_name].attrs['calibration_factor']
        voltage_signal -= np.mean(voltage_signal)
        voltage_signal = scipy.signal.medfilt(voltage_signal, 15)

        current_signal = f['current{}'.format(cs_i + 1)][:] * 1.0
        if offset_current is not None:
            current_signal -= offset_current
        current_signal *= f['current{}'.format(cs_i + 1)].attrs['calibration_factor']
        current_signal -= np.mean(current_signal)
        current_signal = scipy.signal.medfilt(current_signal, 15)

        real_power_name = 'real_power{}'.format(cs_i + 1)

        current = current_signal[:seconds_per_file * int(average_frequency)].reshape(-1, int(average_frequency))
        voltage = voltage_signal[:seconds_per_file * int(average_frequency)].reshape(-1, int(average_frequency))
        real_power[real_power_name] = np.mean(current * voltage, axis=1)
    return real_power


def compute_apparent_power(f, j, seconds_per_file, values):
    """
    Apparent power is the product of the voltage RMS and the current RMS.
    """

    cs = [n for n in list(f) if 'current' in n]
    apparent_power = dict()

    for cs_i, _ in enumerate(cs):
        if 'voltage' in list(f):
            voltage_name = 'voltage_rms'
        else:
            voltage_name = 'voltage_rms{}'.format(cs_i + 1)
        current_rms_name = 'current_rms{}'.format(cs_i + 1)
        apparent_power_name = 'apparent_power{}'.format(cs_i + 1)

        voltage_rms = values[voltage_name][j:j + seconds_per_file]
        current_rms = values[current_rms_name][j:j + seconds_per_file]
        apparent_power[apparent_power_name] = voltage_rms * current_rms
    return apparent_power


def compute_power_factor(f, j, seconds_per_file, values):
    """
    Power factor is the ratio of real power to apparent power.
    """

    cs = [n for n in list(f) if 'current' in n]
    power_factor = dict()

    for cs_i, _ in enumerate(cs):
        real_power_name = 'real_power{}'.format(cs_i + 1)
        apparent_power_name = 'apparent_power{}'.format(cs_i + 1)
        power_factor_name = 'power_factor{}'.format(cs_i + 1)

        real_power = values[real_power_name][j:j + seconds_per_file]
        apparent_power = values[apparent_power_name][j:j + seconds_per_file]
        power_factor[power_factor_name] = real_power / apparent_power
    return power_factor


def compute_mains_frequency(f, j, seconds_per_file, frequency, average_frequency, offset_voltage):
    """
    Mains frequency is calculated by counting zero-crossings in the voltage.

    To get a cleaner value, we take the average across all phases.
    """

    vs = [n for n in list(f) if 'voltage' in n]
    mains_freq = np.zeros((len(vs), seconds_per_file))

    for cs_i, name in enumerate(vs):
        voltage_signal = (f[name][:] * 1.0 - offset_voltage) * f[name].attrs['calibration_factor']
        voltage_signal = scipy.signal.medfilt(voltage_signal, 15)

        for i, j in enumerate(range(0, len(voltage_signal), frequency)):
            voltage_slice = voltage_signal[j:(j + frequency)]
            indices = matplotlib.mlab.find((voltage_slice[1:] >= 0) & (voltage_slice[:-1] < 0))
            crossings = [k - voltage_slice[k] / (voltage_slice[k + 1] - voltage_slice[k]) for k in indices]
            mains_freq[cs_i, i] = frequency / np.mean(np.diff(crossings)) * (average_frequency / frequency)
    return np.mean(mains_freq, axis=0)


def compute_average_frequency(start_file, end_file, files_path, files_length, seconds_per_file):
    """
    Estimate the average sampling rate per day.

    Calculate the difference between the first and last sample of a day based on
    the timestamps of the files.
    """

    with h5py.File(start_file, 'r', driver='core') as f:
        frequency = int(f.attrs['frequency'])
        start_timestamp = datetime.datetime(
            year=int(f.attrs['year']),
            month=int(f.attrs['month']),
            day=int(f.attrs['day']),
            hour=int(f.attrs['hours']),
            minute=int(f.attrs['minutes']),
            second=int(f.attrs['seconds']),
            microsecond=int(f.attrs['microseconds']),
            tzinfo=datetime.timezone(datetime.timedelta(hours=int(f.attrs['timezone'][1:4]), minutes=int(f.attrs['timezone'][4:]))),
        )

    next_folder = datetime.date(start_timestamp.year, start_timestamp.month, start_timestamp.day) + datetime.timedelta(days=1)
    next_files_path = files_path.replace(
        '/{:04d}-{:02d}-{:02d}/'.format(start_timestamp.year, start_timestamp.month, start_timestamp.day),
        '/{:04d}-{:02d}-{:02d}/'.format(next_folder.year, next_folder.month, next_folder.day))
    next_files = sorted(glob.glob(next_files_path))
    if next_files:
        end_file = next_files[0]
        duration = files_length * seconds_per_file
    else:
        end_file = end_file
        duration = (files_length - 1) * seconds_per_file

    with h5py.File(end_file, 'r', driver='core') as f:
        end_timestamp = datetime.datetime(
            year=int(f.attrs['year']),
            month=int(f.attrs['month']),
            day=int(f.attrs['day']),
            hour=int(f.attrs['hours']),
            minute=int(f.attrs['minutes']),
            second=int(f.attrs['seconds']),
            microsecond=int(f.attrs['microseconds']),
            tzinfo=datetime.timezone(datetime.timedelta(hours=int(f.attrs['timezone'][1:4]), minutes=int(f.attrs['timezone'][4:]))),
        )

    return duration / (end_timestamp - start_timestamp).total_seconds() * frequency


def make_hdf5_file(hdf5_file, year, month, day, name, values, delay_after_midnight, frequency, average_frequency):
    with h5py.File(hdf5_file, 'w', driver='core') as f:
        f.attrs.create('year', year, dtype='uint32')
        f.attrs.create('month', month, dtype='uint32')
        f.attrs.create('day', day, dtype='uint32')
        f.attrs.create('name', bytes(name, 'ASCII'))
        f.attrs.create('frequency', frequency, dtype='uint64')
        f.attrs.create('average_frequency', average_frequency, dtype='float')
        f.attrs.create('delay_after_midnight', delay_after_midnight, dtype='int32')

        for k in sorted(values.keys()):
            v = values[k]
            f.create_dataset(
                k,
                data=v,
                shape=v.shape,
                dtype='f',
                fletcher32=True,
                compression='gzip',
                compression_opts=9,
                shuffle=True,
            )


def make_plots(hdf5_file, year, month, day, name, delay_after_midnight):
    with h5py.File(hdf5_file, 'r', driver='core') as f:
        powers = [f[n][:] for n in list(f) if 'apparent_power' in n]

    max_power = np.max([np.max(p) for p in powers])
    if max_power <= 150:
        max_power = 150
    elif max_power <= 200:
        max_power = 200
    elif max_power <= 300:
        max_power = 300
    elif max_power <= 1000:
        max_power = 1000
    elif max_power <= 2000:
        max_power = 2000
    elif max_power <= 3000:
        max_power = 3000

    is_dst_affected = len(powers[0]) > 60 * 60 * 24 + 300  # longer than a full day plus a bit extra

    plt.figure()
    f, axarr = plt.subplots(len(powers), sharex=True)
    for j, power in enumerate(powers):
        power = np.pad(power, (5 - divmod(len(power), 5)[1], 0), 'constant', constant_values=0)
        time_scale = np.arange(delay_after_midnight, delay_after_midnight + len(power), 5) / (60 * 60)
        p = np.median(power.reshape(-1, 5), axis=1)
        axarr[j].plot(time_scale, p, linewidth=0.8)
        axarr[j].set_ylim(0, max_power)
        axarr[j].set_ylabel('Power #{} [W]'.format(j + 1))
    axarr[0].set_title("{} - Apparent Power - {:04d}-{:02d}-{:02d}".format(name, year, month, day))
    plt.xticks(list(range(0, 26, 1)), ['{}:00'.format(j) for j in range(0, 26, 1)], rotation='vertical')
    axarr[-1].set_xlim(0, 25.25 if is_dst_affected else 24.25)
    axarr[-1].set_xlabel('Time of the day')

    filename = "summary-{:04d}-{:02d}-{:02d}-{}.pdf".format(year, month, day, name)
    plt.savefig(os.path.join(os.path.dirname(hdf5_file), filename))
    plt.close()


def compute_one_second_data_summary(folder, path_prefix, results_folder):
    dataset_folder = folder.split('/')[0]

    files_path = os.path.expanduser(os.path.join(path_prefix, folder, '*.hdf5'))
    files = sorted(glob.glob(files_path))
    if len(files) == 0:
        raise ValueError("No files found: " + files_path)

    if folder == 'BLOND-50/2016-10-18/clear':
        # CLEAR had a brief interruption that day.
        len_files = 288
    else:
        len_files = len(files)

    with h5py.File(files[0], 'r', driver='core') as f:
        name = f.attrs['name'].decode()
        year = f.attrs['year']
        month = f.attrs['month']
        day = f.attrs['day']
        frequency = int(f.attrs['frequency'])
        length = len(f[list(f)[0]])
        seconds_per_file = length // frequency
        delay_after_midnight = int(f.attrs['hours']) * 60 * 60 + int(f.attrs['minutes']) * 60 + round(int(f.attrs['seconds']) + int(f.attrs['microseconds']) * 1e-6)

    if folder == 'BLOND-50/2016-10-18/clear':
        # CLEAR had a brief interruption that day.
        average_frequency = 49952.355
    else:
        average_frequency = compute_average_frequency(files[0], files[-1], files_path, len_files, seconds_per_file)

    seconds_per_file = length / average_frequency
    seconds_per_file = int(5 * round(float(seconds_per_file) / 5))

    j = 0
    values = collections.defaultdict(lambda: np.zeros(len_files * seconds_per_file))
    for file in files:
        try:
            with h5py.File(file, 'r', driver='core') as f:
                offset_voltage, offset_current = calibrate_offset(f, average_frequency)

                if folder == 'BLOND-50/2016-10-18/clear' and f.attrs['sequence'] == 0:
                    # CLEAR had a brief interruption that day.
                    # We need to create a gap to align the next data file correctly.
                    j += 8367

                for k, v in compute_rms(f, j, seconds_per_file, average_frequency, offset_voltage, offset_current, name).items():
                    values[k][j:j + seconds_per_file] = v

                for k, v in compute_real_power(f, j, seconds_per_file, average_frequency, offset_voltage, offset_current).items():
                    values[k][j:j + seconds_per_file] = v

                for k, v in compute_apparent_power(f, j, seconds_per_file, values).items():
                    values[k][j:j + seconds_per_file] = v

                for k, v in compute_power_factor(f, j, seconds_per_file, values).items():
                    values[k][j:j + seconds_per_file] = v

                values['mains_frequency'][j:j + seconds_per_file] = compute_mains_frequency(f, j, seconds_per_file, frequency, average_frequency, offset_voltage)
        except IOError:
            pass
        j += seconds_per_file

    filename = 'summary-{:04d}-{:02d}-{:02d}-{}.hdf5'.format(year, month, day, name)
    folder = os.path.expanduser(os.path.join(results_folder, dataset_folder, '{:04d}-{:02d}-{:02d}'.format(year, month, day), name))
    os.makedirs(folder, exist_ok=True)
    hdf5_file = os.path.join(folder, filename)

    make_hdf5_file(hdf5_file, year, month, day, name, values, delay_after_midnight, frequency, average_frequency)
    make_plots(hdf5_file, year, month, day, name, delay_after_midnight)

    job = get_current_job()
    results_q = Queue(connection=job.connection, name='results')
    results_q.enqueue(print, folder)
