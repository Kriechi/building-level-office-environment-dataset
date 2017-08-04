#!/usr/bin/env python3

import matplotlib
%matplotlib inline
matplotlib.rcParams['figure.figsize'] = (10.0, 5.0)
import matplotlib.pyplot as plt

import shutil
import numpy
import numpy as np
import h5py
import os
import glob
import traceback
import matplotlib
import matplotlib.mlab
import scipy
import scipy.signal


from per_file_data_checks_functions import *

checks = {
    'dataset_length': check_dataset_length,
    'mains_frequency': check_mains_frequency,
    'voltage_rms': check_voltage_rms,
    'voltage_values': check_voltage_values,
    'voltage_bandwidth': check_voltage_bandwidth,
    'current_rms': check_current_rms,
    'flat_regions': check_flat_regions,
}

for check_name, check_func in checks.items():
    files = glob.glob('file-checks-test-data/*.hdf5'.format(check_name), recursive=True)
    for file in files:
        with h5py.File(file, 'r', driver='core') as f:
            print(check_name, file)
            check_func(f)

    files = glob.glob('file-checks-test-data/{}/*.hdf5'.format(check_name), recursive=True)
    for file in files:
        with h5py.File(file, 'r', driver='core') as f:
            print(check_name, file)
            try:
                print(check_func(f))
                assert(False)
            except ValueError as e:
                print(e)

print('SUCCESS')

# %%
files = glob.glob('file-checks-test-data/*.hdf5')
os.makedirs('file-checks-test-data/dataset_length', exist_ok=True)
for file in files:
    shutil.copy(file, 'file-checks-test-data/dataset_length/')

files = glob.glob('file-checks-test-data/dataset_length/*.hdf5')
for file in files:
    with h5py.File(file, 'r+') as f:
        for n in list(f):
            f[n].resize(len(f[n]) - 42, axis=0)

shutil.copy(next(file for file in files if 'clear' in file), 'file-checks-test-data/dataset_length/special_clear.hdf5')
with h5py.File('file-checks-test-data/dataset_length/special_clear.hdf5', 'r+') as f:
    f.attrs['frequency'] = 1234

shutil.copy(next(file for file in files if 'medal' in file), 'file-checks-test-data/dataset_length/special_medal.hdf5')
with h5py.File('file-checks-test-data/dataset_length/special_medal.hdf5', 'r+') as f:
    f.attrs['frequency'] = 1234

# %%
files = glob.glob('file-checks-test-data/*.hdf5')
os.makedirs('file-checks-test-data/mains_frequency', exist_ok=True)
for file in files:
    shutil.copy(file, 'file-checks-test-data/mains_frequency/')

files = glob.glob('file-checks-test-data/mains_frequency/*.hdf5')
for file in files:
    with h5py.File(file, 'r+') as f:
        for n in [n for n in list(f) if 'voltage' in n]:
            length = len(f[n][:])
            x = scipy.signal.resample(f[n][:], int(length * 0.75))
            f[n][:len(x)] = x
            f[n][len(x):length] = x[:length - len(x)]

# %%
os.makedirs('file-checks-test-data/voltage_rms', exist_ok=True)
shutil.copy('file-checks-test-data/clear-2017-06-12T11-10-55.327670T+0200-0022211.hdf5', 'file-checks-test-data/voltage_rms/clear-rms.hdf5')
shutil.copy('file-checks-test-data/medal-1-2017-06-12T11-10-33.862780T+0200-0022314.hdf5', 'file-checks-test-data/voltage_rms/medal-rms.hdf5')
shutil.copy('file-checks-test-data/clear-2017-06-12T11-10-55.327670T+0200-0022211.hdf5', 'file-checks-test-data/voltage_rms/clear-mean.hdf5')
shutil.copy('file-checks-test-data/medal-1-2017-06-12T11-10-33.862780T+0200-0022314.hdf5', 'file-checks-test-data/voltage_rms/medal-mean.hdf5')
shutil.copy('file-checks-test-data/clear-2017-06-12T11-10-55.327670T+0200-0022211.hdf5', 'file-checks-test-data/voltage_rms/clear-crest.hdf5')
shutil.copy('file-checks-test-data/medal-1-2017-06-12T11-10-33.862780T+0200-0022314.hdf5', 'file-checks-test-data/voltage_rms/medal-crest.hdf5')

files = glob.glob('file-checks-test-data/voltage_rms/*-rms.hdf5')
for file in files:
    with h5py.File(file, 'r+') as f:
        for n in [n for n in list(f) if 'voltage' in n]:
            f[n][:] = np.rint(f[n][:] * 0.75)

files = glob.glob('file-checks-test-data/voltage_rms/*-mean.hdf5')
for file in files:
    with h5py.File(file, 'r+') as f:
        for n in [n for n in list(f) if 'voltage' in n]:
            f[n][:] += int(8 / f[n].attrs['calibration_factor'])

files = glob.glob('file-checks-test-data/voltage_rms/*-crest.hdf5')
for file in files:
    with h5py.File(file, 'r+') as f:
        for n in [n for n in list(f) if 'voltage' in n]:
            min = np.min(f[n][:])
            max = np.max(f[n][:])
            signs = f[n][:]
            x = f[n][:].astype('i4') * max / 3
            x = np.copysign(x, signs)
            f[n][:] = np.clip(x, min * 0.7, max * 0.7)

# %%
os.makedirs('file-checks-test-data/voltage_values', exist_ok=True)
shutil.copy('file-checks-test-data/BLOND-50-clear-2016-10-02T00-02-44.043307T+0200-0000443.hdf5', 'file-checks-test-data/voltage_values/clear-bandwidth.hdf5')
shutil.copy('file-checks-test-data/BLOND-50-medal-1-2016-10-02T00-02-09.962358T+0200-0000148.hdf5', 'file-checks-test-data/voltage_values/medal-bandwidth.hdf5')
files = glob.glob('file-checks-test-data/voltage_values/*.hdf5')
for file in files:
    with h5py.File(file, 'r+') as f:
        for n in [n for n in list(f) if 'voltage' in n]:
            f[n][:] = np.floor_divide(f[n][:], 2) * 2

# %%
os.makedirs('file-checks-test-data/voltage_bandwidth', exist_ok=True)
shutil.copy('file-checks-test-data/BLOND-50-clear-2016-10-02T00-02-44.043307T+0200-0000443.hdf5', 'file-checks-test-data/voltage_bandwidth/clear-bandwidth.hdf5')
shutil.copy('file-checks-test-data/BLOND-50-medal-1-2016-10-02T00-02-09.962358T+0200-0000148.hdf5', 'file-checks-test-data/voltage_bandwidth/medal-bandwidth.hdf5')
shutil.copy('file-checks-test-data/BLOND-50-clear-2016-10-02T00-02-44.043307T+0200-0000443.hdf5', 'file-checks-test-data/voltage_bandwidth/clear-min.hdf5')
shutil.copy('file-checks-test-data/BLOND-50-medal-1-2016-10-02T00-02-09.962358T+0200-0000148.hdf5', 'file-checks-test-data/voltage_bandwidth/medal-min.hdf5')
shutil.copy('file-checks-test-data/BLOND-50-clear-2016-10-02T00-02-44.043307T+0200-0000443.hdf5', 'file-checks-test-data/voltage_bandwidth/clear-max.hdf5')
shutil.copy('file-checks-test-data/BLOND-50-medal-1-2016-10-02T00-02-09.962358T+0200-0000148.hdf5', 'file-checks-test-data/voltage_bandwidth/medal-max.hdf5')

files = glob.glob('file-checks-test-data/voltage_bandwidth/*-bandwidth.hdf5')
for file in files:
    with h5py.File(file, 'r+') as f:
        for n in [n for n in list(f) if 'voltage' in n]:
            f[n][:] = np.rint(f[n][:] * 0.5)

files = glob.glob('file-checks-test-data/voltage_bandwidth/*-min.hdf5')
for file in files:
    with h5py.File(file, 'r+') as f:
        for n in [n for n in list(f) if 'voltage' in n]:
            max = np.max(f[n][:])
            f[n][:] = np.clip(f[n][:].astype('i4') * 2, -2**15 - 1, max)

files = glob.glob('file-checks-test-data/voltage_bandwidth/*-max.hdf5')
for file in files:
    with h5py.File(file, 'r+') as f:
        for n in [n for n in list(f) if 'voltage' in n]:
            min = np.min(f[n][:])
            f[n][:] = np.clip(f[n][:].astype('i4') * 2, min, 2**15 - 1)


# %%
os.makedirs('file-checks-test-data/current_rms', exist_ok=True)
shutil.copy('file-checks-test-data/clear-2017-06-12T11-10-55.327670T+0200-0022211.hdf5', 'file-checks-test-data/current_rms/clear-rms.hdf5')
shutil.copy('file-checks-test-data/medal-1-2017-06-12T11-10-33.862780T+0200-0022314.hdf5', 'file-checks-test-data/current_rms/medal-rms.hdf5')
shutil.copy('file-checks-test-data/clear-2017-06-12T11-10-55.327670T+0200-0022211.hdf5', 'file-checks-test-data/current_rms/clear-mean.hdf5')
shutil.copy('file-checks-test-data/medal-1-2017-06-12T11-10-33.862780T+0200-0022314.hdf5', 'file-checks-test-data/current_rms/medal-mean.hdf5')
shutil.copy('file-checks-test-data/clear-2017-06-12T11-10-55.327670T+0200-0022211.hdf5', 'file-checks-test-data/current_rms/clear-crest.hdf5')
shutil.copy('file-checks-test-data/medal-1-2017-06-12T11-10-33.862780T+0200-0022314.hdf5', 'file-checks-test-data/current_rms/medal-crest.hdf5')

files = glob.glob('file-checks-test-data/current_rms/*-rms.hdf5')
for file in files:
    with h5py.File(file, 'r+') as f:
        for n in [n for n in list(f) if 'current' in n]:
            f[n][:] = np.rint(np.clip(f[n][:].astype('i4') * 10, -2**15 - 1, 2**15 - 1))

files = glob.glob('file-checks-test-data/current_rms/*-mean.hdf5')
for file in files:
    with h5py.File(file, 'r+') as f:
        for n in [n for n in list(f) if 'current' in n]:
            f[n][:] += int(2 / f[n].attrs['calibration_factor'])

files = glob.glob('file-checks-test-data/current_rms/*-crest.hdf5')
for file in files:
    with h5py.File(file, 'r+') as f:
        frequency = f.attrs['frequency']
        for n in [n for n in list(f) if 'current' in n]:
            s = f[n][:] * f[n].attrs['calibration_factor']
            rms = numpy.max(numpy.sqrt(numpy.mean(numpy.square(s).reshape(-1, frequency), axis=1)))
            f[n][:] = np.rint(np.clip(f[n][:].astype('f4') * 1.5, -rms / f[n].attrs['calibration_factor'], rms / f[n].attrs['calibration_factor']))

# %%
os.makedirs('file-checks-test-data/flat_regions', exist_ok=True)
shutil.copy('file-checks-test-data/BLOND-50-clear-2016-10-02T00-02-44.043307T+0200-0000443.hdf5', 'file-checks-test-data/flat_regions/')
shutil.copy('file-checks-test-data/BLOND-50-medal-1-2016-10-02T00-02-09.962358T+0200-0000148.hdf5', 'file-checks-test-data/flat_regions/')

files = glob.glob('file-checks-test-data/flat_regions/*.hdf5')
for file in files:
    with h5py.File(file, 'r+') as f:
        for n in list(f):
            length = len(f[n])
            f[n][int(length / 2):int(length / 2 + frequency * 2)] = 2000
