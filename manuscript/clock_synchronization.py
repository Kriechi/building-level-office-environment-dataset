# run as jupyter notebook

import datetime
import h5py
import numpy as np
import matplotlib
%matplotlib inline
matplotlib.rcParams['figure.figsize'] = (10.0, 5.0)
import matplotlib.pyplot as plt

LOCAL_PATH_PREFIX = os.environ['LOCAL_PATH_PREFIX']

file = os.path.join(LOCAL_PATH_PREFIX, 'BLOND-250/2017-06-12/clear/clear-2017-06-12T11-10-55.327670T+0200-0022211.hdf5')
with h5py.File(file, 'r', driver='core') as f:
    signal_clear = f['current1'] * f['current1'].attrs['calibration_factor']
    start_clear = datetime.datetime(
        year=int(f.attrs['year']),
        month=int(f.attrs['month']),
        day=int(f.attrs['day']),
        hour=int(f.attrs['hours']),
        minute=int(f.attrs['minutes']),
        second=int(f.attrs['seconds']),
        microsecond=int(f.attrs['microseconds']),
        tzinfo=datetime.timezone(datetime.timedelta(hours=int(f.attrs['timezone'][1:4]), minutes=int(f.attrs['timezone'][4:]))),
    )

file = os.path.join(LOCAL_PATH_PREFIX, 'BLOND-250/2017-06-12/medal-1/medal-1-2017-06-12T11-10-33.862780T+0200-0022314.hdf5')
with h5py.File(file, 'r', driver='core') as f:
    signal_medal = f['current1'] * f['current1'].attrs['calibration_factor']
    signal_medal = np.repeat(signal_medal, 5)  # adapt sampling rate to fit CLEAR from 50kSps to 250kSps
    start_medal = datetime.datetime(
        year=int(f.attrs['year']),
        month=int(f.attrs['month']),
        day=int(f.attrs['day']),
        hour=int(f.attrs['hours']),
        minute=int(f.attrs['minutes']),
        second=int(f.attrs['seconds']),
        microsecond=int(f.attrs['microseconds']),
        tzinfo=datetime.timezone(datetime.timedelta(hours=int(f.attrs['timezone'][1:4]), minutes=int(f.attrs['timezone'][4:]))),
    )

length = round(0.04 * 250000)
start = round(3.14 * 250000)

f, ax = plt.subplots()
ax.plot(signal_clear[start:start + length])
start += round((start_clear - start_medal).total_seconds() * 250000)
ax.plot(signal_medal[start:start + length])

start = 3150
end = 4850

ax.axvline(x=start, color='b', label='CLEAR', linestyle='--', linewidth=1)
ax.axvline(x=end, color='b', label='MEDAL', linestyle='--', linewidth=1)

print('difference of {} samples, which equals {} ms'.format(end - start, (end - start) / 250000))
