#!/usr/bin/env python3

import os
import glob
import h5py
import numpy as np

LOCAL_PATH_PREFIX = os.environ['LOCAL_PATH_PREFIX']

datasets = [
    ('BLOND-50', 50000, 6400),
    ('BLOND-250', 250000, 50000),
]

for name, clear_freq, medal_freq in datasets:
    average_frequencies = {}
    files = glob.glob(os.path.join(LOCAL_PATH_PREFIX, name, '*/*/*.hdf5'), recursive=True)
    files = [f for f in files if 'summary' not in os.path.basename(d)]
    for file in sorted(files):
        with h5py.File(file, 'r') as f:
            average_frequencies[os.path.basename(file)] = f.attrs['average_frequency']

    if '2016-10-18-clear.hdf5' in average_frequencies:
        # CLEAR had a brief interruption on 2017-10-18, so we remove this outlier.
        del average_frequencies['2016-10-18-clear.hdf5']

    units = [
        ([f for n, f in average_frequencies.items() if 'clear' in n], clear_freq),
        ([f for n, f in average_frequencies.items() if 'medal' in n], medal_freq),
    ]

    for freqs, nominal in units:
        std = np.std(freqs)
        var = np.var(freqs)
        min = np.min(freqs)
        mean = np.mean(freqs)
        max = np.max(freqs)
        offset = [abs(max - mean), abs(min - mean)]
        print("{}: nominal sampling rate of \\SI{{{}}}{{Sps}}, standard deviation: {:.3f}, variance: {:.5f}, minimum: {:.3f}, mean: {:.3f}, maximum: {:.3f}, largest offset from mean: {:.4f}\\,\\%.".format(
            name,
            nominal,
            std,
            var,
            min,
            mean,
            max,
            np.amax(offset) / nominal * 100.0,
        ))
