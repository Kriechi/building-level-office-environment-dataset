#!/usr/bin/env python3

import os
import glob
import multiprocessing
import time

jobs = dict()


def convert(local_file):
    import converter
    frequency = 50000 if 'medal' in local_file else 250000
    c = converter.Converter(local_file, frequency)
    c.start()
    return local_file


def cleanup(result):
    del jobs[result]
    print("cleanup: {}".format(result))


def main():
    pool = multiprocessing.Pool(os.cpu_count() - 1)
    while True:
        for f in glob.glob(os.path.expanduser('/energy-daq/tmp/*.bin')):
            if f not in jobs.keys():
                print("activating: {}".format(f))
                jobs[f] = 'running'
                pool.apply_async(convert, (f,), callback=cleanup)
        print("sleeping...")
        time.sleep(10)
    pool.close()


if __name__ == '__main__':
    main()
