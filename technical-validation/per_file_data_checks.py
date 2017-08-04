#!/usr/bin/env python3

import glob
import os
import time
import sys
from datetime import datetime

import progressbar
from redis import Redis
from rq import Queue

from per_file_data_checks_functions import check_file

LOCAL_PATH_PREFIX = os.environ['LOCAL_PATH_PREFIX']
WORKER_PATH_PREFIX = os.environ['WORKER_PATH_PREFIX']


def update_results(results_q):
    done = 0
    while len(results_q) > 0:
        job = results_q.dequeue()
        done += 1
        if len(job.args) != 2:
            print(job.description, file=sys.stderr)
            continue
        file = job.args[0]
        fails = job.args[1]
        if fails is True:
            continue
        for fail in fails:
            print('{}: {}'.format(file, fail), file=sys.stderr)
    return done


if __name__ == '__main__':
    start_time = datetime.now()
    print("Start:", start_time)

    print("Generating file list...")
    files = glob.glob(os.path.join(LOCAL_PATH_PREFIX, 'BLOND-50/**/*.hdf5'), recursive=True)
    files += glob.glob(os.path.join(LOCAL_PATH_PREFIX, 'BLOND-250/**/*.hdf5'), recursive=True)
    files = [os.path.relpath(d, LOCAL_PATH_PREFIX) for d in files if 'summary' not in os.path.basename(d)]

    total_jobs = len(files)
    done_jobs = 0

    print("Enqueueing {} files...".format(total_jobs))
    q = Queue(connection=Redis())
    for file in files:
        q.enqueue_call(check_file, args=(file, WORKER_PATH_PREFIX), timeout=2**31 - 1)

    results_q = Queue(connection=Redis(), name='results')

    print("Processing...")
    with progressbar.ProgressBar(max_value=total_jobs, redirect_stdout=False, redirect_stderr=False) as bar:
        while True:
            done_jobs += update_results(results_q)
            bar.update(done_jobs)
            if total_jobs == done_jobs:
                break
            time.sleep(5)

    end_time = datetime.now()
    print("End:", end_time)
    print("Duration:", end_time - start_time)
