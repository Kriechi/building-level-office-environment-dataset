#!/usr/bin/env python3

import glob
import os
import time
from datetime import datetime

import progressbar
from redis import Redis
from rq import Queue

from checksums_functions import compute_checksum

RESULTS = os.path.join(os.environ['RESULTS'], 'checksums.txt')
LOCAL_PATH_PREFIX = os.environ['LOCAL_PATH_PREFIX']
WORKER_PATH_PREFIX = os.environ['WORKER_PATH_PREFIX']


def update_results(results_q):
    done = 0
    with open(RESULTS, 'a') as f:
        while len(results_q) > 0:
            job = results_q.dequeue()
            done += 1
            for digest, file in job.args[0]:
                print('{} {}'.format(digest, file), file=f)
    return done


if __name__ == '__main__':
    start_time = datetime.now()
    print("Start:", start_time)

    folders = glob.glob(os.path.join(LOCAL_PATH_PREFIX, 'BLOND-50/*/*'), recursive=True)
    folders += glob.glob(os.path.join(LOCAL_PATH_PREFIX, 'BLOND-250/*/*'), recursive=True)
    folders = [os.path.relpath(d, LOCAL_PATH_PREFIX) for d in folders]

    try:
        os.remove(RESULTS)
    except:
        pass

    total_jobs = len(folders)
    done_jobs = 0

    print("Enqueueing {} folders...".format(total_jobs))
    q = Queue(connection=Redis())
    for folder in folders:
        q.enqueue_call(compute_checksum, args=(folder, WORKER_PATH_PREFIX), timeout=2**31 - 1)

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
