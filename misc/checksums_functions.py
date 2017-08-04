import os
import hashlib
import glob

from rq import Queue
from rq import get_current_job


def compute_checksum(folder, path_prefix):
    files_path = os.path.expanduser(os.path.join(path_prefix, folder, '*.*'))
    files = glob.glob(files_path)
    if len(files) == 0:
        raise ValueError("No files found: " + files_path)

    checksums = []

    for file in files:
        try:
            with open(file, 'rb') as f:
                digest = hashlib.sha512(f.read()).hexdigest()
        except IOError:
            digest = 'ERROR'
        checksums.append((digest, os.path.relpath(file, os.path.expanduser(os.path.join(path_prefix)))))

    job = get_current_job()
    results_q = Queue(connection=job.connection, name='results')
    results_q.enqueue(print, checksums)
