#!/usr/bin/env python3

import os
import re
import shutil
import random
import subprocess
import sys
import threading
import time
import datetime
import traceback
import json
import smtplib
import h5py
import numpy
from email.mime.text import MIMEText

from natsort import natsorted
from persistent_queue import PersistentQueue

SSH_KEY_PATH = os.environ['SSH_KEY_PATH']
BASE_DIRECTORY = os.getcwd()
DESTINATION_STORAGE = os.path.abspath('storage')
DESTINATION_TEMP = os.path.abspath('tmp')
MIN_FREE_TEMP = 4 * 1024 * 1024 * 1024  # bytes
MIN_FREE_STORAGE = 4 * 1024 * 1024 * 1024  # bytes
TIMEOUT = 1800  # seconds


class Unit(object):

    def __init__(self, hostname, ip, username, transfer_speed, timeout, file_length, min_filesize, max_filesize):
        self.hostname = hostname
        self.ip = ip
        self.username = username
        self.transfer_speed = transfer_speed  # bytes per second
        self.timeout = timeout  # seconds
        self.file_length = file_length  # minutes
        self.min_filesize = min_filesize  # MB
        self.max_filesize = max_filesize  # MB


units = [
    Unit(hostname='medal-{}'.format(i + 1), ip='192.168.1.{}'.format(i + 200), username='medal', transfer_speed=6000, timeout=25, file_length=15, min_filesize=25, max_filesize=37) for i in range(15)
]
units.extend([
    Unit(hostname='clear', ip='192.168.1.222', username='clear', transfer_speed=15000, timeout=11, file_length=5, min_filesize=100, max_filesize=120),
])

UNITS = {}
for unit in units:
    UNITS[unit.hostname] = unit


def sizeof_fmt(num, suffix='B'):
    for unit in ['', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi']:
        if abs(num) < 1024.0:
            return "{:3.1f} {}{}".format(num, unit, suffix)
        num /= 1024.0
    return "{:.1f} {}{}".format(num, 'Yi', suffix)


def send_email(text, subject):
    with smtplib.SMTP() as s:
        if isinstance(text, Exception):
            text = traceback.format_exc()

        timestamp = time.strftime("%Y-%m-%dT%H:%M:%S%z", time.localtime())
        text = "{}\n\n{}".format(timestamp, text)

        msg = MIMEText(text)
        msg['From'] = 'energy-daq@BLOND.local'
        msg['To'] = 'root'
        msg['Subject'] = '[Energy-DAQ] {}'.format(subject)
        s.send_message(msg)


class BaseThread(threading.Thread):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.log_name = None

    def _log(self, message):
        for _ in range(5):
            # retry writing to stderr at least 5 times before silently giving up
            try:
                print("{:<21} | {}".format(self.log_name, message), file=sys.stderr)
            except:
                continue
            break

        for _ in range(5):
            # retry writing to log file at least 5 times before silently giving up
            try:
                os.makedirs('{}/logs'.format(DESTINATION_STORAGE), exist_ok=True)
                with open('{}/logs/{}.log'.format(DESTINATION_STORAGE, self.log_name), 'a') as f:
                    timestamp = time.strftime("%Y-%m-%dT%H:%M:%S%z", time.localtime())
                    ms = message.split("\n")
                    for m in ms:
                        f.write("{} {} {}\n".format(timestamp, self.log_name, m))
            except:
                continue
            break

    def run(self):
        self._log("{} started.".format(self.name))

        while True:
            try:
                self._process()
            except Exception as e:
                try:
                    self._log(traceback.format_exc())
                    send_email(e, 'Exception caught!')
                except:
                    pass


class CollectorThread(BaseThread):

    def __init__(self, verification_semaphore, verification_queue, statistics_semaphore, statistics_queue, hostname):
        super().__init__(name='EnergyDAQCollectorThread-{}-{}'.format(hostname, UNITS[hostname].ip))
        self.verification_semaphore = verification_semaphore
        self.verification_queue = verification_queue
        self.statistics_semaphore = statistics_semaphore
        self.statistics_queue = statistics_queue
        self.hostname = hostname
        self.log_name = 'collector-{}'.format(self.hostname)
        self.source = '{}@{}:/energy-daq/files/'.format(UNITS[self.hostname].username, UNITS[self.hostname].ip)
        self.connection_error_count = 0

    def _process(self):
        first_run = True

        while True:
            if not first_run:
                time.sleep(30 + random.randint(0, 30))
            first_run = False

            self._check_free_space()

            files = self._get_file_list()
            if not files:
                continue

            ram_files = [f for f in files if f.startswith('ram')]
            persisted_files = [f for f in files if f.startswith('persisted')]

            if len(persisted_files) > 0:
                # if there are any persisted files, download them all,
                # already persisted files are not being moved again
                self._log('Receiving {} persisted files...'.format(len(persisted_files)))
                for file in persisted_files:
                    if not self._transfer_file(file):
                        break
                    time.sleep(5)
            elif len(ram_files) > 2:
                # if there are more than 2 files in RAM, download only one and sleep,
                # the mover could kick in between files
                self._log('Receiving a single file from RAM. {} files still left...'.format(len(files)))
                self._transfer_file(ram_files[0])

                # sleep a bit extra to allow mover to do its job
                time.sleep(60 + random.randint(0, 60))
            else:
                # there are two files or less in ram, download them all,
                # the mover should not kick in because there is enough room for 5-6 files
                files_str = 'files' if len(ram_files) > 1 else 'file'
                self._log('Receiving {} {} from RAM...'.format(len(ram_files), files_str))
                for file in ram_files:
                    if not self._transfer_file(file):
                        break
                    time.sleep(5)

    def _build_ssh_command(self):
        return [
            'ssh',
            '-i', SSH_KEY_PATH,
            '-T',
            '-o', 'StrictHostKeyChecking=no',
            '-o', 'Compression=no',
        ]

    def _build_rsync_command(self):
        return [
            'rsync',
            '--times',
            '--archive',
            '--no-perms',
            '--no-group',
            '--timeout=30',
            '--bwlimit={}'.format(UNITS[self.hostname].transfer_speed),
            '-e', ' '.join(self._build_ssh_command()),
        ]

    def _get_file_list(self):
        os.makedirs(DESTINATION_TEMP, exist_ok=True)

        try:
            c = self._build_rsync_command() + [
                '--dry-run',
                '--info=NAME1',
                '--filter=include /ram',
                '--filter=include /persisted',
                '--filter=include **.hdf5',
                '--filter=exclude *',
                self.source,
                DESTINATION_TEMP,
            ]
            output = subprocess.check_output(c, stderr=subprocess.STDOUT, universal_newlines=True, timeout=15)
        except subprocess.TimeoutExpired:
            self._log('Error: Could not get file list due to timeout.')
            return
        except subprocess.CalledProcessError as e:
            if re.search('No route to host', e.output.strip()):
                self.connection_error_count += 1
                secs = (30, 60, 90, 120, 180, 240, 300)[self.connection_error_count if self.connection_error_count < 6 else -1]
                time.sleep(secs)
                return None

            self._log('Error: {}, {}'.format(
                e.returncode,
                e.output.strip()))
            return None

        self.connection_error_count = 0
        files = output.strip().split('\n')
        files = [file for file in files if file and file.endswith('.hdf5') and not file == './']
        return files

    def _transfer_file(self, file):
        try:
            # unit-2016-06-04T22-24-42.411571+0200-0000001.hdf5
            g = re.match(
                '^'
                '(?P<name>.+)-'
                '(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2})'
                'T(?P<time>.+)-'
                '(?P<sequence>\d+)'
                '.hdf5'
                '$',
                os.path.basename(file)).groupdict()
        except:
            self._log('Error: Filename not matched! Ignoring file: {}'.format(os.path.basename(file)))
            return

        output_directory = os.path.join(DESTINATION_TEMP, self.hostname)
        os.makedirs(output_directory, exist_ok=True)

        if not self._check_free_space(quick_exit=True):
            return False

        try:
            c = self._build_rsync_command() + [
                '--partial',
                '--remove-source-files',
                "{}/{}".format(self.source, file),
                output_directory,
            ]
            start = datetime.datetime.now()
            start_time = time.time()
            subprocess.check_call(c, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            time_elapsed = time.time() - start_time
            self._log('Received {} in {} seconds.'.format(file, int(round(time_elapsed))))
        except subprocess.CalledProcessError as e:
            output = "<no output>"
            if e.output:
                output = e.output.decode('UTF-8').strip()
            self._log('Error: {}, {}'.format(e.returncode, output))
            return

        src = os.path.join(output_directory, os.path.basename(file))
        dst = os.path.join(DESTINATION_STORAGE, self.hostname, g['year'], g['month'], g['day'])

        stats_item = {
            'hostname': self.hostname,
            'state': {
                'last_received_at': start,
                'last_sequence_number': g['sequence'],
                'last_file_size': os.path.getsize(src),
                'last_transfer_duration': time_elapsed,
            }
        }

        self.statistics_queue.push(stats_item)
        self.statistics_semaphore.release()

        self.verification_queue.push((self.hostname, src, dst))
        self.verification_semaphore.release()

        return True

    def _check_free_space(self, quick_exit=False):
        email_sent = False
        free = shutil.disk_usage(DESTINATION_TEMP).free
        while free <= MIN_FREE_TEMP:
            msg = "Error: No more free space on {}. Only {} left. Transferring files halted until more space is available.".format(DESTINATION_TEMP, sizeof_fmt(free))
            self._log(msg)
            if quick_exit:
                return False
            if not email_sent:
                send_email(msg, 'No more free space in tmp directory!')
                email_sent = True
            time.sleep(300)
            free = shutil.disk_usage(DESTINATION_TEMP).free
        return True


class StatisticsThread(BaseThread):

    class DateTimeEncoder(json.JSONEncoder):
        def default(self, o):
            if isinstance(o, datetime.datetime):
                return o.isoformat()

            return json.JSONEncoder.default(self, o)

    def __init__(self, statistics_semaphore, statistics_queue, verification_queue, storage_queue):
        super().__init__(name='EnergyDAQStatisticsThread')
        self.statistics_semaphore = statistics_semaphore
        self.statistics_queue = statistics_queue
        self.verification_queue = verification_queue
        self.storage_queue = storage_queue
        self.log_name = 'statistics'
        self.statistics_file = '{}/logs/statistics.json'.format(DESTINATION_STORAGE)
        self.inactive_units = []
        self.state = {}

        self._load_state()

    def _process(self):
        self._update_state()
        self._dump_statistics()
        stats = self._generate_statistics()
        self._save_statistics(stats)

    def _load_state(self):
        for hostname in UNITS.keys():
            self.state[hostname] = {
                'last_received_at': None,
                'last_sequence_number': None,
                'last_file_size': None,
                'last_transfer_duration': None,
            }

        if os.path.exists(self.statistics_file) and os.path.getsize(self.statistics_file) > 0:
            try:
                with open(self.statistics_file, 'r') as f:
                    self.state = json.load(f)
                    for hostname in self.state:
                        v = self.state[hostname]['last_received_at']
                        if v:
                            self.state[hostname]['last_received_at'] = datetime.datetime.strptime(v, "%Y-%m-%dT%H:%M:%S.%f")
            except Exception as e:
                self._log("Invalid state loaded from json. Starting with empty state.")

    def _update_state(self):
        if not self.statistics_semaphore.acquire(timeout=TIMEOUT):
            send_email("Error: No file received in the last {} minutes!".format(TIMEOUT), 'No files received recently!')
            self._log("Error: No file received in the last {} minutes!".format(TIMEOUT))
            return

        item = self.statistics_queue.pop()
        self.statistics_queue.flush()

        if item == 'none':
            return

        hostname = item['hostname']

        if int(item['state']['last_sequence_number']) != int(self.state[hostname]['last_sequence_number'] or 0) + 1:
            send_email("{}: sequence id mismatch!\nold state: {}\nnew state: {}".format(hostname, self.state[hostname], item['state']), 'ID mismatch detected!')

        self.state[hostname] = item['state']

    def _dump_statistics(self):
        with open(self.statistics_file, 'w') as f:
            json.dump(self.state, f, cls=self.DateTimeEncoder, indent=2, sort_keys=True)

    def _generate_statistics(self):
        stats = []

        timestamp = time.strftime("%Y-%m-%dT%H:%M:%S%z", time.localtime())
        stats.append("Energy DAQ - Collector Statistics")
        stats.append("Statistics last updated: {}".format(timestamp))
        stats.append("Statistics Queue: {} items unprocessed".format(self.statistics_queue.count()))
        stats.append("Verification Queue: {} items unprocessed".format(self.verification_queue.count()))
        stats.append("Storage Queue: {} items unprocessed".format(self.storage_queue.count()))
        stats.append("")

        active = [hostname for hostname in self.state.keys() if self.state[hostname]['last_received_at'] is not None and self.state[hostname]['last_received_at'] > datetime.datetime.now() - datetime.timedelta(minutes=UNITS[hostname].timeout)]
        inactive = [hostname for hostname in self.state.keys() if hostname not in active]

        stats.append("-" * 90)
        stats.append("  Hostname  |     Received At     | Sequence | File Size |  Transfer  | Time")
        stats.append("-" * 90)

        if len(inactive) > 0:
            stats.append("Active:")

        stats.extend(self._generate_list(active))

        if len(inactive) > 0:
            stats.append("")
            stats.append("")
            stats.append("Inactive:")
            stats.extend(self._generate_list(inactive))

        self._check_inactive_units(inactive, stats)

        return stats

    def _check_inactive_units(self, inactive, stats):
        inactive = [h for h in inactive if self.state[h]['last_received_at'] is not None]
        if inactive != self.inactive_units:
            if any(inactive):
                send_email("Inactive units detected:\n\n{}\n\n".format("\n".join(inactive), stats), 'Inactive units detected!')
            else:
                send_email("All units operational.\n\n{}".format("\n".join(stats)), 'Everything is fine again!')
        self.inactive_units = inactive

    def _save_statistics(self, stats):
        os.makedirs('{}/logs'.format(DESTINATION_STORAGE), exist_ok=True)
        with open('{}/logs/statistics.txt'.format(DESTINATION_STORAGE), 'w') as f:
            f.write("\n".join(stats))

    def _generate_list(self, hostnames):
        for hostname in natsorted(hostnames):
            yield "{:<11} | {:^19} |  {:^7} | {:>9} | {:>10} | {}".format(
                hostname,
                self.state[hostname]['last_received_at'].strftime("%Y-%m-%d %H:%M:%S%z") if self.state[hostname]['last_received_at'] is not None else '-',
                self.state[hostname]['last_sequence_number'] or '-',
                sizeof_fmt(self.state[hostname]['last_file_size']) if self.state[hostname]['last_file_size'] is not None else '-',
                "{:.2f} sec.".format(self.state[hostname]['last_transfer_duration']) if self.state[hostname]['last_transfer_duration'] is not None else '-',
                datetime.timedelta(minutes=(int(self.state[hostname]['last_sequence_number']) * UNITS[hostname].file_length)) if self.state[hostname]['last_sequence_number'] is not None else '-',
            )


class VerificationThread(BaseThread):

    def __init__(self, statistics_semaphore, statistics_queue, verification_semaphore, verification_queue, storage_semaphore, storage_queue):
        super().__init__(name='EnergyDAQVerificationThread')
        self.statistics_semaphore = statistics_semaphore
        self.statistics_queue = statistics_queue
        self.verification_semaphore = verification_semaphore
        self.verification_queue = verification_queue
        self.storage_semaphore = storage_semaphore
        self.storage_queue = storage_queue
        self.log_name = 'verification'

    def _process(self):
        if not self.verification_semaphore.acquire(timeout=TIMEOUT):
            send_email("Error: No file verified in the last {} minutes!".format(TIMEOUT), 'No files verified recently!')
            self._log("Error: No file verified in the last {} minutes!".format(TIMEOUT))
            return

        hostname, src, dst = self.verification_queue.peek()

        try:
            self._check_file(hostname, src)

            self.storage_queue.push((src, dst))
            self.storage_semaphore.release()

            self.verification_queue.delete()
            self.verification_queue.flush()
        except Exception as e:
            self.storage_semaphore.release()
            self._log("Verifying file {} failed: {}\n{}".format(src, traceback.format_exc()))
            send_email(e, 'Exception caught!')

        self.statistics_queue.push('none')
        self.statistics_semaphore.release()

    def _check_file(self, hostname, file):
        invalid_channels = []
        with h5py.File(file, 'r') as f:
            for name, values in f.items():
                if not self._check_dataset(name, values):
                    invalid_channels.append(name)

        if invalid_channels:
            send_email("Error: faulty values detected:\n{}\n{}".format(', '.join(invalid_channels), os.path.basename(file)), 'File contains errors!')
            self._log("Error: faulty values detected: {} in {}".format(', '.join(invalid_channels), os.path.basename(file)))
        else:
            self._log("All channels are valid in {}".format(os.path.basename(file)))

        # compare in megabyte
        s = os.path.getsize(file) / 1024 / 1024
        if s < UNITS[hostname].min_filesize:
            send_email("Warning: File seems too small!\n{}\n{} MB".format(os.path.basename(file), s), 'File seems too small!')
        if s > UNITS[hostname].max_filesize:
            send_email("Warning: File seems too large!\n{}\n{} MB".format(os.path.basename(file), s), 'File seems too large!')

    def _check_dataset(self, name, values):
        THRESHOLD = 500

        count = 0
        diff_sig = numpy.diff(values)
        idx_zeros = (diff_sig == 0).nonzero()[0]

        for i in range(idx_zeros.size - 1):
            if idx_zeros[i + 1] - idx_zeros[i] == 1:
                count += 1
                if count == THRESHOLD:
                    return False
            else:
                count = 0
        return True


class StorageThread(BaseThread):

    def __init__(self, statistics_semaphore, statistics_queue, storage_semaphore, storage_queue):
        super().__init__(name='EnergyDAQStorageThread')
        self.statistics_semaphore = statistics_semaphore
        self.statistics_queue = statistics_queue
        self.storage_semaphore = storage_semaphore
        self.storage_queue = storage_queue
        self.log_name = 'storage'

    def _process(self):
        if not self.storage_semaphore.acquire(timeout=TIMEOUT):
            send_email("Error: No file stored in the last {} minutes!".format(TIMEOUT), 'No files stored recently!')
            self._log("Error: No file stored in the last {} minutes!".format(TIMEOUT))
            return

        src, dst = self.storage_queue.peek()

        email_sent = False
        free = shutil.disk_usage(DESTINATION_STORAGE).free
        while free <= MIN_FREE_STORAGE:
            msg = "Error: No more free space on {}. Only {} left. Moving files halted until more space is available.".format(DESTINATION_STORAGE, sizeof_fmt(free))
            self._log(msg)
            if not email_sent:
                send_email(msg, 'No more free space in storage directory!')
                email_sent = True
            time.sleep(300)
            free = shutil.disk_usage(DESTINATION_STORAGE).free

        try:
            os.makedirs(dst, exist_ok=True)

            if not os.path.exists(os.path.join(dst, os.path.basename(src))):
                shutil.move(src, dst)
            else:
                self._log("File {} already exists, skipping.".format(os.path.basename(src)))

            self._log("Moved file {} to {}/".format(os.path.relpath(src, BASE_DIRECTORY), os.path.relpath(dst, BASE_DIRECTORY)))
            self.storage_queue.delete()
            self.storage_queue.flush()
        except Exception as e:
            self.storage_semaphore.release()
            self._log("Moving file {} to {}/ failed: {}\n{}".format(os.path.relpath(src, BASE_DIRECTORY), os.path.relpath(dst, BASE_DIRECTORY), traceback.format_exc()))
            send_email(e, 'Exception caught!')

        self.statistics_queue.push('none')
        self.statistics_semaphore.release()


def __main__():
    threads = []

    statistics_queue = PersistentQueue('tmp/statistics.queue')
    statistics_semaphore = threading.Semaphore(statistics_queue.count())

    verification_queue = PersistentQueue('tmp/verification.queue')
    verification_semaphore = threading.Semaphore(verification_queue.count())

    storage_queue = PersistentQueue('tmp/storage.queue')
    storage_semaphore = threading.Semaphore(storage_queue.count())

    statistics_thread = StatisticsThread(statistics_semaphore, statistics_queue, verification_queue, storage_queue)
    statistics_thread.start()
    threads.append(statistics_thread)

    verification_thread = VerificationThread(statistics_semaphore, statistics_queue, verification_semaphore, verification_queue, storage_semaphore, storage_queue)
    verification_thread.start()
    threads.append(verification_thread)

    storage_thread = StorageThread(statistics_semaphore, statistics_queue, storage_semaphore, storage_queue)
    storage_thread.start()
    threads.append(storage_thread)

    for hostname in UNITS.keys():
        thread = CollectorThread(verification_semaphore, verification_queue, statistics_semaphore, statistics_queue, hostname)
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()


if __name__ == '__main__':
    __main__()
