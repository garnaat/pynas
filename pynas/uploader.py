#!/usr/bin/env python2.6
"""
"""

import json
import multiprocessing
import boto
import os
import datetime
import pynas.index

class Uploader(object):

    def __init__(self, index_path, bucket_name, num_processes=2):
        self.index_path = index_path
        self.bucket_name = bucket_name
        self.num_processes = num_processes
        self.task_queue = multiprocessing.Queue()
        self.status_queue = multiprocessing.Queue()
        self.s3 = boto.connect_s3()
        self.bucket = self.s3.lookup(self.bucket_name)
        self.index = pynas.index.Index(index_path)

    def queue_tasks(self):
        self.n_tasks = 0
        for path in self.index:
            self.task_queue.put(path)
            self.n_tasks += 1

    def get_results(self):
        bytes = 0
        for i in range(self.n_tasks):
            s = self.status_queue.get()
            bytes += int(s)
        print 'total bytes: %d' % bytes

    def stop(self):
        for i in range(self.num_processes):
            self.task_queue.put('STOP')

    def worker(self, input, output):
        for path in iter(input.get, 'STOP'):
            d = self.index.get(path)
            if 'upload_date' not in d:
                upload_date = datetime.datetime.utcnow()
                path = d['entries'][0]['path']
                k = self.bucket.new_key(d['hash'])
                k.set_contents_from_filename(path)
                print 'uploaded %s to %s' % (path, d['hash'])
                output.put('%d' % d['entries'][0]['st_size'])
                self.index.set_value(path, 'upload_date',
                                     upload_date.isoformat())
            else:
                print '%s already uploaded' % path
                output.put('0')

    def main(self):
        self.queue_tasks()
        self.start_time = datetime.datetime.now()
        for i in range(self.num_processes):
            multiprocessing.Process(target=self.worker,
                                    args=(self.task_queue, self.status_queue)).start()
        self.get_results()
        self.stop()
        self.end_time = datetime.datetime.now()
        print 'total time = ', (self.end_time - self.start_time)

