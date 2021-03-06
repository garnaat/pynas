#!/usr/bin/env python2.6
"""
"""

import json
import multiprocessing
import boto
import os
import sys
import datetime
import logging
import pynas.index
import Queue

class Uploader(object):

    def __init__(self, index_path, bucket_name, num_processes=2,
                 log_file=None, log_level=logging.INFO):
        self.index_path = index_path
        self.bucket_name = bucket_name
        self.num_processes = num_processes
        if log_file:
            boto.set_file_logger('pynas-uploader', log_file, log_level)
        else:
            boto.set_stream_logger('pynas-uploader', log_level)
        self.task_queue = multiprocessing.JoinableQueue()
        self.status_queue = multiprocessing.Queue()
        self.s3 = boto.connect_s3()
        self.bucket = self.s3.lookup(self.bucket_name)
        self.index = pynas.index.Index(index_path)
        self.n_tasks = 0

    def queue_tasks(self):
        for path in self.index:
            self.task_queue.put(path)
            self.n_tasks += 1

    def get_results(self):
        bytes = 0
        for i in range(self.n_tasks):
            s = self.status_queue.get()
            bytes += int(s)
        print 'total tasks: %d' % self.n_tasks
        print 'total bytes: %d' % bytes

    def worker(self, input, output):
        while 1:
            try:
                path = input.get(True, 1)
            except Queue.Empty:
                p_name =  multiprocessing.current_process().name
                boto.log.info('%s has no more tasks' % p_name)
                break
            try:
                d = self.index.get(path)
            except IOError:
                boto.log.error('IO Error with %s' % path)
                input.task_done
                continue
            if 'upload_date' not in d:
                upload_date = datetime.datetime.utcnow()
                path = d['entries'][0]['path']
                k = self.bucket.new_key(d['hash'])
                try:
                    k.set_contents_from_filename(path)
                    boto.log.info('uploaded %s to %s' % (path, d['hash']))
                    output.put('%d' % d['entries'][0]['st_size'])
                    d['upload_date'] = upload_date.isoformat()
                    self.index.save(d)
                except:
                    boto.log.error('Unexpected error: %s', sys.exc_info()[0])
                    boto.log.error('Error processing %s' % path)
            else:
                boto.log.info('%s already uploaded' % path)
                output.put('0')
            input.task_done()
                
    def main(self):
        self.queue_tasks()
        self.start_time = datetime.datetime.now()
        for i in range(self.num_processes):
            multiprocessing.Process(target=self.worker,
                                    args=(self.task_queue, self.status_queue)).start()
        self.task_queue.join()
        self.get_results()
        self.task_queue.close()
        self.status_queue.close()
        self.end_time = datetime.datetime.now()
        print 'total time = ', (self.end_time - self.start_time)

