# Copyright (c) 2010 Mitch Garnaat http://garnaat.org/
# All rights reserved.
#
# Permission is hereby granted, free of charge, to any person obtaining a
# copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish, dis-
# tribute, sublicense, and/or sell copies of the Software, and to permit
# persons to whom the Software is furnished to do so, subject to the fol-
# lowing conditions:
#
# The above copyright notice and this permission notice shall be included
# in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
# OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABIL-
# ITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT
# SHALL THE AUTHOR BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, 
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
# IN THE SOFTWARE.
import os
import datetime
import urllib2
import pynas.index

class Indexer(object):
    
    def __init__(self, path, index_path,
                 ignore_dirs=['.svn', '.git', '.pynas']):
        """
        The constructor for the class.

        :type path: str
        :param path: The path that will be indexed.

        :type index_path: str
        :param index_path: The path to where the nasbox index will be stored.

        :type ignore_dirs: list
        :param ingore_dirs: A list of directory names that will be ignored
                            by the indexing process.
        """
        self.path = path
        self.index_path = index_path
        self.ignore_dirs = ignore_dirs
        self.index = pynas.index.Index(self.index_path)
        self.n_files = 0
        self.start_time = None
        self.end_time = None

    def main(self):
        self.start_time = datetime.datetime.now()
        if os.path.isdir(self.path):
            for root, dirs, files in os.walk(self.path):
                for ignore in self.ignore_dirs:
                    if ignore in dirs:
                        dirs.remove(ignore)
                for file in files:
                    fullpath = os.path.join(root, file)
                    d = self.index.get(fullpath)
                    self.n_files += 1
        elif os.path.isfile(self.path):
            d = self.index.get(self.path)
            self.n_files += 1
        self.end_time = datetime.datetime.now()
        delta = self.end_time - self.start_time
        print 'Processed Files Processed: %d' % self.n_files
        print 'Total Processing Time: %s' % delta
                
