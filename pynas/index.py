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
import hashlib
import os
import datetime
import urllib2
#
# For 2.6 and later, we should have a json module
# Otherwise, try simplejson
#
try:
    import json
except ImportError:
    import simplejson as json

class Index(object):
    """
    This class is responsible for building the indexes associated
    with the NAS device.  Basically, the index is local cache of
    information about the files stored on the NAS device.

    For each file on the NAS device, we calculate the SHA1 checksum
    for the contents of that file.  We then get the hexdigeest for
    that checksum which will give us something like this:

    db1192f871cab00c6b41d73e73e375e8c52819

    A 38-digit hexadecimal string.  We then use the GIT trick of
    using the first two digits of this hash as a directory name
    and the remaining digits as the name of a file within that
    directory.  So, if our NAS index is stored in ~/.pynas we
    would see 256 directories within that main index directory
    with each directory named as a two-digit hexadecimal number:

    00
    01
    ...
    db
    ...
    fe
    ff

    Within the "db" directory we would then find a file called

    b1192f871cab00c6b41d73e73e375e8c52819

    which would represent the index entry for all files on the
    NAS device which produced the SHA1 checksum of:

    db1192f871cab00c6b41d73e73e375e8c52819

    this index file, we will find a JSON data structure
    like this:

    {"entries": [{"st_ctime": 1280887087.0, "st_mtime": 1280887087.0,
                  "st_nlink": 1,
                  "metadata_sha1": "5bb2f2835a7586ce5d093b15ebda282dabbf135f",
                  "st_gid": 501, "st_dev": 234881026, "st_size": 510,
                  "st_ino": 10654393, "st_uid": 501,
                  "path": "\/Users\/mitch\/Projects\/boto\/docs\/source\/ref\/contrib.rst",
                  "st_mode": 33188, "st_atime": 1282105485.0}]}

    Which in turn produces a Python data structure like this:

        {u'entries': [{u'metadata_sha1': u'5bb2f2835a7586ce5d093b15ebda282dabbf135f',
                       u'path': u'/Users/mitch/Projects/boto/docs/source/ref/contrib.rst',
                       u'st_atime': 1282105485.0,
                       u'st_ctime': 1280887087.0,
                       u'st_dev': 234881026,
                       u'st_gid': 501,
                       u'st_ino': 10654393,
                       u'st_mode': 33188,
                       u'st_mtime': 1280887087.0,
                       u'st_nlink': 1,
                       u'st_size': 510,
                       u'st_uid': 501}]}

    So, basically each file on the NAS device that has an SHA1 has of:

    db1192f871cab00c6b41d73e73e375e8c52819

    will be represented by a dict within the "entries" list of the JSON
    data structure.  Within that structure, we store the full path to the
    file along with a bunch of metadata about the file derived from the
    stat system call.  In addition, there is a SHA1 checksum of a number
    of the metadata entries combined together.
    
    So, the content associated with this index entry (i.e. the content that
    produced this SHA1 checksum) needs to be stored in S3.  In addition, the
    metadata in each of the elements in the entries list also needs to be stored
    somewhere (SimpleDB?  Local MySQL?  Local NoSQL DB?)

    This class is currently only creating the index for the NAS device.
    The next steps will be to upload the unique content to S3 and to
    store the metadata somewhere.  In addition, we will also want to
    hook up the "process_file" method to some sort of pynotify mechanism
    that will be notified about any new/modified/deleted files on the
    NAS file system.
    """

    def __init__(self, path='~/.pynas', recreate=False):
        """
        The constructor for the class.

        :type path: str
        :param path: The path to where the pynas index will be stored.

        """
        self.path = os.path.expanduser(path)
        self.hex_digits = '0123456789abcdef'
        self.sha1 = hashlib.sha1()
        self.create_index()

    def create_index(self):
        """
        Create the index directory structure, if it doesn't already exist.
        """
        if not os.path.isdir(self.path):
            os.mkdir(self.path)
            for d1 in list(self.hex_digits):
                for d2 in list(self.hex_digits):
                    dir_path = os.path.join(self.path, '%s%s' % (d1, d2))
                    if not os.path.isdir(dir_path):
                        os.mkdir(dir_path)

    def __iter__(self):
        for index_dir in os.listdir(self.path):
            index_dir_path = os.path.join(self.path, index_dir)
            for index_file in os.listdir(index_dir_path):
                index_file_path = os.path.join(index_dir_path, index_file)
                fp = open(index_file_path)
                d = json.load(fp)
                fp.close()
                for entry in d['entries']:
                    entry['path'] = urllib2.unquote(entry['path'])
                    yield entry['path']

    def _calculate_sha1(self, path):
        """
        Calculate the SHA1 hash for a given file.

        :type path: str
        :param path: The fully-qualified path to the file.

        :rtype: str
        :return: The hexdigest string representation of the SHA1 hash.
        """
        sha1 = self.sha1.copy()
        fp = open(path)
        chunk = fp.read()
        while chunk:
            sha1.update(chunk)
            chunk = fp.read()
        fp.close()
        return sha1.hexdigest()

    def _quote_entries(self, entries):
        for entry in entries:
            entry['path'] = urllib2.quote(entry['path'])

    def _unquote_entries(self, entries):
        for entry in entries:
            entry['path'] = urllib2.unquote(entry['path'])

    def _get_index_path(self, hash):
        prefix = hash[0:2]
        index_path = os.path.join(self.path, hash[0:2])
        index_path = os.path.join(index_path, hash[2:])
        return index_path

    def _read_index(self, index_path):
        """
        Read an index for a given path.  If no index exists,
        create a new one and return it.

        :type index_path: str
        :param index_path: Fully qualified path to the index file.

        :rtype: dict
        :return: The current index data, as a Python dictionary.
        """
        if os.path.exists(index_path):
            fp = open(index_path, 'r')
            d = json.load(fp)
            fp.close()
            self._unquote_entries(d['entries'])
        else:
            d = {'hash' : os.path.split(index_path)[-1],
                 'entries' : []}
        return d

    def _write_index(self, index_path, index):
        """
        Write the updated index information to the index file.

        :type index_path: str
        :param index_path: Fully-qualfied path to the index file.

        :type index: dict
        :param index: The new index information to be written.
        """
        fp = open(index_path, 'w')
        self._quote_entries(index['entries'])
        json.dump(index, fp)
        fp.close()
        self._unquote_entries(index['entries'])
        for entry in index['entries']:
            entry['path'] = urllib2.unquote(entry['path'])

    def _create_index_entry(self, path):
        """
        Create a new index entry for a given path.

        :type path: str
        :param path: The fully-qualified path to the file.

        :rtype: dict
        :return: The new index entry for the given path.
        """
        stats = os.stat(path)
        s = {'path' : path,
             'st_mode' : stats.st_mode,
             'st_ino' : stats.st_ino,
             'st_dev' : stats.st_dev,
             'st_nlink' : stats.st_nlink,
             'st_uid' : stats.st_uid,
             'st_gid' : stats.st_gid,
             'st_size' : stats.st_size,
             'st_atime' : stats.st_atime,
             'st_mtime' : stats.st_mtime,
             'st_ctime' : stats.st_ctime}
        sha1 = self.sha1.copy()
        for stat in ('st_mode', 'st_mode', 'st_ino', 'st_size', 'st_ctime', 'st_mtime'):
            sha1.update('%d' % getattr(stats, stat))
        s['metadata_sha1'] = sha1.hexdigest()
        return s

    def get(self, path):
        """
        Return an index entry for a given path.

        :type path: str
        :param path: Fully-qualified path to the file.
        """
        file_hash = self._calculate_sha1(path)
        index_path = self._get_index_path(file_hash)
        index = self._read_index(index_path)
        index_entry = self._create_index_entry(path)
        found = False
        for entry in index['entries']:
            if entry['metadata_sha1'] == index_entry['metadata_sha1']:
                found = True
                break
        if not found:
            index['entries'].append(index_entry)
            self._write_index(index_path, index)
        return index

    def set_value(self, path, key, value):
        """
        Set a key/value pair for a given path.

        :type path: str
        :param path: Fully-qualified path to the file.
        """
        file_hash = self._calculate_sha1(path)
        index_path = self._get_index_path(file_hash)
        index = self._get_current_index(index_path)
        index[key] = value
        self._write_index(index_path, index)

                
