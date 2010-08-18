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

class NASIndexer(object):
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
    directory.  So, if our NAS index is stored in ~/.nasbox we
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

    def __init__(self, path, index_path,
                 ignore_dirs=['.svn', '.git', '.nasbox']):
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
        self.hex_digits = '0123456789abcdef'
        self.sha1 = hashlib.sha1()
        self.n_files = 0
        self.start_time = None
        self.end_time = None
        

    def calculate_sha1(self, path):
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

    def get_current_index(self, index_path):
        """
        Returns the current index data for a given path.

        :type index_path: str
        :param index_path: Fully qualified path to the index file.

        :rtype: dict
        :return: The current index data, as a Python dictionary.
        """
        if os.path.exists(index_path):
            fp = open(index_path, 'r')
            d = json.load(fp)
            fp.close()
        else:
            d = {'entries' : []}
        return d

    def create_index_entry(self, path):
        """
        Create a new index entry for a given path.

        :type path: str
        :param path: The fully-qualified path to the file.

        :rtype: dict
        :return: The new index entry for the given path.
        """
        stats = os.stat(path)
        s = {'path' : urllib2.quote(path),
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

    def write_index(self, index_path, index):
        """
        Write the updated index information to the index file.

        :type index_path: str
        :param index_path: Fully-qualfied path to the index file.

        :type index: dict
        :param index: The new index information to be written.
        """
        fp = open(index_path, 'w')
        json.dump(index, fp)
        fp.close()

    def process_file(self, path):
        """
        Process a file.

        :type path: str
        :param path: Fully-qualified path to the file to be processed.
        """
        self.n_files += 1
        file_hash = self.calculate_sha1(path)
        prefix = file_hash[0:2]
        index_path = os.path.join(self.index_path, prefix)
        index_path = os.path.join(index_path, file_hash[2:])
        print 'Processing %s - %s' % (path, index_path)
        index = self.get_current_index(index_path)
        index_entry = self.create_index_entry(path)
        found = False
        for entry in index['entries']:
            if entry['metadata_sha1'] == index_entry['metadata_sha1']:
                print 'Already indexed %s' % path
                found = True
                break
        if not found:
            index['entries'].append(index_entry)
        self.write_index(index_path, index)

    def create_index(self):
        """
        Create the index directory structure, if it doesn't already exist.
        """
        if not os.path.isdir(self.index_path):
            os.mkdir(self.index_path)
            for d1 in list(self.hex_digits):
                for d2 in list(self.hex_digits):
                    dir_path = os.path.join(self.index_path, '%s%s' % (d1, d2))
                    if not os.path.isdir(dir_path):
                        os.mkdir(dir_path)

    def main(self):
        self.create_index()
        self.start_time = datetime.datetime.now()
        if os.path.isdir(self.path):
            for root, dirs, files in os.walk(self.path):
                for ignore in self.ignore_dirs:
                    if ignore in dirs:
                        dirs.remove(ignore)
                for file in files:
                    fullpath = os.path.join(root, file)
                    self.process_file(fullpath)
        elif os.path.isfile(self.path):
            self.process_file(self.path)
        self.end_time = datetime.datetime.now()
        delta = self.end_time - self.start_time
        print 'Processed Files Processed: %d' % self.n_files
        print 'Total Processing Time: %s' % delta
                
