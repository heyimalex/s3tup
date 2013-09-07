from fnmatch import fnmatch
from base64 import b64encode
import hashlib
import os
import re

class Matcher(object):
    
    def __init__(self, patterns=None, ignore_patterns=None, regexes=None,
                 ignore_regexes=None):
        self.patterns = patterns
        self.ignore_patterns = ignore_patterns
        self.regexes = regexes
        self.ignore_regexes = ignore_regexes

    def match(self, s):
        # If neither patterns nor regexes is set, match everything
        matched = self.patterns is None and self.regexes is None 

        if self.patterns is not None:
            for pattern in self.patterns:
                if fnmatch(s, pattern):
                    matched = True
                    break

        if self.regexes is not None and matched is not True:
            for regex in self.regexes:
                if re.search(regex, s):
                    matched = True
                    break

        if self.ignore_patterns is not None and matched is True:
            for pattern in self.ignore_patterns:
                if fnmatch(s, pattern):
                    return False

        if self.ignore_regexes is not None and matched is True:
            for regex in self.ignore_regexes:
                if re.search(regex, s):
                    return False

        return matched


def file_md5(filename):
    """Return md5 hash of file located at filename"""
    m = hashlib.md5()
    with open(filename,'rb') as f:
        while True:
            buf=f.read(8192)
            if not buf: break
            m.update(buf)
    return b64encode(m.digest()).strip()

def os_walk_iter(src):
    """Return list of all file paths in src relative to src"""
    for path, dirs, files in os.walk(src):
        for f in files:
            full_path = os.path.join(path, f)
            yield os.path.relpath(full_path, src)

def key_diff(before, after):
    """

    Return a dict representing the difference between two runs of Bucket.get_remote_keys(). Useful to see changes in the keys within bucket after some operation.

        before = list(bucket.get_remote_keys())
        do_something()
        diff = key_diff(before, list(bucket.get_remote_keys()))
        print diff['new'] # List of new keys
        print diff['removed'] # List of keys that have been removed
        print diff['modified'] # List of keys that have been modified
        print diff['unmodified'] # List of keys which have not changed

    Be sure to call list() on the result of get_remote_keys before running this; because it's a generator, it won't actually execute until you iterate through it (which, in the example above, would be after the 'after' you're trying to compare to)
    """
    before_set = {k['name'] for k in before}
    after_set = {k['name'] for k in after}

    before_keys = {k['name']: k for k in before}
    after_keys = {k['name']: k for k in after}

    removed = before_set - after_set
    new = after_set - before_set

    modified = set()
    unmodified = set()
    for k in (before_set & after_set):
        if before_keys[k]['etag'] == after_keys[k]['etag'] and \
           before_keys[k]['size'] == after_keys[k]['size']:
            unmodified.add(k)
        else:
            modified.add(k)
    return {'new': new, 'removed': removed, 'modified': modified,
            'unmodified': unmodified}
