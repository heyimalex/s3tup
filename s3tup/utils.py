from fnmatch import fnmatch
from base64 import b64encode
import os
import re
import hashlib
import binascii

from bs4 import BeautifulSoup

class Matcher(object):
    
    def __init__(self, patterns=None, ignore_patterns=None, regexes=None, ignore_regexes=None):
        self.patterns = patterns
        self.ignore_patterns = ignore_patterns
        self.regexes = regexes
        self.ignore_regexes = ignore_regexes

    def match(self, s):
        matched = True if self.patterns is None and self.regexes is None else False

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
    m = hashlib.md5()
    with open(filename,'rb') as f:
        while True:
            buf=f.read(8192)
            if not buf: break
            m.update(buf)
    return b64encode(m.digest()).strip()

def os_walk_iter(src):
    for path, dirs, files in os.walk(src):
        for f in files:
            full_path = os.path.join(path, f)
            yield os.path.relpath(full_path, src)