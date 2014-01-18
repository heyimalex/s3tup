from fnmatch import fnmatch
import hashlib
import os
import re

class Matcher(object):
    """Matches strings based on patterns

    Internally the matcher object has a set of glob style patterns to match
    and to ignore, and a set of regexes to match and to ignore. Each of
    these are tested against the string 's' in the matches method. Ignore
    has the highest precedence. If both patterns and regexes are empty, the
    object assumes that the match is True (though the ignore sets can still
    overpower this assumption).

    """
    def __init__(self, patterns=None, ignore_patterns=None, regexes=None,
                 ignore_regexes=None):
        self.patterns = set(patterns or set())
        self.ignore_patterns = set(ignore_patterns or set())
        self.regexes = set(regexes or set())
        self.ignore_regexes = set(ignore_regexes or set())

    def matches(self, s):
        """Return whether this matcher matches string s"""
        # If neither patterns nor regexes is set, match everything
        matched = not self.patterns and not self.regexes
        for pattern in self.patterns:
            if fnmatch(s, pattern):
                matched = True
                break
        if not matched:
            for regex in self.regexes:
                if re.search(regex, s):
                    matched = True
                    break
        if matched:
            for pattern in self.ignore_patterns:
                if fnmatch(s, pattern):
                    return False
        if matched:
            for regex in self.ignore_regexes:
                if re.search(regex, s):
                    return False
        return matched

    def __add__(self, other):
        """Allow combining of matchers."""
        patterns = self.patterns | other.patterns
        ignore_patterns = self.ignore_patterns | other.ignore_patterns
        regexes = self.regexes | other.regexes
        ignore_regexes = self.ignore_regexes | other.ignore_regexes

        return Matcher(patterns, ignore_patterns, regexes, ignore_regexes)

    def __iadd__(self, other):
        return self.__add__(other)

def f_decorator(func):
    """Makes sure decorated function doesn't mess with file position."""
    def inner(f, *args, **kwargs):
        initial_pos = f.tell() # Get initial pos
        f.seek(0) # Rewind to beginning
        ret = func(f, *args, **kwargs) # Run decorated func
        f.seek(initial_pos) # Seek back to where it was
        return ret
    return inner

@f_decorator
def f_md5(f):
    """Return md5 hash of file like object."""
    m = hashlib.md5()
    while True:
        buf=f.read(8192)
        if not buf: break
        m.update(buf)
    return m.digest()

@f_decorator
def f_sizeof(f):
    """Return size of file like object."""
    f.seek(0, 2)
    return f.tell()

# Note: f *must* be a real file, not just file-like
# as f_chunk re-opens it using f.name
def f_chunk(f, chunk_size):
    """Return file like object split into chunk-sized file like objects"""
    class FChunk(object):
        """Mimics file interface for subset of a real file."""
        def __init__(self, f, start, size):
            self._f = open(f.name, 'rb')
            self._f.seek(start, 0)
            self._start = start
            self._end = start+size
            self._size = size

        def read(self, size=0):
            remaining = self._end - self._f.tell()
            if size <= 0 or size > remaining:
                return self._f.read(remaining)
            else:
                return self._f.read(size)

        def seek(self, offset, whence=0):
            if whence == 0:
                self._f.seek(self._start+offset, 0)
            elif whence == 1:
                self._f.seek(offset, 1)
            elif whence == 2:
                self._f.seek(self._end+offset, 0)
            else:
                raise IOError

        def tell(self):
            return self._f.tell() - self._start

        @property
        def closed(self):
            return self._f.closed

        def close(self):
            self._f.close()

    full_size = f_sizeof(f)
    num_chunks = (full_size+chunk_size-1)/chunk_size # Round up div

    chunks = []
    start = 0
    for r in range(num_chunks):
        # If last block is smaller than chunk_size
        if full_size < (start + chunk_size):
            chunk = FChunk(f, start, full_size-start)
        else:
            chunk = FChunk(f, start, chunk_size)
        chunks.append(chunk)
        start += chunk_size
    return chunks

def os_walk_relative(src):
    """Return list of all file paths in src relative to src."""
    for root, dirs, files in os.walk(src):
        for f in files:
            full_path = os.path.join(root, f)
            yield os.path.relpath(full_path, src)