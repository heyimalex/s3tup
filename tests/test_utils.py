from StringIO import StringIO
from binascii import hexlify
from tempfile import NamedTemporaryFile

import s3tup.utils as utils

class TestMatcher:

    def test_patterns(self):
        m = utils.Matcher(patterns=['*.py', '*.md'])
        assert m.matches('test.py')
        assert m.matches('test.md')
        assert not m.matches('test.ps')
        assert not m.matches('testpy')

    def test_ignore_patterns_only(self):
        m = utils.Matcher(ignore_patterns=['*.py',])
        assert not m.matches('test.py')
        assert m.matches('test.md')
        assert m.matches('test.yml')

    def test_regexes(self):
        m = utils.Matcher(regexes=['\.py$', '\.md$'])
        assert m.matches('test.py')
        assert m.matches('test.md')
        assert not m.matches('test.ps')
        assert not m.matches('testpy')  

    def test_ignore_regexes_only(self):
        m = utils.Matcher(ignore_regexes=['\.py$'])
        assert not m.matches('test.py')
        assert m.matches('test.md')
        assert m.matches('test.yml')

    def test_ignore_overrides(self):
        m = utils.Matcher(['*.py'], ['ignore*.py'])
        assert m.matches('test.py')
        assert not m.matches('test.md')
        assert not m.matches('ignore_test.py')

    def test_makes_sets(self):
        m = utils.Matcher(patterns=['test', 'test'])
        assert isinstance(m.patterns, set)
        assert isinstance(m.ignore_patterns, set)
        assert len(m.patterns) == 1
        assert len(m.ignore_patterns) == 0

    def test_default_matches_everything(self):
        m = utils.Matcher()
        assert m.matches('test.py')
        assert m.matches('test.md')
        assert m.matches('test')

    def test_iadd(self):
        m1 = utils.Matcher(['*.py',])
        m2 = utils.Matcher(['*.md',])
        m3 = m1 + m2
        assert m3.matches('test.py')
        assert m3.matches('test.md')
        assert not m3.matches('test')

class TestFchunk:

    def setup(self):
        self.tmp = NamedTemporaryFile()

    def teardown(self):
        self.tmp.close()

    def test_evenly_divisible(self):
        self.tmp.write('01234567')
        chunks = utils.f_chunk(self.tmp, 2)
        assert len(chunks) == 4
        for r in range(4):
            assert utils.f_sizeof(chunks[r]) == 2
            assert chunks[r].read() == '{}{}'.format(r*2, r*2+1)
            chunks[r].close()
            assert chunks[r].closed

    def test_not_evenly_divisible(self):
        self.tmp.write('012345678')
        chunks = utils.f_chunk(self.tmp, 2)
        assert len(chunks) == 5
        for r in range(4):
            assert utils.f_sizeof(chunks[r]) == 2
            assert chunks[r].read() == '{}{}'.format(r*2, r*2+1)
            chunks[r].close()
            assert chunks[r].closed
        assert chunks[4].read() == '8'
        chunks[4].close()

    def test_seek_tell(self):
        self.tmp.write('01234567')
        c = utils.f_chunk(self.tmp, 4)[1]
        assert c.tell() == 0
        c.seek(0, 2)
        assert c.tell() == 4
        c.seek(0)
        assert c.tell() == 0
        c.seek(2, 1)
        assert c.tell() == 2

def test_f_decorator():
    @utils.f_decorator
    def internal(f):
        f.seek(500)
    s = StringIO('hello')
    s.seek(2, 0)
    internal(s)
    assert s.tell() == 2

def test_f_sizeof():
    s = StringIO('hello')
    s.seek(2, 0)
    assert utils.f_sizeof(s) == 5
    assert s.tell() == 2

def test_f_md5():
    s = StringIO('hello')
    s.seek(2, 0) 
    expected = '5d41402abc4b2a76b9719d911017c592'
    assert hexlify(utils.f_md5(s)) == expected
    assert s.tell() == 2