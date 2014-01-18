from StringIO import StringIO
from binascii import hexlify
from tempfile import NamedTemporaryFile

import s3tup.utils as utils

def test_matcher_patterns():
    m = utils.Matcher(patterns=['*.py', '*.md'])
    assert m.matches('test.py')
    assert m.matches('test.md')
    assert not m.matches('test.ps')
    assert not m.matches('testpy')

def test_matcher_ignore_patterns_only():
    m = utils.Matcher(ignore_patterns=['*.py',])
    assert not m.matches('test.py')
    assert m.matches('test.md')
    assert m.matches('test.yml')

def test_matcher_regexes():
    m = utils.Matcher(regexes=['\.py$', '\.md$'])
    assert m.matches('test.py')
    assert m.matches('test.md')
    assert not m.matches('test.ps')
    assert not m.matches('testpy')  

def test_matcher_ignore_regexes_only():
    m = utils.Matcher(ignore_regexes=['\.py$'])
    assert not m.matches('test.py')
    assert m.matches('test.md')
    assert m.matches('test.yml')

def test_matcher_ignore_overrides():
    m = utils.Matcher(['*.py'], ['ignore*.py'])
    assert m.matches('test.py')
    assert not m.matches('test.md')
    assert not m.matches('ignore_test.py')

def test_matcher_makes_sets():
    m = utils.Matcher(patterns=['test', 'test'])
    assert isinstance(m.patterns, set)
    assert isinstance(m.ignore_patterns, set)
    assert len(m.patterns) == 1
    assert len(m.ignore_patterns) == 0

def test_matcher_default_matches_everything():
    m = utils.Matcher()
    assert m.matches('test.py')
    assert m.matches('test.md')
    assert m.matches('test.ps')
    assert m.matches('testpy')

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

def test_f_chunk_even():
    f = NamedTemporaryFile()
    f.write('01234567')
    chunks = utils.f_chunk(f, 2)
    assert len(chunks) == 4
    for r in range(4):
        assert utils.f_sizeof(chunks[r]) == 2
        assert chunks[r].read() == '{}{}'.format(r*2, r*2+1)
        chunks[r].close()
        assert chunks[r].closed
    f.close()

def test_f_chunk_not_odd():
    f = NamedTemporaryFile()
    f.write('012345678')
    chunks = utils.f_chunk(f, 2)
    assert len(chunks) == 5
    for r in range(4):
        assert utils.f_sizeof(chunks[r]) == 2
        assert chunks[r].read() == '{}{}'.format(r*2, r*2+1)
        chunks[r].close()
        assert chunks[r].closed
    assert chunks[4].read() == '8'
    chunks[4].close()
    f.close()

def test_f_chunk_seek_tell():
    f = NamedTemporaryFile()
    f.write('01234567')
    c = utils.f_chunk(f, 4)[1]
    assert c.tell() == 0
    c.seek(0, 2)
    assert c.tell() == 4
    c.seek(0)
    assert c.tell() == 0
    c.seek(2, 1)
    assert c.tell() == 2
    f.close()
