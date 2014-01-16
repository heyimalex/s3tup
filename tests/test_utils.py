from s3tup.utils import Matcher

def test_matcher_patterns():
    m = Matcher(patterns=['*.py', '*.md'])
    assert m.matches('test.py')
    assert m.matches('test.md')
    assert not m.matches('test.ps')
    assert not m.matches('testpy')

def test_matcher_ignore_patterns_only():
    m = Matcher(ignore_patterns=['*.py',])
    assert not m.matches('test.py')
    assert m.matches('test.md')
    assert m.matches('test.yml')

def test_matcher_regexes():
    m = Matcher(regexes=['\.py$', '\.md$'])
    assert m.matches('test.py')
    assert m.matches('test.md')
    assert not m.matches('test.ps')
    assert not m.matches('testpy')  

def test_matcher_ignore_regexes_only():
    m = Matcher(ignore_regexes=['\.py$'])
    assert not m.matches('test.py')
    assert m.matches('test.md')
    assert m.matches('test.yml')

def test_matcher_ignore_overrides():
    m = Matcher(['*.py'], ['ignore*.py'])
    assert m.matches('test.py')
    assert not m.matches('test.md')
    assert not m.matches('ignore_test.py')

def test_matcher_makes_sets():
    m = Matcher(patterns=['test', 'test'])
    assert isinstance(m.patterns, set)
    assert isinstance(m.ignore_patterns, set)
    assert len(m.patterns) == 1
    assert len(m.ignore_patterns) == 0

def test_matcher_default_matches_everything():
    m = Matcher()
    assert m.matches('test.py')
    assert m.matches('test.md')
    assert m.matches('test.ps')
    assert m.matches('testpy')