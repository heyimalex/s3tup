from nose.tools import raises

from s3tup import parse
from s3tup.exception import ConfigParseError
from s3tup.rsync import RsyncConfig

# parse_config

@raises(ConfigParseError)
def test_parse_config_non_list_config():
    parse.parse_config('invalid')

def test_parse_config_success():
    bucket_config = {'bucket': 'test'}
    config = [bucket_config for r in range(10)]
    buckets = parse.parse_config(config)
    assert len(buckets) == 10
    assert buckets[0].name == 'test'

# parse_bucket

@raises(ConfigParseError)
def test_parse_bucket_invalid_kwarg():
    b = {'bucket': 'test', 'invalid': 'kwarg'}
    parse.parse_bucket(b)

@raises(ConfigParseError)
def test_parse_bucket_no_name_provided():
    parse.parse_bucket({})

def test_parse_bucket_success():
    b = {'bucket': 'test', 'region': 'test'}
    bucket = parse.parse_bucket(b)
    assert bucket.name == 'test'
    assert bucket.region == 'test'

# parse_rsync

def test_parse_rsync_string_input():
    r = parse.parse_rsync('test')
    assert len(r.configs) == 1
    assert isinstance(r.configs[0], RsyncConfig)
    assert r.configs[0].src == 'test'

def test_parse_rsync_dict_input():
    d = {'src': 'test', 'delete': True}
    r = parse.parse_rsync(d)
    assert len(r.configs) == 1
    assert isinstance(r.configs[0], RsyncConfig)
    assert r.configs[0].src == 'test'
    assert r.configs[0].delete

def test_parse_rsync_list_input():
    d = {'src': 'test'}
    l = [d for r in range(5)]
    r = parse.parse_rsync(l)
    assert len(r.configs) == 5
    assert isinstance(r.configs[0], RsyncConfig)
    assert r.configs[0].src == 'test'
    assert not r.configs[0].delete

# parse_rsync_object

@raises(ConfigParseError)
def test_parse_rsync_object_invalid_kwarg():
    d = {'src': 'test', 'invalid': True}
    parse.parse_rsync_object(d)

def test_parse_rsync_object_success():
    d = {'src': 'test', 'delete': True, 'patterns': ['test',]}
    rs = parse.parse_rsync_object(d)
    assert rs.src == 'test'
    assert rs.delete
    assert 'test' in rs.matcher.patterns

# parse_key_config

@raises(ConfigParseError)
def test_non_list_config():
    parse.parse_key_config({'invalid': 'invalid'})

def test_success():
    c = {'patterns': ['*.py'], 'reduced_redundancy': True}
    key_config = [c.copy() for r in range(10)]
    factory = parse.parse_key_config(key_config)
    k = factory.make_key(None, None, 'test')
    assert not k.reduced_redundancy
    k = factory.make_key(None, None, 'test.py')
    assert k.reduced_redundancy

# parse_key_configurator

@raises(ConfigParseError)
def test_parse_key_configurator_invalid_kwarg():
    parse.parse_key_configurator({'invalid': 'invalid'})

def test_parse_key_configurator_success():
    c = {'patterns': ['*.py'], 'reduced_redundancy': True}
    configurator = parse.parse_key_configurator(c)
    assert configurator.effects_key('test.py')
    assert not configurator.effects_key('test.md')
    assert configurator.reduced_redundancy

# extract_matcher

def test_extract_matcher_all_none():
    matcher = parse.extract_matcher({})[0]
    assert matcher == None

def test_extract_matcher_popped_from_config():
    c = {'patterns': [], 'ignore_patterns': [], 'regexes': [],
         'ignore_regexes': [], 'test': 'test'}
    config = parse.extract_matcher(c)[1]
    assert len(config) == 1
    assert config['test'] == 'test'

def test_extract_matcher_success():
    matcher = parse.extract_matcher({'patterns': ['test'],})[0]
    assert len(matcher.patterns) == 1
    assert 'test' in matcher.patterns