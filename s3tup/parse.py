import logging

import yaml

from s3tup.bucket import Bucket
from s3tup.key import KeyFactory, KeyConfigurator, Key
from s3tup.rsync import Rsync
from s3tup.exception import ConfigParseError, ConfigLoadError

log = logging.getLogger('s3tup.parse')

def load_config(config):
    """Return parseable s3tup configuration from whatever 'config' is.

    Probably the most magic method here, load_config guesses how to convert
    'config' into a regular s3tup config list based on its type.

    Currently supports:

        * config is basestring: load yaml from file at 'config'
        * config is file: load yaml from 'config.read()''
        * config is dict: put into list

    """
    if isinstance(config, basestring):
        try:
            config = yaml.load(file(config))
        except yaml.YAMLError as e:
            raise ConfigLoadError(e)
        except IOError:
            raise ConfigLoadError("Config file '{}' does not exist"
                                  .format(config))
    elif isinstance(config, file):
        try:
            config = yaml.load(config)
        except yaml.YAMLError as e:
            raise ConfigLoadError(e)

    return config

def parse_config(config):
    """Return a list of fully configured Buckets from 'config'

    This is the root of the parse methods. You pass it a full fledged
    s3tup config and from it it returns a list of properly configured
    s3tup Buckets.
    """
    if not isinstance(config, list):
        raise ConfigParseError('Config must be a list')

    buckets = []
    for b in config:
        buckets.append(parse_bucket(b))
    return buckets

def parse_bucket(config):
    """Return a properly configured Bucket from 'config'

    Note: If access_key_id and secret_access_key are not in the config
    dict, the returned bucket will have a conn of None, *not* a default
    Connection that tries to load from env vars.
    """
    try:
        bucket_name = config.pop('bucket')
    except KeyError:
        raise ConfigParseError("Bucket config must contain field 'bucket'")

    try:
        access_key_id = config.pop('access_key_id')
        secret_access_key = config.pop('secret_access_key')
    except KeyError:
        conn = None
    else:
        conn = Connection(access_key_id, secret_access_key)

    if 'key_config' in config:
        key_factory = parse_key_config(config.pop('key_config'))
    else:
        key_factory = None

    if 'rsync' in config:
        rsync = parse_rsync(config.pop('rsync'))
    else:
        rsync = []
    try:
        return Bucket(conn, bucket_name, key_factory, rsync, **config)
    except TypeError as e:
        raise ConfigParseError(e)

def parse_rsync(config):
    """Return a list of Rsync objects from 'config'

    This has some magic in that it'll accept a string, dict, or list.
    Done to make rsync definitions less verbose when they don't need to be.

    Examples of valid yaml configs for the rsync field:

        rsync: path/to/some/folder            # string

        rsync:
          src: path/to/some/folder            # dict
          delete: true

        rsync:
          - src: path/to/some/folder          # list
            delete: true
          - src: path/to/some/other/folder
            dest: some/prefix/

    Regardless of the input type it returns a list.
    """
    if isinstance(config, basestring):
        config = [{'src': config},]
    elif isinstance(config, dict):
        config = [config,]

    rsync = []
    for r in config:
        rsync.append(parse_rsync_object(r))
    return rsync

def parse_rsync_object(config):
    """Return a properly configured Rsync object from 'config'"""
    matcher, config = extract_matcher(config)
    try:
        return Rsync(matcher=matcher, **config)
    except TypeError as e:
        raise ConfigParseError(e)

def parse_key_config(config):
    """Return a properly configured KeyFactory from 'config'"""
    # Unlike rsync, key_config must be a list
    if not isinstance(config, list):
        raise ConfigParseError('key_config must be a list')

    configurators = []
    for c in config:
        matcher, config = extract_matcher(c)
        configurators.append(parse_key_configurator(c))
    return KeyFactory(configurators)

def parse_key_configurator(config):
    """Return a properly configured KeyConfigurator from 'config'"""
    matcher, config = extract_matcher(config)
    try:
        return KeyConfigurator(matcher=matcher, **config)
    except TypeError as e:
        raise ConfigParseError(e)

def extract_matcher(config):
    """Return (matcher, config) tuple extracted from 'config'

    Extracts the matcher fields from the passed in config dict, creates a
    Matcher object from them, and then returns the Matcher and the config
    dict minus the matcher fields back in a tuple. If none of the matcher
    fields are present, the matcher returned will be None.
    """
    patterns = config.pop('patterns', None)
    ignore_patterns = config.pop('ignore_patterns', None)
    regexes = config.pop('regexes', None)
    ignore_regexes = config.pop('ignore_regexes', None)

    if patterns and ignore_patterns and regexes and ignore_regexes:
        matcher = Matcher(patterns, ignore_patterns, regexes, ignore_regexes)
    else:
        matcher = None
    return matcher, config