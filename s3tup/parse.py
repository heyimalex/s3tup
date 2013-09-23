"""
s3tup.parse
~~~~~~~~~~~

This module contains all of the parsing methods used to convert YAML
configuration files into usable s3tup objects.
"""

import logging
import copy

from s3tup.connection import Connection
from s3tup.bucket import Bucket
from s3tup.key import KeyFactory, KeyConfigurator, Key
from s3tup.rsync import Rsync
from s3tup.exception import ConfigParseError, ConfigLoadError
from s3tup.utils import Matcher

log = logging.getLogger('s3tup.parse')

def load_config(config):
    """Return parseable s3tup configuration from whatever 'config' is.

    Probably the most magic method here, load_config guesses how to convert
    'config' into a regular s3tup config list based on its type.

    Currently supports:

        * config is basestring: load yaml from file at 'config'
        * config is file: load yaml from config.read()
        * config is dict: put into list
        * config is list: pass it on through

    """
    if isinstance(config, basestring):
        try:
            config = file(config)
        except IOError:
            raise ConfigLoadError("Config file '{}' does not exist"
                                  .format(config))

    if isinstance(config, file):
        import yaml
        try:
            config = yaml.load(config)
        except yaml.YAMLError as e:
            raise ConfigLoadError(e)

    if isinstance(config, dict):
        config = [config,]

    return config

# Recursive config definition
# [x, ...] = list of 'x's
# {a, b, c} = dict with keys 'a', 'b', 'c'
# {a!} = key 'a' is required
# {*iterable} = unpack iterable into definition
# config: [bucket, ...]
# bucket: {bucket!, key_config, rsync, *s3tup.constants.BUCKET_ATTRS}
# key_config: [key_configurator, ...]
# key_configurator: {*s3tup.constants.KEY_ATTRS, *matcher_fields}
# rsync: (src|rsync_object|[rsync_object,])
# rsync_object: {src, dest, delete, *matcher_fields}
# matcher_fields: (patterns, ignore_patterns, regexes, ignore_regexes)

# IMPORTANT:
# Many parse methods directly mutate input by design. This can lead to many
# hard to debug issues, especially when using s3tup programtically (rather
# than through the cli). To counteract this, the parse_method decorator is
# applied to each parse method. This decorator basically calls copy.deepcopy
# on the input before passing it on to the decorated function. Performance
# not really an issue here, anything more fine grained would be premature.

def parse_method(f):
    def inner(config):
        return f(copy.deepcopy(config))
    return inner

@parse_method
def parse_config(config):
    """Return a list of fully configured Buckets from 'config'."""
    if not isinstance(config, list):
        raise ConfigParseError('Config must be a list')

    buckets = []
    for bucket_config in config:
        buckets.append(parse_bucket(bucket_config))
    return buckets

@parse_method
def parse_bucket(config):
    """Return a properly configured Bucket from 'config'.

    Note: If access_key_id and secret_access_key are not in the config
    dict, the returned bucket will have a conn of None, *not* a default
    Connection that tries to load credentials from env vars.

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

    # Attempt to create and return a Bucket from the supplied config.
    # Bucket will throw TypeError if it gets unknown kwargs.
    try:
        return Bucket(conn, bucket_name, key_factory, rsync, **config)
    except TypeError as e:
        raise ConfigParseError(e)

@parse_method
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

    Regardless of the input type, this function returns a list.

    """
    # Convert all acceptable input into a standard list-of-dicts format
    if isinstance(config, basestring):
        config = [{'src': config},]
    elif isinstance(config, dict):
        config = [config,]

    rsync = []
    for r in config:
        rsync.append(parse_rsync_object(r))
    return rsync

@parse_method
def parse_rsync_object(config):
    """Return a properly configured Rsync object from 'config'"""
    matcher, config = extract_matcher(config)
    try:
        return Rsync(matcher=matcher, **config)
    except TypeError as e:
        raise ConfigParseError(e)

@parse_method
def parse_key_config(config):
    """Return a properly configured KeyFactory from 'config'"""
    # Unlike with rsync, key_config must be a list
    if not isinstance(config, list):
        raise ConfigParseError('key_config must be a list')

    configurators = []
    for c in config:
        matcher, config = extract_matcher(c)
        configurators.append(parse_key_configurator(c))
    return KeyFactory(configurators)

@parse_method
def parse_key_configurator(config):
    """Return a properly configured KeyConfigurator from 'config'"""
    matcher, config = extract_matcher(config)
    try:
        return KeyConfigurator(matcher=matcher, **config)
    except TypeError as e:
        raise ConfigParseError(e)

@parse_method
def extract_matcher(config):
    """Return (matcher, config) tuple extracted from 'config'.

    Extracts the matcher fields from the passed in config dict, creates a
    Matcher object from them, and then returns the Matcher and the config
    dict minus the matcher fields back in a tuple. If none of the matcher
    fields are present, the matcher returned will be None.

    """
    patterns = config.pop('patterns', None)
    ignore_patterns = config.pop('ignore_patterns', None)
    regexes = config.pop('regexes', None)
    ignore_regexes = config.pop('ignore_regexes', None)

    params = (patterns, ignore_patterns, regexes, ignore_regexes)
    if any(p is not None for p in params):
        matcher = Matcher(*params)
    else:
        matcher = None
    return matcher, config