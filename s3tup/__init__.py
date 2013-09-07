import logging
import os

import yaml

from connection import Connection, stats, stats_lock
from bucket import make_bucket
from exception import ConfigParseError, AwsCredentialNotFound

# Silence requests logger
requests_log = logging.getLogger("requests")
requests_log.setLevel(logging.WARNING)

log = logging.getLogger('s3tup')

def s3tup(config, access_key_id=None, secret_access_key=None,
          rsync_only=False):
    log.info('**** s3tup ****')

    # Load configuration from filename or file-like object
    try:
        if isinstance(config, basestring):
            config = yaml.load(file(config))
        elif isinstance(config, file):
            config = yaml.load(config)
    except yaml.YAMLError as e:
        raise ConfigParseError(e)

    # Attempt to create Connection from params
    try:
        conn = Connection(access_key_id, secret_access_key)
    except AwsCredentialNotFound:
        conn = None
    for c in config:
        b = make_bucket(conn, **c)
        b.sync(rsync_only)
    
    log.debug('request totals:')
    with stats_lock:
        for k,v in stats.iteritems():
            if v > 0:
                log.debug('- {}: {}'.format(k,v))