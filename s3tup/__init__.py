import logging
import os

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
    try:
        if isinstance(config, basestring):
            import yaml
            config = yaml.load(file(config))
        elif isinstance(config, file):
            import yaml
            config = yaml.load(config)
    except Exception as e:
        raise ConfigParseError(e)
   
    if isinstance(config, dict):
        config = [config,]

    try: conn = Connection(access_key_id, secret_access_key)
    except AwsCredentialNotFound: conn = None
    for c in config:
        b = make_bucket(conn, **c)
        b.sync(rsync_only)
    
    log.info('request totals:')
    with stats_lock:
        for k,v in stats.iteritems():
            if v > 0:
                log.info('- {}: {}'.format(k,v))