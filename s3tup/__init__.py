import logging

# Silence requests logger
requests_log = logging.getLogger("requests")
requests_log.setLevel(logging.WARNING)

log = logging.getLogger('s3tup')
log.addHandler(logging.NullHandler())

from s3tup.connection import Connection, stats, stats_lock
from s3tup.parse import load_config, parse_config

def s3tup(config, access_key_id=None, secret_access_key=None,
          rsync_only=False):
    log.info('**** s3tup ****')

    config = load_config(config)
    buckets = parse_config(config)

    for b in buckets:
        if b.conn is None:
            b.conn = Connection(access_key_id, secret_access_key)
        b.sync(rsync_only)
    
    log.debug('request totals:')
    with stats_lock:
        for k,v in stats.iteritems():
            if v > 0:
                log.debug('- {}: {}'.format(k,v))