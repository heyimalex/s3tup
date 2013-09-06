import logging
import os

import yaml

from connection import Connection
from bucket import BucketFactory

requests_log = logging.getLogger("requests")
requests_log.setLevel(logging.WARNING)

log = logging.getLogger('s3tup')

def s3tup(config, access_key_id=None, secret_access_key=None, rsync_only=False,):

    log.info('**** s3tup ****')
    config = yaml.load(file(config))

    conn = Connection(access_key_id, secret_access_key)
    bf = BucketFactory()
    for c in config:
        b = bf.make_bucket(conn, **c)
        b.sync(rsync_only)
    
    from connection import stats
    log.info('request totals:')
    for k,v in stats.iteritems():
        if v > 0:
            log.info('- {}: {}'.format(k,v))