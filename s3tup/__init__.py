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

    if access_key_id is None:
        try: access_key_id = os.environ['AWS_ACCESS_KEY_ID']
        except KeyError: raise Exception('You must supply an aws access key id.')

    if secret_access_key is None:
        try: secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
        except KeyError: raise Exception('You must supply an aws secret access key.')

    conn = Connection(access_key_id, secret_access_key)
    bf = BucketFactory(conn)
    for c in config:
        b = bf.make_bucket(**c)
        b.sync(rsync_only)
    
    from connection import stats
    log.info('request totals:')
    for k,v in stats.iteritems():
        if v > 0:
            log.info('- {}: {}'.format(k,v))