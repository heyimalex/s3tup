import logging
import os

import yaml

from connection import Connection
from bucket import BucketFactory

requests_log = logging.getLogger("requests")
requests_log.setLevel(logging.WARNING)

log = logging.getLogger('s3tup')

def s3tup(config, access_key_id=None, secret_access_key=None, verbose=False, debug=False):

    if debug:
        logging.basicConfig(format="%(levelname)s: %(message)s", level=logging.DEBUG)
    elif verbose:
        logging.basicConfig(format="%(message)s", level=logging.INFO)
    else:
        logging.basicConfig(format="%(levelname)s: %(message)s")

    log.info('**** s3tup ****\n')

    if isinstance(config, file): # if config is file, load yaml
        config = yaml.load(config)
    elif isinstance(config, basestring): # if config is string, it's a filename
        config = yaml.load(file(config))
    elif isinstance(config, list): # if config is a list
        config = config
    config = config or []

    if access_key_id is None or secret_access_key is None:
        try:
            access_key_id = os.environ.get['AWS_ACCESS_KEY_ID']
            secret_access_key = os.environ.get['AWS_SECRET_ACCESS_KEY']
        except KeyError:
            raise Exception('You must supply credentials')

    conn = Connection(access_key_id, secret_access_key)
    bf = BucketFactory(conn)
    for c in config:
        b = bf.make_bucket(**c)
        b.sync()

    from connection import stats
    log.debug('request totals:')
    for k,v in stats.iteritems():
        if v > 0:
            log.debug('- {}: {}'.format(k,v))