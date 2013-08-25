import logging

import os
from utils import Matcher, list_bucket, file_md5

log = logging.getLogger('s3tup')

def rsync(key_factory, src='', delete=False, patterns=None,
          ignore_patterns=None, regexes=None, ignore_regexes=None):

    matcher = Matcher(patterns, ignore_patterns, regexes, ignore_regexes)
    conn = key_factory.conn
    bucket = key_factory.bucket_name
    log.info("rsyncing folder '{}' with bucket '{}'...".format(src, bucket))

    class RsyncInternalKey(object):
        def __init__(self, name):
            self.name = name
            self.local = False
            self.remote = False
        @property
        def local_path(self):
            return os.path.join(src, self.name)
        @property
        def local_md5(self):
            return file_md5(self.local_path)
        @property
        def new(self):
            return self.local == True and self.remote == False
        @property
        def removed(self):
            return self.local == False and self.remote == True
        @property
        def modified(self):
            if self.local == False or self.remote == False:
                return False
            return self.local_md5 != self.remote_md5
        @property
        def unmodified(self):
            if self.local == False or self.remote == False:
                return False
            return self.local_md5 == self.remote_md5
        def rsync(self):
            key = key_factory.make_key(self.name)
            key.rsync(self.local_path)
        def __lt__(self, other):
            return self.name < other.name


    all_keys = {}

    # Add all local keys
    for path, dirs, files in os.walk(src):
        for f in files:
            full_path = os.path.join(path, f)
            rel_path = os.path.relpath(full_path, src)
            if matcher.match(rel_path):
                k = RsyncInternalKey(rel_path)
                k.local = True
                all_keys[k.name] = k

    # Add all remote keys
    for list_key in list_bucket(key_factory.conn, key_factory.bucket_name):
        try: k = all_keys[list_key['name']]
        except KeyError: k = RsyncInternalKey(list_key['name'])
        k.remote = True
        k.remote_md5 = list_key['etag']
        all_keys[k.name] = k

    new_keys = [k for k,v in all_keys.iteritems() if v.new]
    removed_keys = [k for k,v in all_keys.iteritems() if v.removed]
    modified_keys = [k for k,v in all_keys.iteritems() if v.modified]
    unmodified_keys =[k for k,v in all_keys.iteritems() if v.unmodified]

    if delete == True:
        for i in xrange(0, len(removed_keys), 1000):
            data = """<?xml version="1.0" encoding="UTF-8"?>
                      <Delete>
                      <Quiet>true</Quiet>\n"""
            for k in removed_keys[i:i+1000]:
                log.info('removed: {}'.format(k))
                data += '<Object><Key>{}</Key></Object>\n'.format(k)
            data += '</Delete>'
            conn.make_request('POST', bucket, None, 'delete', data=data)
    else:
        unmodified_keys.extend(removed_keys)
        removed_keys = []

    for k in new_keys:
        log.info('new: {}, uploading now...'.format(k)) 
        key = all_keys[k]
        key.rsync()

    for k in modified_keys:
        log.info('modified: {}, uploading now...'.format(k)) 
        key = all_keys[k]
        key.rsync()

    log.info('rsync complete!')
    log.info('{} new, {} removed, {} modified, {} unmodified'.format(
             len(new_keys), len(removed_keys), len(modified_keys),
             len(unmodified_keys)))

    return {'new': new_keys, 
            'removed': removed_keys, 
            'modified': modified_keys, 
            'unmodified': unmodified_keys}