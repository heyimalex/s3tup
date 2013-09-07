import logging
import os

import utils

log = logging.getLogger('s3tup.rsync')

def rsync(bucket, src='', dest='', delete=False, matcher=utils.Matcher()):
    # Silence key logging to avoid redundant messages
    key_log = logging.getLogger('s3tup.key')
    key_log.setLevel(logging.WARNING)

    bucket_url = bucket.name+'.s3.amazonaws.com'
    log.info("rsyncing '{}'".format(os.path.abspath(src)))
    log.info("    with '{}'...".format(os.path.join(bucket_url, dest)))

    all_keys = {}

    class RsyncKey(object):

        def __init__(self, name):
            self.name = name
            self.remote = False
            self.local = False

        @property
        def new(self):
            return self.local == True and self.remote == False

        @property
        def removed(self):
            return self.local == False and self.remote == True

        @property
        def modified(self):
            if not (self.local and self.remote):
                return False
            if self.local_md5 != self.remote_md5:
                return True
            if self.local_size != int(self.remote_size):
                return True
            return False

        @property
        def unmodified(self):
            if not (self.local and self.remote):
                return False
            if self.local_md5 != self.remote_md5:
                return False
            if self.local_size != self.remote_size:
                return False
            return True 

        def __lt__(self, other):
            return self.name < other.name

        def rsync(self):
            key = bucket.make_key(self.name)
            key.rsync(open(self.local_path, 'rb'))


    # Local keys
    for relpath in utils.os_walk_iter(src):
        if not matcher.match(relpath):
            continue
        key_name = os.path.join(dest, relpath)
        local_path = os.path.join(src, relpath)

        rsk = RsyncKey(key_name)
        rsk.local = True
        rsk.local_path = local_path
        rsk.local_md5 = utils.file_md5(local_path)
        rsk.local_size = os.path.getsize(local_path)

        all_keys[rsk.name] = rsk

    # Remote keys
    prefix = dest if dest != '' else None
    for key in bucket.get_remote_keys(prefix=prefix):
        try:
            rsk = all_keys[key['name']]
        except KeyError:
            rsk = RsyncKey(key['name'])

        rsk.remote = True
        rsk.remote_md5 = key['etag']
        rsk.remote_size = key['size']

        all_keys[rsk.name] = rsk

    new_keys = []
    removed_keys = []
    modified_keys = []
    unmodified_keys = []

    # Sort every key into appropriate list
    for k in sorted(all_keys.iterkeys()):
        v = all_keys[k]
        if v.new:
            new_keys.append(k)
        elif v.removed:
            removed_keys.append(k)
        elif v.modified:
            modified_keys.append(k)
        elif v.unmodified:
            unmodified_keys.append(k)

    # Delete removed keys if delete
    if delete is True:
        delete_keys(bucket, removed_keys)
    else:
        unmodified_keys.extend(removed_keys)
        removed_keys = []

    # Upload new keys
    for k in new_keys:
        key = all_keys[k]
        log.info("new: '{}', uploading now from '{}'...".format(
                 k, key.local_path))
        key.rsync()

    # Upload modified keys
    for k in modified_keys:
        key = all_keys[k]
        log.info("modified: '{}', uploading now from '{}'...".format(k, key.local_path))
        key.rsync()

    key_log.setLevel(logging.DEBUG)

    log.info('rsync complete!')
    log.info('{} new, {} removed, {} modified, {} unmodified'.format(
             len(new_keys), len(removed_keys), len(modified_keys),
             len(unmodified_keys)))

    return {'new': new_keys, 
            'removed': removed_keys, 
            'modified': modified_keys, 
            'unmodified': unmodified_keys}

def delete_keys(bucket, keys):
    for i in xrange(0, len(keys), 1000):
        data = """<?xml version="1.0" encoding="UTF-8"?>
                  <Delete>
                  <Quiet>true</Quiet>\n"""
        for k in keys[i:i+1000]:
            log.info('removed: {}'.format(k))
            data += '<Object><Key>{}</Key></Object>\n'.format(k)
        data += '</Delete>'
        bucket.make_request('POST', 'delete', data=data)