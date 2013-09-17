import logging
import os

import utils

log = logging.getLogger('s3tup.rsync')

def rsync(bucket, src='', dest='', delete=False, match=None,
          protect=None):
    """Rsync a local path with an s3 bucket.

    Will upload all new and modified local files from the src directory into
    the specified bucket. It uses the key's md5 hash and size to determine
    if it's been modified (and wheter or not to upload), and all keys
    uploaded are properly configured by the inputed bucket.

    Params:

    bucket - s3tup.bucket.Bucket object. The name attribute of this object
             is the destination bucket for this rsync and the keys that
             rsync uses to upload are created and configured using its
             make_key method.

    src - Relative or absolute path to the local folder you want to sync.
          Unlike actual rsyncing, trailing slash does not matter.

    dest - Allows you to rsync to a certain prefix on a bucket.
           Ex: If dest is 'assets', files of the src dir will be rsynced to
           example-bucket.s3.amazonaws.com/assets/

    delete - Whether or not to delete files in your s3 bucket that no longer
             exist locally. When used with dest, only files in dest will be
             deleted.

    match - s3tup.utils.Matcher object. If the matcher does not match on
              the local file path, that file will not be rsynced.

    Returns a dict with fields 'new', 'removed', 'modified', and
    'unmodified', and each field contains a list of key names.

    """
    # Silence key logging to avoid redundant messages

    match = match or utils.Matcher()
    key_log = logging.getLogger('s3tup.key')
    key_log.setLevel(logging.WARNING)

    bucket_url = bucket.name+'.s3.amazonaws.com'
    log.info("rsyncing '{}'".format(os.path.join(bucket_url, dest)))
    log.info("    with '{}'...".format(os.path.abspath(src)))
    
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
        if not matcher.matches(relpath):
            continue
        key_name = os.path.join(dest, relpath)
        local_path = os.path.join(src, relpath)

        rsk = RsyncKey(key_name)
        rsk.local = True
        rsk.local_path = local_path
        rsk.local_md5 = utils.file_md5(open(local_path, 'rb'))
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
        rsk.remote_md5 = key['md5']
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
        log.info("new: '{}', uploading now...".format(k))
        key.rsync()

    # Upload modified keys
    for k in modified_keys:
        key = all_keys[k]
        log.info("modified: '{}', uploading now...".format(k))
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
    """Delete a list of keys from a specified bucket.

    Bucket is a s3tup.bucket.Bucket object and keys is a list of str
    key names to be deleted. S3's delete operation has a limit of
    1000 keys per request so this method handles paging as well.

    """
    for i in xrange(0, len(keys), 1000):
        data = """<?xml version="1.0" encoding="UTF-8"?>
                  <Delete>
                  <Quiet>true</Quiet>\n"""
        for k in keys[i:i+1000]:
            log.info('removed: {}'.format(k))
            data += '<Object><Key>{}</Key></Object>\n'.format(k)
        data += '</Delete>'
        bucket.make_request('POST', 'delete', data=data)