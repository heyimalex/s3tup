import logging
import os

import s3tup.utils as utils

log = logging.getLogger('s3tup.rsync')

class Rsync(object):
    def __init__(self, src=None, dest=None, delete=False, matcher=None,
                 protect=None):
        self.src = src
        self.dest = dest
        self.delete = delete
        self.matcher = matcher
        self.protect = protect

    def run(self, bucket, protect=None):
        # Silence key logging to avoid redundant messages
        key_log = logging.getLogger('s3tup.key')
        key_log.setLevel(logging.WARNING)

        src = self.src or ''
        dest = self.dest or ''

        bucket_url = bucket.name+'.s3.amazonaws.com'
        log.info("rsyncing '{}'".format(os.path.join(bucket_url, dest)))
        log.info("    with '{}'...".format(os.path.abspath(src)))

        if protect is not None:
            protect += self.protect
        else:
            protect = self.protect

        local_keys = self._get_local_keys()
        remote_keys = list(bucket.get_remote_keys(prefix=self.dest))

        # Is it smart to depend on key_diff for this?
        keys = utils.key_diff(remote_keys, local_keys)
        new_keys = keys['new']
        removed_keys = keys['removed']
        modified_keys = keys['modified']
        unmodified_keys = keys['unmodified']

        # Delete removed keys if delete
        if self.delete is True:
            bucket.delete_remote_keys(removed_keys)
        else:
            unmodified_keys.extend(removed_keys)
            removed_keys = []

        # Upload new keys
        for k in new_keys:
            key = bucket.make_key(k)
            log.info("new: '{}', uploading now...".format(k))
            key.rsync(self._get_local_file(k))

        # Upload modified keys
        for k in modified_keys:
            key = bucket.make_key(k)
            log.info("modified: '{}', uploading now...".format(k))
            key.rsync(self._get_local_file(k))

        # Unsilence key logging
        key_log.setLevel(logging.DEBUG)

        log.info('rsync complete!')
        log.info('{} new, {} removed, {} modified, {} unmodified'.format(
                 len(new_keys), len(removed_keys), len(modified_keys),
                 len(unmodified_keys)))

        return {'new': new_keys, 
                'removed': removed_keys, 
                'modified': modified_keys, 
                'unmodified': unmodified_keys}

    def _get_local_keys(self):
        src = self.src or '.'
        dest = self.dest or ''
        matcher = self.matcher or utils.Matcher()
        keys = []
        for relpath in utils.os_walk_iter(src):
            if not matcher.matches(relpath):
                continue

            key_name = os.path.join(dest, relpath)
            local_path = os.path.join(src, relpath)

            key = {}
            key['name'] = key_name
            key['md5'] = utils.file_md5(open(local_path, 'rb'))
            key['size'] = os.path.getsize(local_path)
            keys.append(key)
        return keys

    def _get_local_file(self, key_name):
        src = self.src or ''
        dest = self.dest or ''
        unrelative = os.path.relpath(key_name, dest)
        return open(os.path.join(src, unrelative), 'rb')