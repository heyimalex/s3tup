import logging
import binascii

from bs4 import BeautifulSoup

from s3tup.key import Key
import s3tup.constants as constants
import s3tup.utils as utils

log = logging.getLogger('s3tup.bucket')

class Bucket(object):
    """
    Encapsulates configuration for an s3 bucket and its keys. It contains a
    key_factory which handles key configuration and many attributes (listed
    in constants.BUCKET_ATTRS) that you can set, delete, modify, and then
    sync to s3 using the various sync methods provided.

    """
    def __init__(self, conn, name, key_factory=None, rsync=None, **kwargs):
        self.conn = conn
        self.name = name
        self.key_factory = key_factory
        self.rsync = rsync or []
        self.redirects = kwargs.pop('redirects', [])

        for k,v in kwargs.iteritems():
            if k in constants.BUCKET_ATTRS:
                self.__dict__[k] = v
            else:
                raise TypeError("__init__() got an unexpected keyword"
                                " argument '{}'".format(k))

    def make_key(self, key_name):
        """Return a properly configured Key object."""
        if self.key_factory is None:
            return Key(self.conn, key_name, self.name)
        else:
            return self.key_factory.make_key(self.conn, key_name, self.name)

    def make_request(self, method, params=None, data=None, headers=None):
        """Convenience method for self.conn.make_request; has the bucket and
        key fields already filled in."""
        return self.conn.make_request(method, self.name, None, params,
                                      data=data, headers=headers)

    # Named get_remote_keys instead of just overriding __iter__
    # to avoid ambiguity. Returns dicts instead of objects for the
    # same reason.
    def get_remote_keys(self, prefix=None):
        """Generate list of dicts representing all keys in this s3 bucket.

        Each dict returned contains fields 'name', 'md5', 'size', and
        'modified'. Paging is handled automatically. Optional (str)
        prefix param will limit the results to those keys prefixed by it.

        """
        more = True
        marker = None
        while more is True:
            params = {'marker': marker, 'prefix': prefix}
            r = self.conn.make_request('GET', self.name, params=params)
            root = BeautifulSoup(r.text).find('listbucketresult')
            for c in root.find_all('contents'):
                key = c.find('key').text
                marker = key
                modified = c.find('lastmodified').text
                size = int(c.find('size').text)
                md5_hex = c.find('etag').text.replace('"', '')
                md5_bin = binascii.unhexlify(md5_hex)
                md5 = binascii.b2a_base64(md5_bin).strip()
                yield {'name': key, 'md5': md5, 'size': size,
                       'modified': modified}
            more = root.find('istruncated').text == 'true'

    def delete_remote_keys(self, keys):
        """Delete a list of keys from this bucket.

        Keys is a list of str key names to be deleted. S3's delete operation
        has a limit of 1000 keys per request so this method handles paging
        as well.

        """
        for i in xrange(0, len(keys), 1000):
            data = ('<?xml version="1.0" encoding="UTF-8"?>\n'
                    '<Delete>\n  <Quiet>true</Quiet>\n')
            for k in keys[i:i+1000]:
                log.info('removed: {}'.format(k))
                data += '  <Object><Key>{}</Key></Object>\n'.format(k)
            data += '</Delete>'
            self.make_request('POST', 'delete', data=data)

    def sync(self, rsync_only=False):
        """Sync everything.

        Takes every applicable attribute set on this Bucket object and
        configures its respective s3 bucket (defined by self.name) to match
        it. Optional rsync only mode will only run rsync (no setting bucket
        configuration, no syncing unmodified keys, no making redirects).

        """
        log.info("syncing bucket '{}'...".format(self.name))

        self.create_bucket()

        if rsync_only and 'rsync' not in self.__dict__:
            log.warning("Running in rsync only mode with no rsync config.")

        if not rsync_only:
            self.sync_bucket()

        # Get the key list before we rsync so we can run diff after and only
        # sync unmodified keys.
        if not rsync_only and self.key_factory is not None:
            before = list(self.get_remote_keys())
        
        self.rsync_keys()

        if not rsync_only and self.key_factory is not None:
            if 'rsync' not in self.__dict__:
                keys = [k['name'] for k in before]
            else:
                after = list(self.get_remote_keys())
                keys = utils.key_diff(before, after)['unmodified']
            self.sync_keys(keys)

        if not rsync_only:
            self.sync_redirects()

        log.info("bucket '{}' sucessfully synced!\n".format(self.name))

    def create_bucket(self):
        """Create bucket on s3."""
        try: headers = {'x-amz-acl': self.canned_acl}
        except AttributeError: headers = None
        try:
            if self.region.strip() != '' and self.region is not None:
                data = ('<CreateBucketConfiguration '
                        'xmlns="http://s3.amazonaws.com/doc/2006-03-01/">\n'
                        '  <LocationConstraint>{}</LocationConstraint>\n'
                        '</CreateBucketConfiguration>').format(self.region)
            else:
                data = None
        except AttributeError:
            data = None
        self.conn.make_request('PUT', self.name, headers=headers, data=data)

    def sync_bucket(self):
        """Run all of the bucket sync methods."""
        self.sync_acl()
        self.sync_cors()
        self.sync_lifecycle()
        self.sync_logging()
        self.sync_notification()
        self.sync_policy()
        self.sync_tagging()
        self.sync_versioning()
        self.sync_website()

    def sync_keys(self, keys=None):
        """Sync every key in keys.

        Keys should be a list of key names. Will not run if self.key_factory
        is None as that implies no key config is set on this object (and it
        would just restore every key to its default). If the keys param is
        not provided, it will run on all keys currently in the s3 bucket by
        first calling self.get_remote_keys.

        """
        if self.key_factory is None:
            return
        if keys is None:
            keys = [k['name'] for k in self.get_remote_keys()]

        if len(keys) < 1:
            log.info('no keys need to be synced!')
            return

        key_log = logging.getLogger('s3tup.key')
        key_log.setLevel(logging.WARNING)

        log.info('syncing all keys...')

        for k in keys:
            key = self.make_key(k)
            key.sync()
            log.info("key '{}' sucessfully synced!".format(k))

        key_log.setLevel(logging.DEBUG)

    def sync_redirects(self):
        """Create all redirects defined in self.redirects"""
        for k,v in self.redirects:
            log.info("creating redirect from {} to {}".format(k, v))
            headers = {'x-amz-website-redirect-location': v}
            self.conn.make_request('PUT', self.name, k, headers=headers)

    def rsync_keys(self):
        """Run rsync for every rsync in self.rsync"""
        for rsync in self.rsync:
            rsync.run(self)

    # Individual syncing methods.
    #
    # Each checks if this object has its respective attr set and, if it does,
    # proceeds to sync that value with the s3 bucket. Each will return s3's
    # response in the form of a requests.Response object if the attr is set,
    # and if not will return False. These map directly to the fields
    # defined in the bucket config section of the readme, so if you're
    # looking for details on what values are allowed check there.

    def sync_acl(self):
        try: acl = self.acl
        except AttributeError: return False
        
        if acl is not None:
            log.info("setting bucket acl...")
            return self.make_request('PUT', 'acl', data=acl)
        else:
            log.info("reverting to default bucket acl...")
            return self.make_request('PUT', 'acl',
                                     headers={"x-amz-acl":"private"})

    def sync_cors(self):
        try: cors = self.cors
        except AttributeError: return False

        if cors is not None:
            log.info("setting cors configuration...")
            return self.make_request('PUT', 'cors', data=cors)
        else:
            log.info("deleting cors configuration...")
            return self.make_request('DELETE', 'cors')

    def sync_lifecycle(self):
        try: lifecycle = self.lifecycle
        except AttributeError: return False

        if lifecycle is not None:
            log.info("setting lifecycle configuration...")
            return self.make_request('PUT', 'lifecycle', data=lifecycle)
        else:
            log.info("deleting lifecycle configuration...")
            return self.make_request('DELETE', 'lifecycle')

    def sync_logging(self):
        try: logging = self.logging
        except AttributeError: return False

        if logging is not None:
            log.info("setting logging configuration...")
            data = logging
        else:
            log.info("deleting logging configuration...")
            data = ('<?xml version="1.0" encoding="UTF-8"?>\n'
                    '<BucketLoggingStatus '
                    'xmlns="http://doc.s3.amazonaws.com/2006-03-01"/>')
        return self.make_request('PUT', 'logging', data=data)

    def sync_notification(self):
        try: notification = self.notification
        except AttributeError: return False

        if notification is not None:
            log.info("setting notification configuration...")
            data = notification
        else:
            log.info("deleting notification configuration...")
            data = '<NotificationConfiguration />'
        return self.make_request('PUT', 'notification', data=data)

    def sync_policy(self):
        try: policy = self.policy
        except AttributeError: return False

        if policy is not None:
            log.info("setting bucket policy...")
            return self.make_request('PUT', 'policy', data=policy)
        else:
            log.info("deleting bucket policy...")
            return self.make_request('DELETE', 'policy')

    def sync_tagging(self):
        try: tagging = self.tagging
        except AttributeError: return False

        if tagging is not None:
            log.info("setting bucket tags...")
            return self.make_request('PUT', 'tagging', data=tagging)
        else:
            log.info("deleting bucket tags...")
            return self.make_request('DELETE', 'tagging')

    def sync_versioning(self):
        try: versioning = self.versioning
        except AttributeError: return False

        if versioning:
            log.info("enabling versioning...")
            status = 'Enabled'
        else:
            log.info("suspending versioning...")
            status = 'Suspended'
        data = ('<VersioningConfiguration '
                'xmlns="http://s3.amazonaws.com/doc/2006-03-01/">\n'
                '  <Status>{}</Status>\n'
                '</VersioningConfiguration>').format(status)
        return self.make_request('PUT', 'versioning', data=data)

    def sync_website(self):
        try: website = self.website
        except AttributeError: return False

        if website is not None:
            log.info("setting website configuration...")
            return self.make_request('PUT', 'website', data=website)
        else:
            log.info("deleting website configuration...")
            return self.make_request('DELETE', 'website')