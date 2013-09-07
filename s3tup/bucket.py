import logging
import binascii

from bs4 import BeautifulSoup

from connection import Connection
from key import KeyFactory, Key
from rsync import rsync
from exception import AwsCredentialNotFound
import utils
import constants

log = logging.getLogger('s3tup.bucket')

def make_bucket(conn=None, **kwargs):
    bucket_name = kwargs.pop('bucket')

    if conn is None:
        access_key_id = kwargs.pop('access_key_id', None)
        secret_access_key = kwargs.pop('secret_access_key', None)
        conn = Connection(access_key_id, secret_access_key)

    if 'key_config' in kwargs:
        key_factory = KeyFactory(conn, bucket_name, kwargs.pop('key_config'))
    else:
        key_factory = None

    return Bucket(conn, bucket_name, key_factory, **kwargs)


class Bucket(object):

    def __init__(self, conn, name, key_factory=None, **kwargs):
        self.conn = conn
        self.name = name
        self.key_factory = key_factory

        self.redirects = kwargs.pop('redirects', [])
        for attr in kwargs:
            if attr in constants.BUCKET_ATTRS:
                self.__dict__[attr] = kwargs[attr]
            else:
                raise TypeError("Bucket.__init__() got an unexpected keyword"
                                " argument '{}'".format(attr))

    def make_key(self, key_name):
        """Return a properly configured Key object"""
        if self.key_factory is None:
            return Key(self.conn, key_name, self.name)
        else:
            return self.key_factory.make_key(key_name)

    def make_request(self, method, params=None, data=None, headers=None):
        """Convenience method for self.conn.make_request"""
        return self.conn.make_request(method, self.name, None, params,
                                      data=data, headers=headers)

    def get_remote_keys(self, prefix=None):
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
                etag_hex = c.find('etag').text.replace('"', '')
                etag_bin = binascii.unhexlify(etag_hex)
                etag = binascii.b2a_base64(etag_bin).strip()
                yield {'name': key, 'etag': etag, 'size': size,
                       'modified': modified}
            more = root.find('istruncated').text == 'true'

    def sync(self, rsync_only=False):
        log.info("syncing bucket '{}'...".format(self.name))

        # Create bucket
        try: headers = {'x-amz-acl': self.canned_acl}
        except AttributeError: headers = None
        try:
            data = """<CreateBucketConfiguration 
                   xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                   <LocationConstraint>{}</LocationConstraint> 
                   </CreateBucketConfiguration >""".format(self.region)
        except AttributeError:
            data = None
        self.conn.make_request('PUT', self.name, headers=headers, data=data)

        if rsync_only and 'rsync' not in self.__dict__:
            log.warning("Running in rsync only mode with no rsync config.")

        if not rsync_only:
            self._sync_bucket()

        if not rsync_only and self.key_factory is not None:
            before = list(self.get_remote_keys())
        
        self._rsync_keys()

        if not rsync_only and self.key_factory is not None:
            if 'rsync' not in self.__dict__:
                keys = before
            else:
                after = list(self.get_remote_keys())
                keys = utils.key_diff(before, after)['unmodified']
            self._sync_keys(keys)

        if not rsync_only:
            self._sync_redirects()

        log.info("bucket '{}' sucessfully synced!\n".format(self.name))

    def _sync_bucket(self):
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

    def _sync_keys(self, keys=None):
        if self.key_factory is None:
            return
        if keys is None:
            keys = self.get_remote_keys()
        for k in keys:
            key = self.make_key(k)
            key.sync()

    def _sync_redirects(self):
        for k,v in self.redirects:
            log.info("creating redirect from {} to {}".format(k, v))
            headers = {'x-amz-website-redirect-location': v}
            self.conn.make_request('PUT', self.name, k, headers=headers)

    def _rsync_keys(self):
        # Accept either a dict or a list dicts
        if isinstance(self.rsync, dict):
            self.rsync = [self.rsync,]

        for rs_config in self.rsync:
            matcher = utils.Matcher(
                rs_config.pop('patterns', None),
                rs_config.pop('ignore_patterns', None),
                rs_config.pop('regexes', None),
                rs_config.pop('ignore_regexes', None),
            )
            rsync(self, matcher=matcher, **rs_config)

    # Individual syncing methods

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
            data = """<?xml version="1.0" encoding="UTF-8"?>
                   <BucketLoggingStatus
                   xmlns="http://doc.s3.amazonaws.com/2006-03-01"/>"""
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
        data = """<VersioningConfiguration
               xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
               <Status>{}</Status>
               </VersioningConfiguration>""".format(status)
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