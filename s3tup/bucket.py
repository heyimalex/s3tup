import logging

from connection import Connection
from key import KeyFactory
from rsync import rsync
import utils
import constants

log = logging.getLogger('s3tup.bucket')

class BucketFactory(object):

    def make_bucket(self, conn=None, **kwargs):
        
        bucket_name = kwargs.pop('bucket')

        if conn is None:
            access_key_id = kwargs.pop('access_key_id')
            secret_access_key = kwargs.pop('secret_access_key')
            conn = Connection(access_key_id, secret_access_key)

        if 'key_config' in kwargs:
            key_factory = KeyFactory(conn, bucket_name, kwargs.pop('key_config'))
        else: key_factory = None

        bucket = Bucket(conn, bucket_name, key_factory, **kwargs)
        return bucket
        

class Bucket(object):

    def __init__(self, conn, name, key_factory=None, **kwargs):
        self.conn = conn
        self.name = name

        self.key_factory = key_factory

        # set defaults for required attributes
        self.redirects = kwargs.pop('redirects', [])

        for attr in kwargs:
            if attr in constants.BUCKET_ATTRS:
                self.__dict__[attr] = kwargs[attr]
            else:
                raise TypeError("Bucket.__init__() got an unexpected keyword\
                                 argument '{}'".format(attr))

    def sync(self, rsync_only=False):
        self._sync_bucket(rsync_only)

    def _sync_bucket(self, rsync_only=False):

        log.info("syncing bucket '{}'...".format(self.name))

        # create bucket
        headers = {}
        try: headers['x-amz-acl'] = self.canned_acl
        except AttributeError: pass
        try:
            data = """<CreateBucketConfiguration 
                       xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                      <LocationConstraint>{}</LocationConstraint> 
                      </CreateBucketConfiguration >""".format(self.region)
        except AttributeError:
            data = None
        self.conn.make_request('PUT', self.name, headers=headers, data=data)

        self.sync_acl()
        self.sync_cors()
        self.sync_lifecycle()
        self.sync_logging()
        self.sync_notification()
        self.sync_policy()
        self.sync_tagging()
        self.sync_versioning()
        self.sync_website()

        if 'rsync' in self.__dict__:
            rs_out = rsync(self.key_factory, **self.rsync)
            unmodified = rs_out['unmodified']
        else:
            unmodified = [k['name'] for k in utils.list_bucket(self.conn, self.name)]

        try:
            for k,v in self.redirects:
                log.info("creating redirect from {} to {}".format(k, v))
                headers = {'x-amz-website-redirect-location': v}
                self.conn.make_request('PUT', self.name, k, headers=headers, data=None)
        except AttributeError: pass

        if not rsync_only and self.key_factory is not None:
            for k in unmodified:
                key = self.key_factory.make_key(k)
                key.sync()

        log.info("bucket '{}' sucessfully synced!\n".format(self.name))

    def _sync_keys(self):
        pass

    # Individual syncing methods

    def sync_acl(self):
        try:
            if self.acl is not None:
                log.info("setting bucket acl...")
                headers = {}
                data = self.acl
            else:
                log.info("setting default bucket acl...")
                headers = {"x-amz-acl":"private"}
                data = None
            self.conn.make_request('PUT', self.name, None, 'acl', data=data, headers=headers)
        except AttributeError: pass

    def sync_cors(self):
        try:
            if self.cors is not None:
                log.info("setting cors configuration...")
                self.conn.make_request('PUT', self.name, None, 'cors',
                                       data=self.cors)
            else:
                log.info("deleting cors configuration...")
                self.conn.make_request('DELETE', self.name, None, 'cors')
        except AttributeError: pass

    def sync_lifecycle(self):
        try:
            if self.lifecycle is not None:
                log.info("setting lifecycle configuration...")
                self.conn.make_request('PUT', self.name, None, 'lifecycle',
                                       data=self.lifecycle)
            else:
                log.info("deleting lifecycle configuration...")
                self.conn.make_request('DELETE', self.name, None, 'lifecycle')
        except AttributeError: pass

    def sync_logging(self):
        try:
            if self.logging is not None:
                log.info("setting logging configuration...")
                data = self.logging
            else:
                log.info("deleting logging configuration...")
                data = '<?xml version="1.0" encoding="UTF-8"?>\
                        <BucketLoggingStatus xmlns="http://doc.s3.amazonaws.com/2006-03-01" />'
            self.conn.make_request('PUT', self.name, None, 'logging', data=data)
        except AttributeError: pass

    def sync_notification(self):
        try:
            if self.notification is not None:
                log.info("setting notification configuration...")
                data = self.notification
            else:
                log.info("deleting notification configuration...")
                data = '<NotificationConfiguration />'
            self.conn.make_request('PUT', self.name, None, 'notification', data=data)
        except AttributeError: pass

    def sync_policy(self):
        try:
            if self.policy is not None:
                log.info("setting bucket policy...")
                self.conn.make_request('PUT', self.name, None, 'policy', data=self.policy)
            else:
                log.info("deleting bucket policy...")
                self.conn.make_request('DELETE', self.name, None, 'policy')
        except AttributeError: pass

    def sync_tagging(self):
        try:
            if self.tagging is not None:
                log.info("setting bucket tags...")
                self.conn.make_request('PUT', self.name, None, 'tagging', data=self.tagging)
            else:
                log.info("deleting bucket tags...")
                self.conn.make_request('DELETE', self.name, None, 'tagging')
        except AttributeError: pass

    def sync_versioning(self):
        try:
            if self.versioning:
                log.info("enabling versioning...")
                status = 'Enabled'
            else:
                log.info("suspending versioning...")
                status = 'Suspended'
            data = '<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">\
                      <Status>{}</Status>\
                    </VersioningConfiguration>'.format(status)
            self.conn.make_request('PUT', self.name, None, 'versioning', data=data)
        except AttributeError: pass

    def sync_website(self):
        try:
            if self.website is not None:
                log.info("setting website configuration...")
                self.conn.make_request('PUT', self.name, None, 'website',
                                       data=self.website)
            else:
                log.info("deleting website configuration...")
                self.conn.make_request('DELETE', self.name, None, 'website')
        except AttributeError: pass