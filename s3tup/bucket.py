import logging

from connection import Connection
from key import KeyFactory
from rsync import rsync
import utils
import constants

log = logging.getLogger('s3tup.bucket')

class BucketFactory(object):

    def __init__(self, conn=None):
        self.conn = conn

    def make_bucket(self, **kwargs):
        bucket_name = kwargs.pop('bucket')

        try:
            access_key_id = kwargs.pop('access_key_id')
            secret_access_key = kwargs.pop('secret_access_key')
            conn = Connection(access_key_id, secret_access_key)
        except KeyError:
            conn = self.conn

        bucket = Bucket(conn, bucket_name, **kwargs)
        return bucket
        

class Bucket(object):

    def __init__(self, conn, name, **kwargs):
        self.conn = conn
        self.name = name

        for attr in kwargs:
            if attr in constants.BUCKET_ATTRS:
                self.__dict__[attr] = kwargs[attr]
            else:
                raise TypeError("Bucket.__init__() got an unexpected keyword\
                                 argument '{}'".format(attr))

    def sync(self, rsync_only=False):

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

        # TODO- implement all of these methods
        # self.sync_acl()
        # self.sync_logging()
        # self.sync_notification()
        # self.sync_policy()
        # self.sync_tagging()
        # self.sync_versioning()
        self.sync_lifecycle()
        self.sync_cors()
        self.sync_website()

        try: # create key factory from key_config if it's set
            kf = KeyFactory(self.conn, self.name, self.key_config)
        except AttributeError: kf = KeyFactory(self.conn, self.name)

        if 'rsync' in self.__dict__:
            rs_out = rsync(kf, **self.rsync)
            unmodified = rs_out['unmodified']
        else:
            unmodified = [k['name'] for k in utils.list_bucket(self.conn, self.name)]

        if not rsync_only:
            if 'key_config' in self.__dict__:
                for k in unmodified:
                    key = kf.make_key(k)
                    key.sync()

        log.info("bucket '{}' sucessfully synced!\n".format(self.name))

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