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
    """Return a properly configured Bucket object from a s3tup configuration.

    Takes every kwarg detailed in the bucket config section of the readme.
    Also takes an optional Connection object as the first parameter. If the
    'access_key_id' and 'secret_access_key' kwargs are not set, this
    connection will be used in the bucket returned. If no connection object
    is passed, it will just call the default Connection constructor which
    will attempt to read your credentials from your env vars.

    """

    bucket_name = kwargs.pop('bucket')

    try:
        access_key_id = kwargs.pop('access_key_id')
        secret_access_key = kwargs.pop('secret_access_key')
        conn = Connection(access_key_id, secret_access_key)
    except KeyError:
        pass

    if conn is None:
        conn = Connection()
        

    if 'key_config' in kwargs:
        key_factory = KeyFactory(kwargs.pop('key_config'))
    else:
        key_factory = None

    return Bucket(conn, bucket_name, key_factory, **kwargs)


class Bucket(object):
    """
    Encapsulates configuration for an s3 bucket and its keys. It contains a
    key_factory which handles key configuration and many attributes (listed
    in constants.BUCKET_ATTRS) that you can set, delete, modify, and then
    sync to s3 using the various sync methods provided.
    """

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
        """Return a properly configured Key object."""
        if self.key_factory is None:
            return Key(self.conn, key_name, self.name)
        else:
            return self.key_factory.make_key(self.conn, key_name, self.name)

    def make_request(self, method, params=None, data=None, headers=None):
        """
        Convenience method for self.conn.make_request; has the bucket and
        key fields already filled in.
        """
        return self.conn.make_request(method, self.name, None, params,
                                      data=data, headers=headers)

    # Named get_remote_keys instead of just overriding __iter__
    # to avoid ambiguity. Returns dicts instead of objects for the
    # same reason.
    def get_remote_keys(self, prefix=None):
        """Generate list of dicts representing all keys in this s3 bucket.

        Each dict returned contains fields 'name', 'etag', 'size', and
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
                etag_hex = c.find('etag').text.replace('"', '')
                etag_bin = binascii.unhexlify(etag_hex)
                etag = binascii.b2a_base64(etag_bin).strip()
                yield {'name': key, 'etag': etag, 'size': size,
                       'modified': modified}
            more = root.find('istruncated').text == 'true'

    def sync(self, rsync_only=False):
        """Sync everything.

        Takes every applicable attribute set on this Bucket object and
        configures its respective s3 bucket (defined by self.name) to match
        it. Optional rsync only mode will only run rsync (no setting bucket
        configuration, no syncing unmodified keys, no making redirects).

        """
        log.info("syncing bucket '{}'...".format(self.name))

        # Create the bucket
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
        not provided, it will run on all keys currently in the s3 bucket. 

        """

        if self.key_factory is None:
            return
        if keys is None:
            keys = [k['name'] for k in self.get_remote_keys()]
        for k in keys:
            key = self.make_key(k)
            key.sync()

    def sync_redirects(self):
        """Create all redirects defined in self.redirects"""
        for k,v in self.redirects:
            log.info("creating redirect from {} to {}".format(k, v))
            headers = {'x-amz-website-redirect-location': v}
            self.conn.make_request('PUT', self.name, k, headers=headers)

    def rsync_keys(self):
        """Run rsync for every rsync config in self.rsync"""
        try: rsync_configs = self.rsync
        except AttributeError: return False

        # Accept either a dict or a list of dicts
        if isinstance(rsync_configs, dict):
            rsync_configs = [rsync_configs,]

        for cfg in rsync_configs:
            matcher = utils.Matcher(
                cfg.pop('patterns', None),
                cfg.pop('ignore_patterns', None),
                cfg.pop('regexes', None),
                cfg.pop('ignore_regexes', None),
            )
            rsync(self, matcher=matcher, **cfg)

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
            data = '<?xml version="1.0" encoding="UTF-8"?>\
                    <BucketLoggingStatus \
                    xmlns="http://doc.s3.amazonaws.com/2006-03-01"/>'
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
        data = '<VersioningConfiguration \
                xmlns="http://s3.amazonaws.com/doc/2006-03-01/">\
                <Status>{}</Status>\
                </VersioningConfiguration>'.format(status)
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