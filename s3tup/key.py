import logging
import mimetypes
import os

import utils
import constants



log = logging.getLogger('s3tup.key')

class KeyFactory(object):
    """
    Basically just a container for KeyConfigurators. make_key will create a
    key based on the input parameters and then run each configurator on it
    sequentially, returning a fully configured key ready for sync.

    """

    def __init__(self, configs=[]):
        self.configurators = []
        for c in configs:
            self.add_key_configurator(**c)

    def add_key_configurator(self, **kwargs):
        """Add a configurator to this factory.

        kwargs are everything in constants.KEY_ATTRS plus the matcher fields:
        'patterns', 'ignore_patterns', 'regexes', and 'ignore_regexes'.

        """
        matcher = utils.Matcher(
            kwargs.pop('patterns', None),
            kwargs.pop('ignore_patterns', None),
            kwargs.pop('regexes', None),
            kwargs.pop('ignore_regexes', None),
        )
        self.configurators.append(KeyConfigurator(matcher=matcher, **kwargs))

    def make_key(self, conn, key_name, bucket_name):
        """Return a properly configured key"""
        key = Key(conn, key_name, bucket_name)
        for c in self.configurators:
            if c.effects_key(key.name):
                key = c.configure(key)
        return key


class KeyConfigurator(object):

    def __init__(self, matcher=None, **kwargs):
        self.matcher = matcher

        for k, v in kwargs.iteritems():
            if k in constants.KEY_ATTRS:
                self.__dict__[k] = v
            else:
                raise TypeError("KeyConfigurator.__init__() got an"
                                " unexpected keyword argument'{}'"
                                 .format(attr))

    def effects_key(self, key_name):
        """Return whether this configurator effects key_name"""
        try: return self.matcher.match(key_name)
        except AttributeError: return True

    def configure(self, key):
        """Return the input key with all configurations applied"""
        for attr in constants.KEY_ATTRS:
            if attr in self.__dict__ and attr != 'metadata':
                key.__dict__[attr] = self.__dict__[attr]

        try: key.metadata.update(self.metadata)
        except AttributeError: pass

        return key


class Key(object):
    """
    Encapsulates configuration for a particular s3 key. It has attributes
    (all defined in constants.KEY_ATTRS) that you can set, delete, modify, 
    and then sync to s3 using the sync or rsync methods.
    """
    
    def __init__(self, conn, name, bucket_name, **kwargs):
        self.conn = conn
        self.name = name
        self.bucket_name = bucket_name

        # Set defaults for required attributes
        self.reduced_redundancy = kwargs.pop('reduced_redundancy', False)
        self.encrypt = kwargs.pop('encrypt', False)
        self.metadata = kwargs.pop('metadata', {})

        for k,v in kwargs.iteritems():
            if k in constants.KEY_ATTRS:
                self.__dict__[k] = v
            else:
                raise TypeError("Key.__init__() got an unexpected keyword"
                                " argument '{}'".format(k))

    @property
    def headers(self):
        """Return the headers associated with this key"""
        headers = {}

        try: headers['x-amz-acl'] = self.canned_acl
        except AttributeError: pass

        try:
            if self.reduced_redundancy:
                headers['x-amz-storage-class'] = 'REDUCED_REDUNDANCY'
        except AttributeError: pass

        try: 
            if self.encrypt:
                headers['x-amz-server-side-encryption'] = 'AES256'
        except AttributeError: pass

        for k, v in self.metadata.iteritems():
            headers['x-amz-meta-' + k] = v

        for k in constants.KEY_HEADERS:
            try:
                if self.__dict__[k] is not None:
                    headers[k.replace('_', '-')] = self.__dict__[k]
            except KeyError: pass

        # Guess content-type
        if 'content-type' not in headers:
            content_type_guess = mimetypes.guess_type(self.name)[0]
            if content_type_guess is not None:
                headers['Content-Type'] = content_type_guess

        return headers

    def sync(self):
        """Sync this object's configuration with its respective key on s3"""
        log.info("syncing key '{}'...".format(self.name))

        headers = self.headers
        headers['x-amz-copy-source'] = '/'+self.bucket_name+'/'+self.name
        headers['x-amz-metadata-directive'] = 'REPLACE'

        self.conn.make_request('PUT', self.bucket_name, self.name,
                               headers=headers)
        self.sync_acl()

    def rsync(self, file_like_object):
        """Upload file_like_object to s3 with this object's configuration"""
        log.info("uploading key '{}'...".format(self.name))

        headers = self.headers
        data = file_like_object.read()

        self.conn.make_request('PUT', self.bucket_name, self.name,
                               headers=headers, data=data)
        self.sync_acl()

    def sync_acl(self):
        try: acl = self.acl
        except AttributeError: return False

        if acl is not None:
            self.conn.make_request('PUT', self.bucket_name, self.name, 'acl',
                                   data=acl)
        else:
            self.conn.make_request('PUT', self.bucket_name, self.name, 'acl',
                                   headers={'x-amz-acl': 'private'})
        
        