from collections import namedtuple
import logging

from bs4 import BeautifulSoup

from s3tup.key import KeyFactory, redirect_key, delete_key
from s3tup.rsync import RsyncPlanner, ActionPlan
import s3tup.constants as constants

log = logging.getLogger('s3tup.bucket')


class Bucket(object):

    """Encapsulates configuration for an s3 bucket and its keys.

    Buckets contain a KeyFactory, which handles key configuration,
    an RsyncPlanner, which decides which actions to take on keys,
    and attributes (listed in constants.BUCKET_ATTRS) that you can set,
    delete, modify, and then sync to s3 using the various sync methods
    provided.

    """

    def __init__(self, conn, name, key_factory=None,
                 rsync_planner=None, **kwargs):
        self.conn = conn
        self.name = name
        self.key_factory = key_factory or KeyFactory()
        self.rsync_planner = rsync_planner or RsyncPlanner()
        self.redirects = kwargs.pop('redirects', [])

        # Add all kwargs passed in that are named in
        # constants.BUCKET_ATTRS to this object instance
        for k, v in kwargs.items():
            if k in constants.BUCKET_ATTRS:
                self.__dict__[k] = v
            else:
                msg = ("__init__() got an unexpected keyword"
                       " argument '{}'".format(k))
                raise TypeError(msg)

    def make_request(self, method, params=None, data=None, headers=None):
        """Convenience method for self.conn.make_request."""
        # Has bucket and key fields already filled in.
        return self.conn.make_request(
            method=method,
            bucket=self.name,
            key=None,
            params=params,
            data=data,
            headers=headers
        )

    # KEY METHODS

    def make_key(self, key_name):
        return self.key_factory.make_key(self.conn, self.name, key_name)

    def sync_key(self, key_name):
        key = self.make_key(key_name)
        key.sync()

    def upload_key(self, key_name, path):
        key = self.make_key(key_name)
        key.upload(open(path.replace(" ", "\\ "), 'rb'))

    def redirect_key(self, key_name, redirect_url):
        redirect_key(self.conn, self.name, key_name, redirect_url)

    def delete_key(self, key_name):
        delete_key(self.conn, self.name, key_name)

    def delete_keys(self, key_names):
        """Delete a list of keys from this bucket.

        key_names is a list of str key names to be deleted. S3's delete
        operation has a limit of 1000 keys per request, so this method
        handles paging automatically as well.

        """
        for i in range(0, len(key_names), 1000):
            data = ('<?xml version="1.0" encoding="UTF-8"?>'
                    '<Delete><Quiet>true</Quiet>')
            for k in key_names[i:i+1000]:
                log.info('delete: s3://{}/{}'.format(self.name, k))
                data += '<Object><Key>{}</Key></Object>'.format(k)
            data += '</Delete>'
            # TODO: Make this joinable.
            self.make_request('POST', 'delete', data=data)

    # Named get_remote_keys instead of just overriding __iter__
    # to avoid ambiguity. Doesn't return s3tup.key.Key objects for the
    # same reason. Requests can't be parallel as each depends on
    # the marker from the last.
    def get_remote_keys(self, prefix=None):
        """Return list representing all keys in this bucket.

        Each namedtuple returned contains fields 'name', 'md5', 'size',
        and 'modified'. Paging is handled automatically. Optional (str)
        prefix param will limit the results to those keys prefixed by it.

        """
        KeyTuple = namedtuple('KeyTuple', ['name', 'md5', 'size', 'modified'])
        keys = {}
        more = True
        marker = None

        while more:

            params = {'marker': marker, 'prefix': prefix}
            resp = self.make_request('GET', params)

            root = BeautifulSoup(resp.text).find('listbucketresult')
            for c in root.find_all('contents'):
                key = c.find('key').text
                marker = key
                modified = c.find('lastmodified').text
                size = int(c.find('size').text)
                md5 = c.find('etag').text.replace('"', '')
                keys[key] = KeyTuple(key, md5, size, modified)

            more = root.find('istruncated').text == 'true'

        return keys

    # SYNC METHODS

    def sync(self, dryrun=False, rsync=False):
        """Sync all of this bucket's configurations.

        Takes every applicable attribute set on this Bucket object and
        configures its respective s3 bucket (defined by self.name) to match
        it. Optional rsync only mode will only run rsync (no setting bucket
        configuration, no syncing unmodified keys, no making redirects).

        """
        log.info("syncing bucket '{}'...".format(self.name))

        self.create()
        if not rsync:
            self.sync_bucket(dryrun=dryrun)
        self.sync_keys(dryrun=dryrun, rsync=rsync)

        log.info("bucket '{}' sucessfully synced!\n".format(self.name))

    def create(self):
        """Create this bucket."""
        try:
            headers = {'x-amz-acl': self.canned_acl}
        except AttributeError:
            headers = None
        try:
            data = ('<CreateBucketConfiguration '
                    'xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
                    '  <LocationConstraint>{}</LocationConstraint>'
                    '</CreateBucketConfiguration>').format(self.region)
        except AttributeError:
            data = None
        self.make_request('PUT', headers=headers, data=data)

    def sync_bucket(self, dryrun=False):
        if dryrun:
            tmp = self.make_request
            self.make_request = lambda *args, **kwargs: None
        self.conn.join([
            self.sync_acl,
            self.sync_cors,
            self.sync_lifecycle,
            self.sync_logging,
            self.sync_notification,
            self.sync_policy,
            self.sync_tagging,
            self.sync_versioning,
            self.sync_website,
        ])
        if dryrun:
            self.make_request = tmp

    def sync_keys(self, dryrun=False, rsync=False):
        plan = self._create_action_plan(rsync)
        if not dryrun:
            self._execute_action_plan(plan)
        else:
            for k, path in plan.to_upload:
                log.info("upload: {} <- {}".format(k, path))
            for k in plan.to_sync:
                log.info("sync: {}".format(k))
            for k, url in plan.to_redirect:
                log.info("redirect: {} -> {}".format(k, url))
            for k in plan.to_delete:
                log.info("delete: {}".format(k))

    def _create_action_plan(self, rsync=False):

        remote_keys = self.get_remote_keys()
        plan = self.rsync_planner.plan(remote_keys)

        # Add in redirects
        for key, url in self.redirects:
            plan.add_redirect(key, url)

        # Sync all keys with no action yet associated
        affected_keys = set(plan.affected_keys)
        old_keys = set(remote_keys.keys())
        for k in (old_keys-affected_keys):
            plan.add_sync(k)

        if rsync:
            plan.remove_actions('sync', 'redirect')

        return plan

    def _execute_action_plan(self, plan):
        actions = []
        for k, path in plan.to_upload:
            actions.append([self.upload_key, k, path])
        for k in plan.to_sync:
            actions.append([self.sync_key, k])
        for k, url in plan.to_redirect:
            actions.append([self.redirect_key, k, url])
        actions.append([self.delete_keys, list(plan.to_delete)])
        self.conn.join(actions)

    # INDIVIDUAL BUCKET SYNCING METHODS
    #
    # Each checks if this bucket has its respective attr set and, if it does,
    # proceeds to sync that value with the s3 bucket. Each will return s3's
    # response in the form of a requests.Response object if the attr is set,
    # and if not will return False. These map directly to the fields
    # defined in the bucket config section of the readme, so if you're
    # looking for details on what values are allowed check there.

    def sync_acl(self):
        try:
            acl = self.acl
        except AttributeError:
            return False

        if acl is not None:
            log.info("set xml acl")
            return self.make_request('PUT', 'acl', data=acl)
        else:
            log.info("revert to default acl")
            headers = {"x-amz-acl": "private"}
            return self.make_request('PUT', 'acl', headers=headers)

    def sync_cors(self):
        try:
            cors = self.cors
        except AttributeError:
            return False

        if cors is not None:
            log.info("set cors configuration")
            return self.make_request('PUT', 'cors', data=cors)
        else:
            log.info("delete cors configuration")
            return self.make_request('DELETE', 'cors')

    def sync_lifecycle(self):
        try:
            lifecycle = self.lifecycle
        except AttributeError:
            return False

        if lifecycle is not None:
            log.info("set lifecycle configuration")
            return self.make_request('PUT', 'lifecycle', data=lifecycle)
        else:
            log.info("delete lifecycle configuration")
            return self.make_request('DELETE', 'lifecycle')

    def sync_logging(self):
        try:
            logging = self.logging
        except AttributeError:
            return False

        if logging is not None:
            log.info("set logging configuration")
            data = logging
        else:
            log.info("delete logging configuration")
            data = ('<?xml version="1.0" encoding="UTF-8"?>'
                    '<BucketLoggingStatus '
                    'xmlns="http://doc.s3.amazonaws.com/2006-03-01"/>')
        return self.make_request('PUT', 'logging', data=data)

    def sync_notification(self):
        try:
            notification = self.notification
        except AttributeError:
            return False

        if notification is not None:
            log.info("set notification configuration")
            data = notification
        else:
            log.info("delete notification configuration")
            data = '<NotificationConfiguration />'
        return self.make_request('PUT', 'notification', data=data)

    def sync_policy(self):
        try:
            policy = self.policy
        except AttributeError:
            return False

        if policy is not None:
            log.info("set bucket policy")
            return self.make_request('PUT', 'policy', data=policy)
        else:
            log.info("delete bucket policy")
            return self.make_request('DELETE', 'policy')

    def sync_tagging(self):
        try:
            tagging = self.tagging
        except AttributeError:
            return False

        if tagging is not None:
            log.info("set bucket tags")
            return self.make_request('PUT', 'tagging', data=tagging)
        else:
            log.info("delete bucket tags")
            return self.make_request('DELETE', 'tagging')

    def sync_versioning(self):
        try:
            versioning = self.versioning
        except AttributeError:
            return False

        if versioning:
            log.info("enable versioning")
            status = 'Enabled'
        else:
            log.info("suspend versioning")
            status = 'Suspended'
        data = ('<VersioningConfiguration '
                'xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
                '  <Status>{}</Status>'
                '</VersioningConfiguration>').format(status)
        return self.make_request('PUT', 'versioning', data=data)

    def sync_website(self):
        try:
            website = self.website
        except AttributeError:
            return False

        if website is not None:
            log.info("set website configuration")
            return self.make_request('PUT', 'website', data=website)
        else:
            log.info("delete website configuration")
            return self.make_request('DELETE', 'website')
