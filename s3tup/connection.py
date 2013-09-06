from base64 import b64encode
from email.utils import formatdate
from threading import Lock
import os
import logging
import hashlib
import hmac
import urllib

from bs4 import BeautifulSoup
from requests import Session, Request
from requests.structures import CaseInsensitiveDict

log = logging.getLogger('s3tup.connection')

stats_lock = Lock()
stats = {'GET':0, 'POST':0, 'PUT':0, 'DELETE':0, 'HEAD':0}

class S3Exception(Exception):
    pass


class Connection(object):

    def __init__(self, access_key_id=None, secret_access_key=None):

        if access_key_id is None:
            try: access_key_id = os.environ['AWS_ACCESS_KEY_ID']
            except KeyError: raise Exception('You must supply an aws access key id.')

        if secret_access_key is None:
            try: secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
            except KeyError: raise Exception('You must supply an aws secret access key.')

        self.access_key_id = access_key_id
        self.secret_access_key = secret_access_key

    def make_request(self, method, bucket, key=None, params=None, headers={}, data=None):

        # Remove params that are set to None
        if isinstance(params, dict):
            for k,v in params.copy().iteritems():
                if v is None:
                    params.pop(k)

        # Construct target url
        url = 'http://{}.s3.amazonaws.com'.format(bucket)
        url += '/{}'.format(key) if key is not None else '/'
        try: url += '?{}'.format(urllib.urlencode(params))
        except TypeError:
            url += '?{}'.format(params) if params is not None else ''

        # Make headers case insensitive
        headers = CaseInsensitiveDict(headers)

        headers['Host'] = '{}.s3.amazonaws.com'.format(bucket)

        if data is not None:
            m = hashlib.md5()
            m.update(data)
            md5 = b64encode(m.digest())
            headers['Content-MD5'] = md5
        else:
            md5 = ''

        try: content_type = headers['Content-Type']
        except KeyError: content_type = ''

        date = formatdate(timeval=None, localtime=False, usegmt=True)
        headers['x-amz-date'] = date

        # Construct canonicalized resource string
        canonicalized_resource = '/' + bucket
        canonicalized_resource += '/{}'.format(key) if key is not None else '/'
        if isinstance(params, basestring):
            canonicalized_resource += '?{}'.format(params)

        # Construct canonicalized amz headers string
        canonicalized_amz_headers = ''
        amz_keys = sorted([k for k in headers.keys() if k.startswith('x-amz-')])
        for k in amz_keys:
            canonicalized_amz_headers += '{}:{}\n'.format(k, headers[k].strip())

        # Construct string to sign
        string_to_sign = method.upper() + '\n'
        string_to_sign += md5 + '\n'
        string_to_sign += content_type + '\n'
        string_to_sign += '\n' # date is always done through x-amz-date header
        string_to_sign += canonicalized_amz_headers + canonicalized_resource

        # Create signature
        h = hmac.new(self.secret_access_key, string_to_sign, hashlib.sha1)
        signature = b64encode(h.digest())

        # Set authorization header
        headers['Authorization'] = 'AWS {}:{}'.format(self.access_key_id, signature)

        # Prepare Request
        s = Session()
        req = Request(method, url, data=data, headers=headers).prepare()

        # Log request data
        log.debug('{} {}'.format(method, url))
        log.debug('headers:')
        for k in sorted(req.headers.iterkeys()):
            log.debug(' {}: {}'.format(k, req.headers[k]))

        # Send request
        resp = s.send(req)
        log.debug('response: {}\n'.format(resp.status_code))

        # Update stats
        with stats_lock:
            stats[method.upper()] += 1
        
        # Handle errors
        if resp.status_code/100 != 2:
            log.error("S3 replied with non 2xx response code!!!!")
            log.error('  request: {} {}'.format(method, url))
            soup = BeautifulSoup(resp.text)
            error = soup.find('error')
            for c in error.children:
                log.error('  {}: {}'.format(c.name, c.text))

            code = error.find('code').text
            message = error.find('message').text
            raise S3Exception("{}: {}".format(code, message))

        return resp

