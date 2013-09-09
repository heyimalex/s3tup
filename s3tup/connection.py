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

from exception import S3Error, SecretAccessKeyNotFound, AccessKeyIdNotFound

log = logging.getLogger('s3tup.connection')

stats_lock = Lock()
stats = {'GET':0, 'POST':0, 'PUT':0, 'DELETE':0, 'HEAD':0}

class Connection(object):

    def __init__(self, access_key_id=None, secret_access_key=None):

        if access_key_id is None:
            try: access_key_id = os.environ['AWS_ACCESS_KEY_ID']
            except KeyError:
                raise AccessKeyIdNotFound('Connection object could not be created: You must either supply the access_key_id parameter or set the AWS_ACCESS_KEY_ID environment variable.')

        if secret_access_key is None:
            try: secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
            except KeyError:
                raise SecretAccessKeyNotFound('Connection object could not be created: You must either supply the secret_access_key parameter or set the AWS_SECRET_ACCESS_KEY environment variable.')

        self.access_key_id = access_key_id
        self.secret_access_key = secret_access_key

    def make_request(self, method, bucket, key=None, params=None, data=None,
                     headers=None):

        # Remove params that are set to None
        if isinstance(params, dict):
            for k,v in params.copy().iteritems():
                if v is None:
                    params.pop(k)

        # Construct target url
        url = 'http://{}.s3.amazonaws.com'.format(bucket)
        url += '/{}'.format(key) if key is not None else '/'
        if isinstance(params, dict) and len(params) > 0:
            url += '?{}'.format(urllib.urlencode(params))
        elif isinstance(params, basestring):
            url += '?{}'.format(params)

        # Make headers case insensitive
        if headers is None:
            headers = {}
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

        # Construct canonicalized amz headers string
        canonicalized_amz_headers = ''
        amz_keys = [k for k in headers.iterkeys() if k.startswith('x-amz-')]
        for k in sorted(amz_keys):
            v = headers[k].strip()
            canonicalized_amz_headers += '{}:{}\n'.format(k.lower(), v)

        # Construct canonicalized resource string
        canonicalized_resource = '/' + bucket
        canonicalized_resource += '/' if key is None else '/{}'.format(key)
        if isinstance(params, basestring):
            canonicalized_resource += '?{}'.format(params)

        # Construct string to sign
        string_to_sign = method.upper() + '\n'
        string_to_sign += md5 + '\n'
        string_to_sign += content_type + '\n'
        string_to_sign += '\n' # date is always set through x-amz-date
        string_to_sign += canonicalized_amz_headers + canonicalized_resource

        # Create signature
        h = hmac.new(self.secret_access_key, string_to_sign, hashlib.sha1)
        signature = b64encode(h.digest())

        # Set authorization header
        headers['Authorization'] = 'AWS {}:{}'.format(self.access_key_id,
                                                      signature)

        # Prepare Request
        s = Session()
        req = Request(method, url, data=data, headers=headers).prepare()

        # Log request data
        # Combine into a single message so we don't have to bother with
        # locking to make lines appear together
        log_message = '{} {}\n'.format(method, url)
        log_message += 'headers:'
        for k in sorted(req.headers.iterkeys()):
            log_message += '\n {}: {}'.format(k, req.headers[k])
        log.debug(log_message)

        # Send request
        resp = s.send(req)
        log.debug('response: {} ({} {})'.format(resp.status_code, 
                                                    method, url))
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
            raise S3Error("{}: {}".format(code, message))

        return resp