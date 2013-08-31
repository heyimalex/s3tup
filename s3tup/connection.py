from base64 import b64encode
from email.utils import formatdate
import logging
import hashlib
import hmac
import urllib

from bs4 import BeautifulSoup
import requests

log = logging.getLogger('s3tup.connection')
stats = {'GET':0, 'POST':0, 'PUT':0, 'DELETE':0, 'HEAD':0}

class Connection(object):

    def __init__(self, access_key_id, secret_access_key):
        self.access_key_id = access_key_id
        self.secret_access_key = secret_access_key

    def make_request(self, method, bucket, key=None, params=None, headers={}, data=None):

        # Construct target url
        url = 'http://{}.s3.amazonaws.com'.format(bucket)
        url += '/{}'.format(key) if key is not None else '/'
        try: url += '?{}'.format(urllib.urlencode(params))
        except TypeError:
            url += '?{}'.format(params) if params is not None else ''

        # Lowercase all header keys
        headers = {k.lower():v for k,v in headers.iteritems()}

        headers['host'] = '{}.s3.amazonaws.com'.format(bucket)

        if 'content-md5' in headers:
            md5 = headers['content-md5']
        elif data is not None:
            m = hashlib.md5()
            m.update(data)
            md5 = b64encode(m.digest())
            headers['content-md5'] = md5
        else:
            md5 = ''

        try: content_type = headers['content-type']
        except KeyError: content_type = ''

        try: date = headers['x-amz-date']
        except KeyError:
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

        # Logging stuff
        log.debug('{}: {}'.format(method, url))
        log.debug('headers:')
        for k, v in headers.iteritems():
            log.debug(' {}: {}'.format(k, v))

        r = requests.request(method, url, headers=headers, data=data)
        log.debug('response: {}\n'.format(r.status_code))
        stats[method.upper()] += 1
        
        # Handle errors
        if r.status_code/100 != 2:
            log.error("s3 responded with non 2xx response code!")
            log.error("response code: {}".format(r.status_code))
            log.error("request headers:")
            for k,v in r.request.headers.iteritems():
                log.error("  {}: {}".format(k,v))
            try:
                soup = BeautifulSoup(r.text)
                error = soup.find('error')
                for c in error.children:
                    log.error('{}: {}'.format(c.name, c.text))
            except:pass
            raise Exception("s3 responded with non 2xx response code")

        return r

