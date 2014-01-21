from base64 import b64encode
from email.utils import formatdate
from contextlib import contextmanager
import os
import logging
import hashlib
import hmac
import urllib

import gevent
from gevent.pool import Pool
from gevent import monkey
monkey.patch_all(thread=False, select=False)

# Make greenlets not print traceback info on exception.
# I imagine this isn't a good thing to do, but pushing a
# traceback to stdout on a cli is a no go.
from gevent.hub import Hub
Hub.print_exception = lambda *args, **kwargs: None

from bs4 import BeautifulSoup
from requests import Session, Request
from requests.structures import CaseInsensitiveDict

from s3tup.exception import S3ResponseError, AccessKeyIdNotFound, \
                            SecretAccessKeyNotFound
import s3tup.utils as utils

log = logging.getLogger('s3tup.connection')


class Connection(object):

    def __init__(self, access_key_id=None, secret_access_key=None,
                 hostname=None, concurrency=5):
        if access_key_id is None:
            try:
                access_key_id = os.environ['AWS_ACCESS_KEY_ID']
            except KeyError:
                raise AccessKeyIdNotFound()
        if secret_access_key is None:
            try:
                secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
            except KeyError:
                raise SecretAccessKeyNotFound()
        if hostname is None:
            hostname = "s3.amazonaws.com"

        self.access_key_id = access_key_id
        self.secret_access_key = secret_access_key
        self.hostname = hostname

        self.concurrency = concurrency
        self._joined = False

        self.stats = {'GET': 0, 'POST': 0, 'PUT': 0, 'DELETE': 0, 'HEAD': 0}

    @property
    def concurrency(self):
        return self._concurrency

    @concurrency.setter
    def concurrency(self, val):
        try:
            self._pool.join()
        except AttributeError:
            pass
        if val > 0:
            self._pool = Pool(val)
        self._concurrency = val

    # Join requires some strange context management because it's
    # possible for joined methods to themselves call join. If
    # these methods then saturate the pool, the joins that they're
    # waiting on will never complete. To counteract this we allow
    # another greenlet into the pool for the duration of the join
    # call *if* the join is within a join already.
    @contextmanager
    def joincontext(self):
        old = self._joined
        self._joined = True
        if old:
            self._pool._semaphore.counter += 1
            yield
            self._pool._semaphore.counter -= 1
        else:
            yield
        self._joined = old

    def join(self, functions):
        if self.concurrency <= 0:  # Useful for debugging
            out = []
            for f in functions:
                if hasattr(f, '__iter__'):
                    out.append(f[0](*f[1:]))
                else:
                    out.append(f())
            return out

        with self.joincontext():
            greenlets = []
            for f in functions:
                if hasattr(f, '__iter__'):
                    greenlet = gevent.spawn(*f)
                else:
                    greenlet = gevent.spawn(f)
                self._pool.add(greenlet)
                greenlets.append(greenlet)
            gevent.joinall(greenlets, raise_error=True)
            return [g.get() for g in greenlets]

    # Here be dragons
    def make_request(self, method, bucket, key=None, params=None, data=None,
                     headers=None):
        # Remove params that are set to None
        if isinstance(params, dict):
            for k, v in params.copy().items():
                if v is None:
                    params.pop(k)

        # Construct target url
        url = 'http://{}.{}'.format(bucket, self.hostname)
        url += '/{}'.format(key) if key is not None else '/'
        if isinstance(params, dict) and len(params) > 0:
            url += '?{}'.format(urllib.urlencode(params))
        elif isinstance(params, basestring):
            url += '?{}'.format(params)

        # Make headers case insensitive
        if headers is None:
            headers = {}
        headers = CaseInsensitiveDict(headers)

        headers['Host'] = '{}.{}'.format(bucket, self.hostname)

        if data is not None:
            try:
                raw_md5 = utils.f_md5(data)
            except:
                m = hashlib.md5()
                m.update(data)
                raw_md5 = m.digest()
            md5 = b64encode(raw_md5)
            headers['Content-MD5'] = md5
        else:
            md5 = ''

        try:
            content_type = headers['Content-Type']
        except KeyError:
            content_type = ''

        date = formatdate(timeval=None, localtime=False, usegmt=True)
        headers['x-amz-date'] = date

        # Construct canonicalized amz headers string
        canonicalized_amz_headers = ''
        amz_keys = [k for k in list(headers.keys()) if k.startswith('x-amz-')]
        for k in sorted(amz_keys):
            v = headers[k].strip()
            canonicalized_amz_headers += '{}:{}\n'.format(k.lower(), v)

        # Construct canonicalized resource string
        canonicalized_resource = '/' + bucket
        canonicalized_resource += '/' if key is None else '/{}'.format(key)
        if isinstance(params, basestring):
            canonicalized_resource += '?{}'.format(params)
        elif isinstance(params, dict) and len(params) > 0:
            canonicalized_resource += '?{}'.format(urllib.urlencode(params))

        # Construct string to sign
        string_to_sign = method.upper() + '\n'
        string_to_sign += md5 + '\n'
        string_to_sign += content_type + '\n'
        string_to_sign += '\n'  # date is always set through x-amz-date
        string_to_sign += canonicalized_amz_headers + canonicalized_resource

        # Create signature
        h = hmac.new(self.secret_access_key, string_to_sign, hashlib.sha1)
        signature = b64encode(h.digest())

        # Set authorization header
        auth_head = 'AWS {}:{}'.format(self.access_key_id, signature)
        headers['Authorization'] = auth_head

        # Prepare Request
        req = Request(method, url, data=data, headers=headers).prepare()

        # Log request data.
        # Prepare request beforehand so requests-altered headers show.
        # Combine into a single message so we don't have to bother with
        # locking to make lines appear together.
        log_message = '{} {}\n'.format(method, url)
        log_message += 'headers:'
        for k in sorted(req.headers.keys()):
            log_message += '\n {}: {}'.format(k, req.headers[k])
        log.debug(log_message)

        # Send request
        resp = Session().send(req)

        # Update stats, log response data.
        self.stats[method.upper()] += 1
        log.debug('response: {} ({} {})'.format(resp.status_code, method, url))

        # Handle errors
        if resp.status_code/100 != 2:
            soup = BeautifulSoup(resp.text)
            error = soup.find('error')

            log_message = "S3 replied with non 2xx response code!!!!\n"
            log_message += '  request: {} {}\n'.format(method, url)
            for c in error.children:
                error_name = c.name
                error_message = c.text.encode('unicode_escape')
                log_message += '  {}: {}\n'.format(error_name, error_message)
            log.debug(log_message)

            code = error.find('code').text
            message = error.find('message').text
            raise S3ResponseError(code, message, resp)

        return resp
