from StringIO import StringIO

from nose.tools import raises

from s3tup.key import Key, KeyConfigurator
from s3tup.utils import Matcher

from utils import ConnMock

# KEY CONFIGURATOR

def test_key_configurator_configure_key():
    c = KeyConfigurator()
    c.reduced_redundancy = True
    c.encrypted = False
    c.cache_control = 'test'
    c.canned_acl = 'public-read'
    c.metadata = {'some':'metadata'}

    # Create new key and run configurator on it
    k = Key(None, 'bucket', 'key', canned_acl='private', encrypted=True,
            metadata={'some':'otherdata'}, cache_control='original')
    k = c.configure_key(k)

    # Confirm configurator worked as expected
    assert k.reduced_redundancy
    assert not k.encrypted
    assert k.cache_control == 'test'
    assert k.canned_acl == 'public-read'
    assert k.metadata['some'] == 'metadata'

def test_key_configurator_effects_key():
    c = KeyConfigurator(matcher=Matcher(['*.py']))
    assert c.effects_key('test.py')
    assert not c.effects_key('test.md')

# KEY

def test_key_pretty_path():
    k = Key(None, 'bucket', 'key')
    assert k.pretty_path == 's3://bucket/key'

def test_key_delete():
    conn = ConnMock()
    key = Key(conn, 'test', 'test')
    key.delete()
    conn.make_request.assert_called_once_with(
        'DELETE',
        'test',
        'test'
    )

def test_key_redirect():
    conn = ConnMock()
    key = Key(conn, 'test', 'test')
    key.redirect('url')
    conn.make_request.assert_called_once_with(
        'PUT',
        'test',
        'test',
        headers={'x-amz-website-redirect-location':'url'}
    )

def test_key_sync():
    conn = ConnMock()
    key = Key(conn, 'test', 'test')
    key.encrypted = True
    key.sync()
    expected_headers = {
        'x-amz-copy-source':'/test/test',
        'x-amz-metadata-directive':'REPLACE',
        'x-amz-server-side-encryption':'AES256'
    }
    conn.make_request.assert_called_once_with(
        'PUT',
        'test',
        'test',
        headers=expected_headers,
    )

def test_key_upload():
    conn = ConnMock()
    key = Key(conn, 'test', 'test')
    s = StringIO('test')
    key.upload(s)
    conn.make_request.assert_called_once_with(
        'PUT',
        'test',
        'test',
        data='test'
    )

def test_key_init():
    k = Key(
        None,
        'key',
        'bucket',
        cache_control = 'public',
        canned_acl = 'public-read',
        content_disposition = 'attachment',
        content_encoding = 'gzip',
        content_type = 'text/html',
        content_language = 'mi, en',
        encrypted = True,
        expires = 'Thu, 01 Dec 1994 16:00:00 GMT',
        metadata = {'k1': 'v1', 'k2': 'v2'},
        reduced_redundancy = True,
    )
    assert k.cache_control == 'public'
    assert k.content_encoding == 'gzip'
    assert k.content_disposition == 'attachment'
    assert k.content_language == 'mi, en'
    assert k.content_type == 'text/html'
    assert k.expires == 'Thu, 01 Dec 1994 16:00:00 GMT'
    assert k.canned_acl == 'public-read'
    assert k.encrypted == True
    assert k.reduced_redundancy == True
    assert k.metadata['k1'] == 'v1'
    assert k.metadata['k2'] == 'v2'

@raises(TypeError)
def test_key_init_invalid_kwarg():
    Key(None, None, None, invalid_kwarg=True)

def test_key_init_defaults():
    k = Key(None, None, None)
    assert not k.encrypted
    assert not k.reduced_redundancy
    assert k.metadata == {}

def test_key_headers():
    k = Key(None, 'bucket', 'key')

    k.cache_control = 'public'
    k.canned_acl = 'public-read'
    k.content_disposition = 'attachment'
    k.content_encoding = 'gzip'
    k.content_type = 'text/html'
    k.content_language = 'mi, en'
    k.encrypted = True
    k.expires = 'Thu, 01 Dec 1994 16:00:00 GMT'
    k.metadata = {'k1': 'v1', 'k2': 'v2'}
    k.reduced_redundancy = True

    headers = k.get_headers()

    assert headers['cache-control'] == 'public'
    assert headers['content-encoding'] == 'gzip'
    assert headers['content-disposition'] == 'attachment'
    assert headers['content-language'] == 'mi, en'
    assert headers['content-type'] == 'text/html'
    assert headers['expires'] == 'Thu, 01 Dec 1994 16:00:00 GMT'
    assert headers['x-amz-acl'] == 'public-read'
    assert headers['x-amz-server-side-encryption'] == 'AES256'
    assert headers['x-amz-storage-class'] == 'REDUCED_REDUNDANCY'
    assert headers['x-amz-meta-k1'] == 'v1'
    assert headers['x-amz-meta-k2'] == 'v2'
    assert len(headers) == 11

def test_key_headers_acl_is_none():
    k = Key(None, 'bucket', 'key')
    k.acl = None

    headers = k.get_headers()
    assert headers['x-amz-acl'] == 'private'

def test_key_headers_mimetype_guessing():
    k = Key(None, 'bucket', 'test.css')
    headers = k.get_headers()
    assert headers['content-type'] == 'text/css'

