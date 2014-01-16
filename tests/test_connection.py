import os

from nose.tools import raises

from s3tup.connection import Connection
from s3tup.exception import AwsCredentialNotFound

@raises(AwsCredentialNotFound)
def test_connection_init_no_credentials():
    os.environ.pop('AWS_ACCESS_KEY_ID', None)
    os.environ.pop('AWS_SECRET_ACCESS_KEY', None)
    Connection()

def test_connection_init_explicit_credentials():
    os.environ.pop('AWS_ACCESS_KEY_ID', None)
    os.environ.pop('AWS_SECRET_ACCESS_KEY', None)
    c = Connection('explicit_access_key_id', 'explicit_secret_access_key')
    assert c.access_key_id == 'explicit_access_key_id'
    assert c.secret_access_key == 'explicit_secret_access_key'

def test_connection_init_environ_credentials():
    os.environ['AWS_ACCESS_KEY_ID'] = 'environ_access_key_id'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'environ_secret_access_key'
    c = Connection()
    assert c.access_key_id == 'environ_access_key_id'
    assert c.secret_access_key == 'environ_secret_access_key'

def test_connection_init_explicit_supercedes_environ():
    os.environ['AWS_ACCESS_KEY_ID'] = 'environ'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'environ'
    c = Connection('explicit', 'explicit')
    assert c.access_key_id == 'explicit'
    assert c.secret_access_key == 'explicit'

#hey