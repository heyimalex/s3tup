from nose.tools import raises

from s3tup.bucket import Bucket

from utils import ConnMock

@raises(TypeError)
def test_bucket_init_invalid_kwarg():
    Bucket(None, None, None, invalid='invalid')

@raises(TypeError)
def test_bucket_init_missing_name():
    Bucket(None)

def test_bucket_init_success():
    b = Bucket(None, 'test', acl='test')
    assert b.name == 'test'
    assert b.acl == 'test'