from mock import MagicMock

class ConnMock(object):
    def __init__(self):
        self.make_request = MakeRequestMock()

# Needed to ignore redundant default kwargs
# That is, MagicMock didn't recognize that 'data=None'
# is the same as 'data' not being present at all.
# MakeRequestMock removes None/default kwargs all together
# from both __call__ and assert_called_once_with.
# It also reads data if data is a file like object.
class MakeRequestMock(MagicMock):
    def __call__(self, *args, **kwargs):
        kwargs = self.clean_kwargs(kwargs)
        super(MakeRequestMock, self).__call__(*args, **kwargs)

    def assert_called_once_with(self, *args, **kwargs):
        kwargs = self.clean_kwargs(kwargs)
        super(MakeRequestMock, self).assert_called_once_with(*args, **kwargs)

    @staticmethod
    def clean_kwargs(kwargs):
        cleaned = {}
        for k,v in kwargs.iteritems():
            if k == 'headers' and v in (None, {}):
                pass
            elif k == 'data' and v in ('', None):
                pass
            elif k == 'data' and is_readable(v):
                cleaned[k] = v.read()
            elif v is not None:
                cleaned[k] = v
        return cleaned

def is_readable(f):
    return callable(getattr(f, "read", None))