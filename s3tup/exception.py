class S3ResponseError(Exception):
    def __init__(self, code, message):
        msg = "{}: {}".format(code, message)
        super(S3ResponseError, self).__init__(msg)

class ActionConflict(Exception):
    pass

class ConfigLoadError(Exception):
    pass

class ConfigParseError(Exception):
    pass

class AwsCredentialNotFound(Exception):
    pass

class SecretAccessKeyNotFound(AwsCredentialNotFound):
    pass

class AccessKeyIdNotFound(AwsCredentialNotFound):
    pass