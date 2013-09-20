class S3ResponseError(Exception):
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