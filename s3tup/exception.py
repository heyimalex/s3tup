class S3ResponseError(Exception):
    def __init__(self, error_code, description, response):
        msg = "{}: {}".format(error_code, description)
        super(S3ResponseError, self).__init__(msg)
        self.error_code = error_code
        self.description = description
        self.status_code = response.status_code
        self.raw_response = response


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
