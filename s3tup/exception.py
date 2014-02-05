class S3ResponseError(Exception):
    def __init__(self, error_code, description, response):
        msg = "{}: {}".format(error_code, description)
        super(S3ResponseError, self).__init__(msg)
        self.error_code = error_code
        self.description = description
        self.status_code = response.status_code
        self.raw_response = response


class ActionConflict(Exception):
    def __init__(self, key, action1, action2):
        msg = "Conflicting actions set on key '{}':\n".format(key)
        for action in (action1, action2):
            if action['type'] in ('sync', 'delete'):
                msg += action['type']
            elif action['type'] == 'redirect':
                msg += "{} -> {}".format(action['type'], action['url'])
            elif action['type'] == 'upload':
                msg += "{} <- {}".format(action['type'], action['path'])
            msg += " + "
        msg = msg[:-3]
        super(ActionConflict, self).__init__(msg)


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
