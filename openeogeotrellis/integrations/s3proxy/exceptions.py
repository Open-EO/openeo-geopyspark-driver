class ProxyException(RuntimeError):
    pass


class S3ProxyDisabled(ProxyException):
    pass


class S3ProxyUnsupportedBucketType(ProxyException):
    pass


class DriverCannotIssueTokens(ProxyException):
    pass


class CredentialsException(ProxyException):
    pass
