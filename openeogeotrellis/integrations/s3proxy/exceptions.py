class ProxyException(RuntimeError):
    ...


class S3ProxyDisabled(ProxyException):
    ...


class S3ProxyUnsupportedBucketType(ProxyException):
    ...


class DriverCannotIssueTokens(ProxyException):
    ...
