class ValidationError(Exception):
    pass


class APIError(Exception):
    pass


class CredentialError(Exception):
    pass


class DBDataAccessError(Exception):
    pass


class DataRangeError(Exception):
    pass


class DataBufferExceeded(Exception):
    pass
