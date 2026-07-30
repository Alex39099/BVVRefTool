class GoogleAPIException(Exception):
    pass

class InvalidArgumentValue(GoogleAPIException):
    """ Invalid value for argument """

class IncorrectCellLabel(GoogleAPIException):
    """ cell label is incorrect """