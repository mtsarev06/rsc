class NotPerformedException(Exception):
    """
    Raised when an operation wasn't successful and didn't finish properly.
    """
    def __str__(self):
        return self.args[0]


class ConnectionFailure(NotPerformedException, ConnectionError):
    """
    Raised when there was an error connecting to a remote machine.
    """
    pass


class FileNotFound(NotPerformedException, FileNotFoundError):
    """
    Raised when there is no such file or a directory.
    """
    pass


class FileExists(NotPerformedException, FileExistsError):
    """
    Raised when there is already a file with such name.
    """
    pass


