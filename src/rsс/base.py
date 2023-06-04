import pathlib
from os import PathLike
from typing import Union
from datetime import datetime


class Path(PathLike):
    """
    Class for working with paths and path-like objects.
    This were created because default Path lib has pretty low
    compatibility with different OS (code working on Windows
    can be broken on Linux).
    It expands default Path methods, which means that you can also
    use all Path methods.
    Also it's important to mention that it uses first argument as
    root and others as additional paths.
    So joining '/debian' to the '/home/' (Path('/home/', '/debian')
    will result to '/home/debian'.
    """

    __slots__ = ['as_posix', 'drive', 'root', 'anchor', 'name', 'suffix',
                 'suffixes', 'stem', 'with_name', 'with_suffix',
                 'relative_to', 'parts', 'joinpath', 'is_absolute',
                 'is_reserved', 'match']

    def __init__(self, *args):
        self._path = pathlib.PureWindowsPath(*args)
        for member in self.__slots__:
            setattr(self, member, getattr(self._path, member))

    def mkdir(self, mode=0o777, parents=False, exist_ok=False):
        return pathlib.Path(self._path.as_posix()).\
            mkdir(mode, parents, exist_ok)

    @property
    def parent(self):
        return self.__class__(self._path.parent)

    @property
    def parents(self):
        parents = self._path.parents
        result = []
        for parent in parents:
            if str(parent) != '.':
                result.append(self.__class__(parent))
        return result

    def as_win(self):
        return str(self._path)

    def __str__(self):
        return self.as_win()

    def __repr__(self):
        return f'Path({self._path.as_posix()})'

    def __fspath__(self):
        return self.as_posix()


class FileAttributes:
    def __init__(self, name: str, size: int, type: str, path: Union[str, Path],
                 absolute_path: Union[str, Path],
                 modification_time: Union[str, datetime],
                 last_access_time: Union[str, datetime],
                 create_time: Union[str, datetime] = None, **kwargs):
        """
        Parameters
        ----------
        name: str
            Name of the file or a dictionary without path.
        size: int
            Size of the file in bytes
        type: str
            'directory' or 'file'
        path: [str, Path]
            Relative path to the file
        absolute_path: [str, Path]
            Absolute path to the file
        modification_time: [str, datetime]
            Time of last modification of the file
        last_access_time: [str, datetime]
            Time of the last access to the file
        create_time: [str, datetime]
            When the file was created
        """
        self.name = name
        self.size = int(size)
        if type not in ['directory', 'file', 'symlink']:
            raise ValueError(f"Type must be either 'directory', "
                             f"'file', or 'symlink' (got {type}).")
        self.type = type
        self.path = Path(path)
        self.absolute_path = Path(absolute_path)

        if not isinstance(modification_time, datetime):
            if isinstance(modification_time, (float, int)):
                modification_time = datetime.\
                    fromtimestamp(modification_time)
            else:
                modification_time = datetime.\
                    fromisoformat(str(modification_time))
        self.modification_time = modification_time

        if not isinstance(last_access_time, datetime):
            if isinstance(last_access_time, (float, int)):
                last_access_time = datetime.fromtimestamp(last_access_time)
            else:
                last_access_time = datetime.\
                    fromisoformat(str(last_access_time))
        self.last_access_time = last_access_time

        if not isinstance(create_time, datetime) and create_time is not None:
            if isinstance(create_time, (float, int)):
                create_time = datetime.fromtimestamp(create_time)
            else:
                create_time = datetime.fromisoformat(str(create_time))
        self.create_time = create_time

    def to_dict(self):
        """
        Turns the FileAttributes object into a dictionary.

        Returns
        -------
        dict
            FileAttributes dictionary representation.
        """
        result = dict(vars(self))
        result['path'] = result['path'].as_posix()
        result['absolute_path'] = result['absolute_path'].as_posix()
        return result

    def is_directory(self):
        return self.type == 'directory'

    def is_file(self):
        return self.type == 'file'

    def is_symlink(self):
        return self.type == 'symlink'

    def __getitem__(self, item):
        return getattr(self, item)

    def __iter__(self):
        dict = self.to_dict()
        for key, value in dict.items():
            yield key, value

    def __repr__(self):
        return f"rsc.base.FileAttributes['{self.absolute_path}']"

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False
        return self.to_dict() == other.to_dict()

