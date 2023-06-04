import os
import socket
from typing import Union, IO, BinaryIO
from abc import abstractmethod, ABCMeta
from stat import S_ISDIR
from datetime import datetime
from io import IOBase, StringIO, BytesIO

import avmwc
import paramiko
import smb.base
from smb.SMBConnection import SMBConnection as pySMBConnection
from smb.smb_structs import OperationFailure

from .exceptions import (NotPerformedException, FileNotFound,
                         ConnectionFailure, FileExists)
from .base import FileAttributes, Path


class RemoteStorageConnection(metaclass=ABCMeta):
    """
    Abstract class for implementing connections for
    operating files on a remote server.

    Attributes
    ----------
    _connection: Any
        Main object which is used to operate files on a remote storage.
    """

    _connection = None

    @abstractmethod
    def __init__(self, remote_ip: str, username: str, password: str,
                 work_dir: Union[str, Path] = "", **kwargs):
        """
        Every client object has to implement an easy way to connect to
        a remote machine by using the remote machine ip/name, username
        and password. Extra arguments are optional.

        Parameters
        ----------
        remote_ip: str
            IP or name of the remote machine to connect to.
        username: str
            Username which is used to login on the remote machine.
        password: str
            Password for the current user to log in.
        work_dir: Union[str, Path]
            Working directory, i.e. the root place which will be
            added to the absolute path of a file.
        """
        self._work_dir = work_dir

    @abstractmethod
    def __del__(self):
        """
        Destroys the session and disconnects from the remote
        server when the object is deleted.
        """
        raise NotImplementedError()

    @property
    def connection(self):
        return self._connection

    @property
    def work_dir(self):
        return self._work_dir

    @work_dir.setter
    def work_dir(self, work_dir: Union[str, Path]):
        self._work_dir = work_dir

    def send_file_object(self, file_object: BinaryIO,
                         remote_path_to_save: Union[str, Path],
                         create_parents: bool = None, **kwargs):
        """
        Saves contents of a binary IO object to a file on a remote server.

        Parameters
        ----------
        file_object: BinaryIO
            Binary IO variable containing data to put
            in the file on the remote machine.
        remote_path_to_save: Union[str, Path]
            Remote path of a file which will be used to put data into.
        create_parents: bool
            Whether the script should automatically create
            missing path directories in the path or not.

        """
        if create_parents:
            self.create_directory(remote_path_to_save,
                                  create_parents=create_parents,
                                  exist_ok=True)
        remote_path_to_save = Path(self.work_dir, remote_path_to_save)
        return self._send_file_object(file_object=file_object,
                                      remote_path_to_save=remote_path_to_save,
                                      **kwargs)

    @abstractmethod
    def _send_file_object(self, file_object: BinaryIO,
                          remote_path_to_save: Union[str, Path], **kwargs):
        """
        Should implement a way of sending a binary IO object to
        a file on a remote server.

        Parameters
        ----------
        file_object: BinaryIO
            Binary IO variable containing data to put
            in the file on the remote machine.
        remote_path_to_save: Union[str, Path]
            Remote path of a file which will be used to put data into.
        """
        raise NotImplementedError()

    def get_file_to_object(self, remote_file_path: Union[str, Path],
                           file_object: BinaryIO, **kwargs) -> int:
        """
        Gets a file from a remote server and put it's contents in
        binary IO object.

        Parameters
        ----------
        remote_file_path: Union[str, Path]
            Path to the file on the remote machine to retrieve.
        file_object: BinaryIO
            Where to put the remote file on the local machine.

        Returns
        ----------
        int
            Number of bytes put into the file object.
        """
        remote_file_path = Path(self.work_dir, remote_file_path)
        return self._get_file_to_object(remote_file_path=remote_file_path,
                                        file_object=file_object, **kwargs)

    @abstractmethod
    def _get_file_to_object(self, remote_file_path: Union[str, Path],
                            file_object: BinaryIO, **kwargs) -> int:
        """
        Should implement a way of getting a file from a remote server
        and putting it's contents to binary IO object.

        Parameters
        ----------
        remote_file_path: Union[str, Path]
            Path to the file on the remote machine to retrieve.
        file_object: BinaryIO
            Where to put the remote file on the local machine.

        Returns
        ----------
        int
            Number of bytes put into the file object.
        """
        raise NotImplementedError()

    def file_exists(self, remote_file_path: Union[str, Path], **kwargs):
        """
        Checks if a file or a directory with the passed path exists
        on the remote machine.

        Parameters
        ----------
        remote_file_path: Union[str, Path]
            Path to the file on the remote server to check.

        Returns
        ----------
        bool
            Whether the file exists or not
        """
        remote_file_path = Path(self.work_dir, remote_file_path)
        raise self._file_exists(remote_file_path, **kwargs)

    @abstractmethod
    def _file_exists(self, remote_file_path: Union[str, Path], **kwargs):
        """
        Should implement a way of checking if file exists on the remote
        storage.

        Parameters
        ----------
        remote_file_path: Union[str, Path]
            Path to the file on the remote server to check.

        Returns
        ----------
        bool
            Whether the file exists or not
        """
        raise NotImplementedError()

    def delete_file(self, remote_file_path: Union[str, Path], **kwargs):
        """
        Removes a file from remote machine.

        Parameters
        ----------
        remote_file_path: Union[str, Path]
            Path to the file to remove.
        """
        if not self.file_exists(remote_file_path):
            raise FileNotFound(f"File with such path ({remote_file_path}) "
                               f"doesn't exist on the remote storage.")

        remote_file_path = Path(self.work_dir, remote_file_path)
        return self._delete_file(remote_file_path=remote_file_path, **kwargs)

    @abstractmethod
    def _delete_file(self, remote_file_path: Union[str, Path], **kwargs):
        """
        Should implement a way of deleting a file from remote machine.

        Parameters
        ----------
        remote_file_path: Union[str, Path]
            Path to the file to remove.
        """
        raise NotImplementedError()

    def get_file_attributes(self, remote_file_path: Union[str, Path],
                            **kwargs) -> FileAttributes:
        """
        Retrieves remote file attributes
        (such as file size, creation date, etc.)

        Parameters
        ----------
        remote_file_path: Union[str, Path]
            Path to the file to get attributes of.

        Returns
        ----------
        rsc.base.FileAttributes
            Attributes of the file.
        """
        if not self.file_exists(remote_file_path):
            raise FileNotFound(f"File with such path ({remote_file_path}) "
                               f"doesn't exist on the remote storage.")

        remote_file_path = Path(self.work_dir, remote_file_path)
        return self._get_file_attributes(remote_file_path=remote_file_path,
                                         **kwargs)

    @abstractmethod
    def _get_file_attributes(self, remote_file_path: Union[str, Path],
                             **kwargs) -> FileAttributes:
        """
        Should implement a way of retrieving a file information and
        returning it in a FileAttributes representation.

        Parameters
        ----------
        remote_file_path: Union[str, Path]
            Path to the file to get attributes of.

        Returns
        ----------
        rsc.base.FileAttributes
            Attributes of the file.
        """
        raise NotImplementedError()

    def create_directory(self, new_directory_path: Union[str, Path],
                         create_parents: bool = False,
                         exist_ok: bool = False, **kwargs):
        """
        Creates a directory with the passed path.

        Parameters
        ----------
        new_directory_path: Union[str, Path]
            New directory absolute path.
        create_parents: bool
            Whether the script should automatically create missing
            directories on the local machine or not.
        exist_ok: bool
            Whether the script should raise an exception if directory exists.

        Returns
        ----------
        bool
            True if the directory has been created and False otherwise.
        """
        if not exist_ok and self.file_exists(new_directory_path):
            raise FileExists(f"Directory with such path "
                             f"({new_directory_path}) already "
                             f"exists on the remote storage.")

        if create_parents:
            path_parents = list(Path(new_directory_path).parents)[::-1]
            for parent in path_parents:
                self.create_directory(parent, exist_ok=True)

        new_directory_path = Path(self.work_dir, new_directory_path)
        return self._create_directory(new_directory_path=new_directory_path,
                                      **kwargs)

    @abstractmethod
    def _create_directory(self, new_directory_path: Union[str, Path],
                          **kwargs):
        """
        Should implement a way of creating one directory with passed path.

        Parameters
        ----------
        new_directory_path: Union[str, Path]
            New directory absolute path.

        Returns
        ----------
        bool
            True if the directory has been created and False otherwise.
        """
        raise NotImplementedError()

    def delete_directory(self, remote_directory_path: Union[str, Path],
                         recursive: bool = False, **kwargs):
        """
        Removes a directory with passed path.

        Parameters
        ----------
        remote_directory_path: Union[str, Path]
            Remote directory path.
        recursive: bool
            Whether the method should also delete all folders and
            files inside the directory.

        Returns
        ----------
        bool
            True if the directory has been removed successfully
            and False otherwise.
        """
        if not self.file_exists(remote_directory_path):
            raise FileNotFound(f"Directory with such path "
                               f"({remote_directory_path}) doesn't "
                               f"exists on the remote storage.")
        if recursive:
            current_dir = self.get_file_attributes(remote_directory_path)
            path_list = self.list_path(remote_directory_path)
            for file in path_list:
                if current_dir.path == file.path or \
                        file.path.as_posix()[-2:] in ['/.', '..']:
                    continue
                if file.is_directory():
                    self.delete_directory(file.path, True)
                else:
                    self.delete_file(file.path)

        remote_directory_path = Path(self.work_dir, remote_directory_path)
        return self._delete_directory(
            remote_directory_path=remote_directory_path,
            **kwargs
        )

    @abstractmethod
    def _delete_directory(self, remote_directory_path: Union[str, Path],
                          **kwargs):
        """
        Should implement a way of removing one directory with passed path.

        Parameters
        ----------
        remote_directory_path: Union[str, Path]
            Remote directory path.

        Returns
        ----------
        bool
            True if the directory has been removed successfully
            and False otherwise.
        """
        raise NotImplementedError()

    def get_directory(self, remote_directory_path: Union[str, Path],
                      local_path_to_save: Union[str, Path], **kwargs):
        """
        Retrieves a directory from the remote storage to local.
        It's important to note that it transfers files one by one,
        which can take long.

        Parameters
        ----------
        remote_directory_path: Union[str, Path]
            Path to the remote directory to transfer to the local machine.
        local_path_to_save: Union[str, Path]
            Path on the local storage to put the remote directory.

        """
        if not os.path.exists(local_path_to_save):
            raise FileNotFound(f'There is no local directory '
                               f'with path {local_path_to_save}.')
        for remote_file in self.list_path(remote_directory_path):
            local_file_path = Path(local_path_to_save,
                                   remote_file.name).as_posix()
            if not os.path.exists(local_file_path) or \
                    remote_file.is_directory() or \
                    remote_file.size != os.stat(local_file_path).st_size:
                if remote_file.is_symlink():
                    continue
                elif remote_file.is_directory():
                    if not os.path.exists(local_file_path):
                        os.mkdir(local_file_path)
                    self.get_directory(remote_file.path, local_file_path)
                else:
                    self.get_file(remote_file.path, local_file_path)

    def list_path(self, remote_directory_path: Union[str, Path] = '',
                  **kwargs):
        """
        Lists all files and directories in the path on the remote machine.

        Parameters
        ----------
        remote_directory_path: [str, Path]
            Path on the remote machine to list files in.

        Returns
        -------
        list
            List of the files and directories in the path.
        """
        if not self.file_exists(remote_directory_path):
            raise FileNotFound(f"Directory with such path "
                               f"({remote_directory_path}) doesn't exist on "
                               f"the remote storage.")

        remote_directory_path = Path(self.work_dir, remote_directory_path)
        return self._list_path(remote_directory_path=remote_directory_path)

    @abstractmethod
    def _list_path(self, remote_directory_path: Union[str, Path] = '',
                   **kwargs):
        """
        Should implement a way of getting a list of all files and directories
        in the path on the remote machine.

        Parameters
        ----------
        remote_directory_path: [str, Path]
            Path on the remote machine to list files in.

        Returns
        -------
        list
            List of the files and directories in the path.
        """
        raise NotImplementedError()

    def send_file(self, local_file: Union[str, Path, IO],
                  remote_path_to_save: Union[str, Path],
                  create_parents: bool = False, **kwargs):
        """
        Sends a file to a remote server.

        Parameters
        ----------
        local_file: Union[str, Path, IO]
            Path to a local file or an IO object to send and
            store on the remote server.
        remote_path_to_save: Union[str, Path]
            The place to store the file on the remote server.
        create_parents: bool
            Whether the script should automatically create missing path
            directories in the path or not.

        """
        remote_path_to_save = Path(remote_path_to_save)
        if create_parents:
            self.create_directory(remote_path_to_save.parent.as_posix(),
                                  create_parents=create_parents, exist_ok=True)

        file_object = self.__get_binary_io_object(local_file, 'rb')
        file_object.seek(0)

        result = self.send_file_object(file_object,
                                       remote_path_to_save,
                                       **kwargs)

        if isinstance(local_file, (str, os.PathLike)):
            file_object.close()

        return result

    def get_file(self, remote_file_path: Union[str, Path],
                 where_to_put: Union[str, Path, IO],
                 create_parents: bool = False, **kwargs) -> int:
        """
        Gets a file from the remote server.

        Parameters
        ----------
        remote_file_path: Union[str, Path]
            Path to the file on the remote machine to retrieve.
        where_to_put: Union[str, Path, IO]
            Where to put the remote file on the local machine.
        create_parents: bool
            Whether the script should automatically create missing
            directories on the local machine or not.

        Returns
        ----------
        int
            Number of bytes saved put into the destination point.
        """
        if not self.file_exists(remote_file_path):
            raise FileNotFound(f"File with such path ({remote_file_path}) "
                               f"doesn't exist on the remote storage.")

        if create_parents and not isinstance(where_to_put, IOBase):
            Path(where_to_put).parent.mkdir(parents=True, exist_ok=True)

        file_object = self.__get_binary_io_object(where_to_put)

        result = self.get_file_to_object(remote_file_path, file_object,
                                         **kwargs)

        if isinstance(where_to_put, (str, os.PathLike)):
            file_object.close()

        return result

    def send_directory(self, local_directory_path: Union[str, Path],
                       remote_path_to_save: Union[str, Path],
                       create_parents: bool = False):
        """
        Sends a local directory to the remote server. Notice, that files are
        transferred one by one, which can be long.

        Parameters
        ----------
        local_directory_path: Union[str, Path]
            Local directory to send on the remote storage.
        remote_path_to_save: Union[str, Path]
            Remote path to put the directory.
        create_parents: bool
            Whether the script should automatically create missing parents on
            the way to the directory.
        """
        remote_path_to_save = Path(remote_path_to_save)
        if create_parents:
            self.create_directory(remote_path_to_save.parent.as_posix(),
                                  create_parents=create_parents, exist_ok=True)

        self.create_directory(remote_path_to_save)
        for file in os.listdir(local_directory_path):
            file_abspath = Path(local_directory_path, file)
            if os.path.isdir(file_abspath):
                self.send_directory(file_abspath,
                                    Path(remote_path_to_save, file))
            else:
                self.send_file(file_abspath, Path(remote_path_to_save, file))

    def create_file(self, remote_file_path: Union[str, Path],
                    file_contents: Union[str, bytes, bytearray, IO] = None,
                    create_parents: bool = False):
        """
        Creates files on the remote machine with given contents.
        Uses implemented send_file method of the current
        class passing IO object as a file argument.

        Parameters
        ----------
        remote_file_path: Union[str, Path]
            Path on the remote machine to create new file on.
        file_contents: Union[str, bytes, bytearray, IO]
            What to put into the newly created file.
        create_parents: bool
            Whether the method should create parent folders if don't exist.
        """
        remote_file_path = Path(remote_file_path)
        if create_parents and not self.file_exists(remote_file_path.parent) \
                and remote_file_path.parent.as_posix() != ".":
            self.create_directory(remote_file_path.parent,
                                  create_parents=create_parents,
                                  exist_ok=True)

        if isinstance(file_contents, str) or file_contents is None:
            file_contents = StringIO(file_contents)
        elif isinstance(file_contents, (bytes, bytearray)):
            file_contents = BytesIO(file_contents)
        elif not isinstance(file_contents, IOBase):
            raise TypeError('File contents must be an instance '
                            'of str, bytes or IO type.')

        self.send_file(file_contents, remote_file_path, create_parents)

    def convert_to_fileattributes(self, file_object,
                                  remote_file_path: Union[str, Path]):
        """
        Should return rsc.base.FileAttributes object,
        which is derived from the passed file object.

        Parameters
        ----------
        file_object: Any
            File object which is returned by the remote storage
            connection library.
        remote_file_path
            Path to the object on the remote storage.
        Returns
        -------
        rsc.base.FileAttributes
            FileAttributes representation of the file.
        """
        raise NotImplementedError()

    @staticmethod
    def __get_binary_io_object(source: Union[str, Path, StringIO, BytesIO],
                               mode="ab+"):
        if isinstance(source, (str, os.PathLike)):
            file_object = open(source, mode)
        elif isinstance(source, StringIO):
            file_object = BytesIO(source.getvalue().encode('utf-8'))
        elif isinstance(source, BytesIO):
            file_object = source
        else:
            raise TypeError(f'local_file_path must be an instance of str, '
                            f'Path or IO object (got {type(source)} instead).')
        return file_object


class SMBConnection(RemoteStorageConnection):
    """
    Connection which uses "pysmb" library to operate files
    on a remote storage using SMB.
    """

    @property
    def connection(self) -> pySMBConnection:
        return self._connection

    @property
    def shared_folder(self):
        return self._shared_folder

    def __init__(self, remote_ip: str, username: str, password: str,
                 shared_folder: str = "", remote_name: str = None,
                 is_direct_tcp: str = None, my_name: str = None,
                 work_dir: Union[str, Path] = "", **kwargs):
        connection = pySMBConnection(username=username, password=password,
                                     my_name=my_name or username,
                                     remote_name=remote_name or remote_ip,
                                     is_direct_tcp=is_direct_tcp or True)
        try:
            if not connection.connect(remote_ip, 445):
                raise ConnectionFailure("Couldn't connect to the SMB storage "
                                        "with such credentials.")
        except socket.gaierror:
            raise ConnectionFailure(f"There was an error connecting to the "
                                    f"SMB storage ({remote_ip}): host is not "
                                    f"accessable.")
        self._connection = connection
        self._shared_folder = shared_folder
        self._work_dir = work_dir

    def __del__(self):
        if hasattr(self, '_connection') and self._connection:
            self._connection.close()

    def _send_file_object(self, file_object: BinaryIO,
                          remote_path_to_save: Path,
                          **kwargs):
        try:
            self._connection.storeFile(
                self.shared_folder, remote_path_to_save.as_posix(),
                file_object, **kwargs
            )
        except OperationFailure as exception:
            raise NotPerformedException(f"Couldn't save the file on the "
                                        f"remote machine: "
                                        f"{str(exception.args)}")

    def _get_file_to_object(self, remote_file_path: Path,
                            file_object: BinaryIO, **kwargs) -> int:
        try:
            return self._connection.retrieveFile(
                self.shared_folder, remote_file_path.as_posix(),
                file_object, **kwargs)[1]
        except OperationFailure as exception:
            raise NotPerformedException(f"Couldn't retrieve the file from "
                                        f"the remote machine: "
                                        f"{str(exception.args)}")

    def _file_exists(self, remote_file_path: Path, **kwargs):
        if remote_file_path.as_posix() == ".":
            return True
        try:
            self._connection.getAttributes(self.shared_folder,
                                           remote_file_path.as_posix(),
                                           **kwargs)
        except OperationFailure:
            return False
        return True

    def _delete_file(self, remote_file_path: Path, **kwargs):
        try:
            self._connection.deleteFiles(self.shared_folder,
                                         remote_file_path.as_posix(), **kwargs)
        except OperationFailure as exception:
            raise NotPerformedException(f"Couldn't delete the file: "
                                        f"{str(exception.args)}")
        return True

    def _get_file_attributes(self, remote_file_path: Path,
                             **kwargs):
        try:
            attributes = self._connection.getAttributes(
                self.shared_folder, remote_file_path.as_posix(), **kwargs)
        except OperationFailure as error:
            raise NotPerformedException(f"Couldn't get file attributes of a "
                                        f"file with such path: "
                                        f"{remote_file_path}. {error}")
        return self.convert_to_fileattributes(
            attributes, remote_file_path=remote_file_path)

    def _create_directory(self, new_directory_path: Path,
                          **kwargs):
        try:
            self._connection.createDirectory(self.shared_folder,
                                             new_directory_path.as_posix(),
                                             **kwargs)
        except OperationFailure as error:
            raise NotPerformedException(f"Couldn't create a directory: "
                                        f"{str(error)}")
        return True

    def _delete_directory(self, remote_directory_path: Path,
                          **kwargs):
        try:
            self._connection.deleteDirectory(self.shared_folder,
                                             remote_directory_path.as_posix(),
                                             **kwargs)
        except OperationFailure as error:
            raise NotPerformedException(f"Couldn't delete the "
                                        f"directory: {str(error.args)}")
        return True

    def _list_path(self, remote_directory_path: Path = '',
                   **kwargs):
        try:
            path_list = self._connection.listPath(
                self.shared_folder,
                remote_directory_path.as_posix(),
                **kwargs
            )
        except OperationFailure as error:
            raise NotPerformedException(f"Couldn't list files of a dictionary "
                                        f"with such path: "
                                        f"{remote_directory_path}. {error}")
        result = []
        for file in path_list:
            if file.filename not in ['.', '..']:
                file_path = Path(remote_directory_path, file.filename)
                result.append(self.convert_to_fileattributes(file, file_path))
        return result

    def convert_to_fileattributes(self, file_object: smb.base.SharedFile,
                                  remote_file_path: Union[str, Path]):
        remote_file_path = Path(remote_file_path)
        result = {
            'name': file_object.filename,
            'size': file_object.file_size,
            'type': 'directory' if file_object.isDirectory else 'file',
            'path': str(remote_file_path),
            'absolute_path': str(Path('\\', self.work_dir, remote_file_path)),
            'modification_time':
                datetime.fromtimestamp(file_object.last_write_time),
            'last_access_time':
                datetime.fromtimestamp(file_object.last_access_time),
            'create_time': datetime.fromtimestamp(file_object.create_time),
        }
        return FileAttributes(**result)


class VMWCConnection(RemoteStorageConnection):
    """
    Connection which uses "avmwc" library to operate files on a remote
    storage using VMWareClient.

    There is a speciality working with VMWC: it won't work properly with
    posix paths.
    You need to use Windows paths when setting the remote path,
    that's why str() is used instead of to_posix().
    """

    def __init__(self, remote_ip: str, username: str, password: str,
                 machine_name: str, machine_login: str,
                 machine_password: str, work_dir: str = '/', **kwargs):
        self._vcenter_client = avmwc.VMWareClient(
            remote_ip, username, password).__enter__()
        self._vm = self._vcenter_client.get_virtual_machine(machine_name)
        self._vm.vmware_tools.login(machine_login, machine_password)
        self._work_dir = work_dir or "E:\\"

    def __del__(self):
        if hasattr(self, '_vcenter_client') and self._vcenter_client:
            self._vcenter_client.__exit__(None, None, None)

    @property
    def connection(self) -> avmwc.VirtualMachine:
        return self._vm

    def _send_file_object(self, file_object: BinaryIO,
                          remote_path_to_save: Path,
                          **kwargs):
        remote_path_to_save = self.__prepare_remote_path(remote_path_to_save)
        self._vm.vmware_tools.upload_file(file_object, remote_path_to_save)

    def _get_file_to_object(self, remote_file_path: Path,
                            file_object: BinaryIO, **kwargs) -> int:
        remote_file_path = self.__prepare_remote_path(remote_file_path)
        return self._vm.vmware_tools.download_file(file_object,
                                                   remote_file_path)

    def file_exists(self, remote_file_path: Path, **kwargs):
        remote_file_path = self.__prepare_remote_path(remote_file_path)
        try:
            result = self._vm.vmware_tools.file_exists(remote_file_path)
        except Exception as error:
            raise NotPerformedException(f"Couldn't check if a file with "
                                        f"such path ({remote_file_path}) "
                                        f"exists on the remote storage: "
                                        f"{error}")
        return result

    def _delete_file(self, remote_file_path: Path, **kwargs):
        remote_file_path = self.__prepare_remote_path(remote_file_path)
        try:
            result = self._vm.vmware_tools.delete_file(remote_file_path)
        except Exception as error:
            raise NotPerformedException(f"Couldn't delete a file with such "
                                        f"path ({remote_file_path}) from the "
                                        f"remote storage: {error}")
        return result

    def _get_file_attributes(self, remote_file_path: Path,
                             **kwargs):
        remote_file_path = self.__prepare_remote_path(remote_file_path)
        try:
            file_object = self._vm.vmware_tools.\
                get_file_attributes(remote_file_path)
        except Exception as error:
            raise NotPerformedException(f"Couldn't get attributes of the file "
                                        f"with such path "
                                        f"({remote_file_path}): {error}")
        return self.convert_to_fileattributes(file_object, remote_file_path)

    def _create_directory(self, new_directory_path: Path,
                          **kwargs):
        new_directory_path = self.__prepare_remote_path(new_directory_path)
        try:
            self._vm.vmware_tools.create_directory(new_directory_path)
        except Exception as exception:
            raise NotPerformedException(f"Couldn't create a dictionary with "
                                        f"such path ({new_directory_path}): "
                                        f"{str(exception)}")

    def delete_directory(self, remote_directory_path: Path,
                         recursive: bool = False, **kwargs):
        directory_path = self.__prepare_remote_path(remote_directory_path)
        try:
            self._vm.vmware_tools.delete_directory(directory_path, recursive)
        except Exception as exception:
            raise NotPerformedException(f"Couldn't delete a dictionary: "
                                        f"{str(exception.args)}")
        return True

    def _list_path(self, remote_directory_path: Path = '',
                   **kwargs):
        remote_directory_path = self.\
            __prepare_remote_path(remote_directory_path)
        try:
            file_objects = self._vm.vmware_tools.\
                list_path(remote_directory_path)
        except Exception as error:
            raise NotPerformedException(f"Couldn't get file list of the path "
                                        f"({remote_directory_path}): {error}")
        result = []
        for file_object in file_objects:
            if file_object.path not in ['.', '..']:
                file_path = Path(remote_directory_path, file_object.path)
                result.append(self.convert_to_fileattributes(file_object,
                                                             file_path))
        return result

    def convert_to_fileattributes(self, file_object,
                                  remote_file_path: Union[str, Path]):
        remote_file_path = Path(remote_file_path)
        result = {
            'name': remote_file_path.name,
            'size': file_object.size,
            'type': file_object.type,
            'path': str(remote_file_path),
            'absolute_path': str(Path(self._work_dir, remote_file_path)),
            'modification_time': getattr(file_object.attributes,
                                         'modificationTime').\
                                                        replace(tzinfo=None),
            'last_access_time': getattr(file_object.attributes, 'accessTime').\
                                                        replace(tzinfo=None),
            'create_time': getattr(file_object.attributes, 'createTime', None),
        }
        if result['create_time']:
            result['create_time'] = result['create_time'].replace(tzinfo=None)
        return FileAttributes(**result)

    def __prepare_remote_path(self, remote_path: Union[str, Path]):
        os_type = self.connection.summary.guest.guestId
        return remote_path.as_win() if os_type.find('win') != -1 \
            else remote_path.as_posix()


class SSHConnection(RemoteStorageConnection):
    """
    Connection which uses "paramiko" library to operate files
    on a remote storage using SSH.
    """

    @property
    def connection(self) -> paramiko.SFTPClient:
        return self._connection

    def __init__(self, remote_ip: str, username: str, password: str,
                 work_dir="", remote_port: int = 22, **kwargs):
        try:
            ssh = paramiko.Transport((remote_ip, int(remote_port)), **kwargs)
            ssh.connect(hostkey=None, username=username, password=password)
        except paramiko.SSHException as e:
            raise ConnectionFailure(f"Couldn't connect to the "
                                    f"virtual machine: {e}")

        self._connection = paramiko.SFTPClient.from_transport(ssh)
        self._work_dir = work_dir

    def __del__(self):
        if hasattr(self, '_connection') and self._connection:
            self._connection.get_channel().get_transport().close()

    def _send_file_object(self, file_object: BinaryIO,
                          remote_path_to_save: Path,
                          **kwargs):
        try:
            self._connection.putfo(file_object,
                                   remote_path_to_save.as_posix(), **kwargs)
        except IOError as error:
            raise NotPerformedException(
                f"Couldn't send file to a dictionary with such "
                f"path: {remote_path_to_save}. {error}")

    def _get_file_to_object(self, remote_file_path: Path,
                            file_object: BinaryIO, **kwargs):
        try:
            return self._connection.getfo(remote_file_path.as_posix(),
                                          file_object, **kwargs)
        except IOError as error:
            raise NotPerformedException(f"Couldn't get file with such path: "
                                        f"{remote_file_path}. {error}")

    def _file_exists(self, remote_file_path: Path, **kwargs):
        try:
            self._connection.stat(remote_file_path.as_posix())
        except IOError:
            return False
        return True

    def _delete_file(self, remote_file_path: Path, **kwargs):
        try:
            self._connection.remove(remote_file_path.as_posix())
        except IOError as error:
            raise NotPerformedException(f"Couldn't delete a file with such "
                                        f"path: {remote_file_path}. {error}")

    def _get_file_attributes(self, remote_file_path: Path,
                             **kwargs):
        try:
            stats = self._connection.stat(remote_file_path.as_posix())
        except IOError as error:
            raise NotPerformedException(f"Couldn't retrieve attributes of a "
                                        f"file with such path: "
                                        f"{remote_file_path}. {error}")
        return self.convert_to_fileattributes(stats, remote_file_path)

    def _create_directory(self, new_directory_path: Path,
                          mode: int = 777):
        posix_modes = {
            666: 438,
            660: 432,
            644: 420,
            600: 384,
            777: 511,
            700: 448,
            70: 56
        }
        if mode not in posix_modes:
            raise NotPerformedException(f"Mode {mode} is not supported "
                                        f"by the lib.")
        try:
            self._connection.mkdir(new_directory_path.as_posix(),
                                   posix_modes[mode])
        except IOError as error:
            raise NotPerformedException(f"Couldn't create a dictionary with "
                                        f"such path: {new_directory_path}. "
                                        f"{error}")

    def _delete_directory(self, remote_directory_path: Path,
                          **kwargs):
        try:
            self._connection.rmdir(remote_directory_path.as_posix())
        except IOError as error:
            raise NotPerformedException(f"Couldn't delete a dictionary with "
                                        f"such path: {remote_directory_path}. "
                                        f"{error}")

    def _list_path(self, remote_directory_path: Path = '', **kwargs):
        try:
            path_list = self._connection.listdir_attr(
                remote_directory_path.as_posix())
        except IOError as error:
            raise NotPerformedException(f"Couldn't list files in the path: "
                                        f"{remote_directory_path}. {error}")
        result = []
        for file_object in path_list:
            result.append(
                self.convert_to_fileattributes(file_object,
                                               Path(remote_directory_path,
                                                    file_object.filename)))
        return result

    def convert_to_fileattributes(self, file_object,
                                  remote_file_path: Union[str, Path]):
        remote_file_path = Path(remote_file_path)
        result = {
            'name': remote_file_path.name,
            'size': file_object.st_size,
            'type': 'directory' if S_ISDIR(file_object.st_mode) else 'file',
            'path': str(remote_file_path),
            'absolute_path': str(Path(self._work_dir, remote_file_path)),
            'modification_time': datetime.fromtimestamp(file_object.st_mtime),
            'last_access_time': datetime.fromtimestamp(file_object.st_atime),
            'create_time': datetime.fromtimestamp(file_object.st_ctime) \
                if hasattr(file_object, 'st_ctime') else None
        }
        return FileAttributes(**result)


class LocalStorage(RemoteStorageConnection):
    """
    Emulates connection to the local storage.
    Essentially it is an adapter for default os methods.
    """
    def __init__(self, remote_ip: str = None, username: str = None,
                 password: str = None, work_dir: Union[str, Path] = "",
                 **kwargs):
        super().__init__(remote_ip, username, password, work_dir)

    def __del__(self):
        pass

    def _send_file_object(self, file_object: BinaryIO,
                          remote_path_to_save: Path, **kwargs):
        with open(remote_path_to_save, "wb") as destination_file:
            destination_file.write(file_object.read())

    def _get_file_to_object(self, remote_file_path: Path,
                            file_object: BinaryIO, **kwargs):
        with open(remote_file_path, 'rb') as file:
            file_object.write(file.read())

    def _file_exists(self, remote_file_path: Path, **kwargs):
        return os.path.exists(remote_file_path)

    def _delete_file(self, remote_file_path: Path, **kwargs):
        os.remove(remote_file_path)

    def _get_file_attributes(self, remote_file_path: Path,
                             **kwargs):
        file_object = os.stat(remote_file_path)
        return self.convert_to_fileattributes(file_object, remote_file_path)

    def _create_directory(self, new_directory_path: Path,
                          **kwargs):
        return os.makedirs(new_directory_path)

    def _delete_directory(self, remote_directory_path: Path,
                          recursive: bool = False, **kwargs):
        return os.rmdir(remote_directory_path)

    def _list_path(self, remote_directory_path: Path = '',
                   **kwargs):
        result = []
        for file in os.listdir(remote_directory_path):
            result.append(
                self.convert_to_fileattributes(
                    os.stat(Path(remote_directory_path, file)),
                    Path(remote_directory_path, file)
                )
            )
        return result

    def convert_to_fileattributes(self, file_object,
                                  remote_file_path: Union[str, Path]):
        remote_file_path = Path(remote_file_path)
        result = {
            'name': remote_file_path.name,
            'size': file_object.st_size,
            'type': 'directory' if S_ISDIR(file_object.st_mode) else 'file',
            'path': str(remote_file_path),
            'absolute_path': str(Path(self._work_dir, remote_file_path)),
            'modification_time': datetime.fromtimestamp(file_object.st_mtime),
            'last_access_time': datetime.fromtimestamp(file_object.st_atime),
            'create_time': datetime.fromtimestamp(file_object.st_ctime) \
                if hasattr(file_object, 'st_ctime') else None
        }
        return FileAttributes(**result)

