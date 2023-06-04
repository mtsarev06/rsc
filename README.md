Library for managing files on remote storages
==========

Provides useful clients for accessing and operating files on a remote storage. 
By using this library you can send and retrieve files from remote storage,
get information about files (like size, type etc.) and other things associated
with it.

Connections
----------
Objects used for working with files on a remote storage. They are responsible
for getting and sending files, retrieving attributes, existence checking 
and etc. All Connections are inherited from an abstract class called 
**RemoteStorageConnection**, so that you can be sure that all connections
use the same approach for operating files, thus might as well be
interchangeable.

At this point implemented the following connections:

1. **SMBConnection** - uses SMB protocol for operating files. Can be used
   while working with shared folders.
2. **SSHConnection** - uses SSH protocol for operating files. Can be used
   while working with remote machines.
3. **VMWCConnection** - uses Advanced VMWC library for operating files. Can be
   used while working with vCenter API of VMWare.
   
There are the following main methods implemented in Connection class:
1. **get_file(remote_file_path, local_path_to_save)** - 
   download file from remote storage. 
2. **get_file_to_object(remote_file_path, file_object)** - 
   download file from remote storage to an IO object
3. **send_file(local_file_path, remote_path_to_save)** - 
   upload file to remote storage.
4. **send_file_object(file_object, remote_path_to_save)** - 
   upload contents of IO object to a file on remote storage.
5. **file_exists(remote_file_path)** - 
   check if file with such path exists on remote storage. 
6. **delete_file(remote_file_path)** - remove a file from remote storage.
7. **create_directory(new_directory_path, create_parents, exist_ok)** - 
   create a directory with specified path on remote storage
8. **delete_directory(remote_directory_path, recursive)** - 
   remove a directory with specified path on remote storage.
9. **get_directory(remote_directory_path, local_path_to_save)** -
   download directory from remote storage to local.
10. **send_directory(local_directory_path, remote_path_to_save, 
   create_parents)** - upload a directory to remote storage.
11. **create_file(remote_file_path, file_contents, create_parents)** - 
   create a file with specific contents on remote storage.
12. **get_file_attributes(remote_file_path)** - 
   retrieve attributes of a remote file (like size, creation date, 
   modification date etc.) in *FileAttributes* representation.
13. **list_path(remote_directory_path)** - 
   retrieve list of files in a path on remote storage.

Simple usage example
----------

```python
from rsc.connections import SMBConnection, VMWCConnection, SSHConnection

# Let's connect to remote storage by SSH and get file list:
connection = SSHConnection("192.168.62.42", "admin", "password", 
                           work_dir="/home/admin")
file_list = connection.list_path('.') # [FileAttributes['/home/admin/test.txt'], FileAttributes['/home/admin/some_dir'],...]

# Let's download only files
for file in file_list:
   if file.is_file():
      connection.get_file(file.path, 'E:\\test_folder\\'+file.name)

# Or send it to SMB storage
smb_connection = SMBConnection("192.168.62.45", "admin", "password", 
                               shared_folder="my_shared_folder")

for file in file_list:
   if file.is_file():
      smb_connection.send_file('E:\\test_folder\\'+file.name, 
                               file.name)
```
