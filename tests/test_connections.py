import os
import io
import shutil
import unittest
from time import sleep
from pathlib import Path

from src.rsс.connections import (SMBConnection, VMWCConnection,
                                 SSHConnection, LocalStorage)


class TestConnections(unittest.TestCase):
    # TODO: Tests are ugly, had better rewrite them

    test_file = Path('E:\\test.txt')

    @classmethod
    def setUp(cls) -> None:
        with open(cls.test_file, 'w') as file:
            file.write('123')

    @classmethod
    def tearDown(cls) -> None:
        os.remove(cls.test_file)

    def test_smb_connection(self):
        try:
            connection = SMBConnection("192.168.13.55",
                                       "user",
                                       "userpassword",
                                       "sharedfolder")
        except Exception:
            self.fail('There was an error connecting '
                      'using SMBConnection class.')
        self.connection_testing(connection)

    def test_ssh_connection(self):
        try:
            connection = SSHConnection("192.168.13.111",
                                       "debian", "0",
                                       "/home/debian")
        except Exception:
            self.fail('There was an error connecting using '
                      'SSHConnection class.')
        self.connection_testing(connection)

    def test_vmwc_connection(self):
        """
        Don't run it unless you have a spare bulk of time.
        """
        parameters = ("vcenter",
                      "vcenter_user", "user_password",
                      "test-1", "debian", "0",
                      "/home/debian/test_vmwc_connection")
        try:
            connection = VMWCConnection(*parameters)
        except Exception as error:
            self.fail(f'There was an error connecting using '
                      f'VMWCConnection class: {error}')
        self.connection_testing(connection)

    def connection_testing(self, connection):
        if connection.file_exists(self.test_file.name):
            connection.delete_file(self.test_file.name)
        connection.send_file(self.test_file, self.test_file.name,
                             create_parents=True)
        self.assertTrue(connection.file_exists(self.test_file.name))

        # Check create_parents and getting file alongside each other
        new_file_path = Path(self.test_file.parent, 'test/some_test',
                             self.test_file.name+'2')
        connection.get_file(self.test_file.name, new_file_path,
                            create_parents=True)
        self.assertTrue(os.path.exists(new_file_path))

        with open(new_file_path) as file:
            with open(self.test_file) as file2:
                self.assertEqual(file.read(), file2.read())

        sleep(1)
        os.remove(new_file_path)
        os.rmdir(new_file_path.parent)

        test_bytes = io.BytesIO()

        connection.get_file(self.test_file.name, test_bytes)

        with open(self.test_file, "rb") as file:
            self.assertEqual(file.read(), test_bytes.getvalue())

        attributes = connection.get_file_attributes(self.test_file.name)
        expected_attributes = ['name', 'size', 'type', 'path',
                               'absolute_path', 'modification_time',
                               'last_access_time', 'create_time']
        self.assertEqual(list(dict(attributes).keys()), expected_attributes)

        if connection.file_exists('test_dir_123'):
            connection.delete_directory('test_dir_123')
        connection.create_directory('test_dir_123')
        self.assertTrue(connection.file_exists('test_dir_123'))
        connection.delete_file(self.test_file.name)
        self.assertFalse(connection.file_exists(self.test_file.name))
        connection.delete_directory('test_dir_123')
        self.assertFalse(connection.file_exists('test_dir_123'))

        # Test list_path
        if connection.file_exists('test_dir_125'):
            connection.delete_directory('test_dir_125', True)
        connection.create_directory('test_dir_125')
        connection.create_directory('test_dir_125/test_dir')
        connection.send_file(self.test_file,
                             'test_dir_125/'+self.test_file.name)
        new_dir_attributes = connection.\
            get_file_attributes('test_dir_125/test_dir')
        new_file_attributes = connection.\
            get_file_attributes('test_dir_125/'+self.test_file.name)
        path_list = connection.list_path('test_dir_125')
        self.maxDiff = None

        self.assertTrue(new_dir_attributes in path_list)
        self.assertTrue(new_file_attributes in path_list)

        # Test is_directory and is_file
        order = (path_list[0], path_list[1]) \
            if path_list[0].name == self.test_file.name \
            else (path_list[1], path_list[0])
        self.assertTrue((order[0].is_file(),
                         order[0].is_directory()) == (True, False))
        self.assertTrue((order[1].is_file(),
                         order[1].is_directory()) == (False, True))

        connection.delete_directory('test_dir_125', True)
        if connection.file_exists('test_dir_125'):
            self.fail("Couldn't delete test directory test_dir_125.")

        # Test send_directory
        local_dir = Path(self.test_file.parent, 'send_dir_test')
        if os.path.exists(local_dir):
            shutil.rmtree(local_dir)
        os.mkdir(local_dir)
        os.mkdir(Path(local_dir, 'inner_dir'))
        with open(Path(local_dir, 'test_file.txt'), "w") as file:
            file.write('123')
        with open(Path(local_dir, 'inner_dir', 'test_file.txt'), "w") as file:
            file.write('123')

        if connection.file_exists('test_send_directory'):
            connection.delete_directory('test_send_directory', True)
        connection.send_directory(local_dir, "test_send_directory")
        self.assertTrue(connection.file_exists('test_send_directory'))
        self.assertTrue(connection.
                        file_exists('test_send_directory/test_file.txt'))
        self.assertTrue(connection.
                        file_exists('test_send_directory/inner_dir'))
        self.assertTrue(connection.
                        file_exists('test_send_directory/inner_dir/'
                                    'test_file.txt'))

        # check get_directory

        local_dir_2 = local_dir.as_posix()+'_2'
        if os.path.exists(local_dir_2):
            shutil.rmtree(local_dir_2)
        os.mkdir(local_dir_2)
        connection.get_directory('test_send_directory', local_dir_2)
        self.assertTrue(os.path.exists(local_dir_2+'/inner_dir'))
        self.assertTrue(os.path.exists(local_dir_2+'/inner_dir/test_file.txt'))
        self.assertEqual(os.stat(local_dir.as_posix()+'/inner_dir/'
                                                      'test_file.txt').st_size,
                         os.stat(local_dir_2+'/inner_dir/'
                                             'test_file.txt').st_size)

        connection.delete_directory('test_send_directory', True)
        self.assertFalse(connection.file_exists('test_send_directory'))
        shutil.rmtree(local_dir)
        shutil.rmtree(local_dir_2)

        # Test creating files

        test_data = {
            "123": 3,
            b'1234': 4,
            io.StringIO('123'): 3,
            io.BytesIO(b'123'): 3,
            '': 0,
            None: 0
        }

        if connection.file_exists("test_creating_files"):
            connection.delete_directory("test_creating_files", recursive=True)
        connection.create_directory("test_creating_files")
        for data in test_data:
            connection.create_file("test_creating_files/test_file.txt", data)
            self.assertTrue(connection.
                            file_exists("test_creating_files/test_file.txt"))
            self.assertEqual(connection.
                             get_file_attributes("test_creating_files/"
                                                 "test_file.txt").size,
                             test_data[data])
            connection.delete_file("test_creating_files/test_file.txt")
        connection.delete_directory("test_creating_files")

    def test_local_storage_connection(self):
        try:
            connection = LocalStorage()
        except Exception as error:
            self.fail(f'There was an error initializing '
                      f'LocalStorage instance: {error}')
        self.connection_testing(connection)
