import pathlib
from unittest import TestCase
from datetime import datetime

from src.rsс.base import FileAttributes, Path


class TestFileAttrubutes(TestCase):
    # TODO: Test are ugly, had better rewrite them
    def test_initialization(self):
        valid_data = {
            'name': 'something.zip',
            'size': 1,
            'type': 'directory',
            'path': '/test/directory',
            'absolute_path': '/home/debian/test/directory',
            'modification_time': datetime.fromtimestamp(123),
            'last_access_time': datetime.fromtimestamp(124),
            'create_time': None
        }
        try:
            valid_test_object = FileAttributes(**valid_data)
        except Exception as error:
            self.fail(f"There was an error initializing "
                      f"a valid object: {error}")

        invalid_data = {
            'name': None,
            'size': '123s',
            'type': 'something',
            'path': None,
            'modification_time': None,
            'last_access_time': '57',
            'create_time': '123dsaz'
        }

        for field in invalid_data:
            self.assertRaises(Exception, FileAttributes,
                              **{field: invalid_data[field]})

    def test_to_dict(self):
        valid_data = {
            'name': 'something',
            'size': 1,
            'type': 'directory',
            'path': '/test/directory',
            'absolute_path': '/home/debian/test/directory',
            'modification_time': datetime.fromtimestamp(123),
            'last_access_time': datetime.fromtimestamp(124),
            'create_time': None
        }
        test_object = FileAttributes(**valid_data)
        self.assertEqual(valid_data, test_object.to_dict())
        self.assertEqual(valid_data, dict(test_object))


class TestPath(TestCase):
    def test_init(self):
        test_args = (pathlib.PureWindowsPath('E:\\folder\\file.txt'),
                     pathlib.PurePosixPath('/var/mobile/something'),
                     '/tmp/something')
        test_objects = []
        try:
            for test_arg in test_args:
                test_objects.append(Path(test_arg))
        except Exception as error:
            self.fail(f'There was an error initializing Path object: {error}')

        self.assertEqual(test_args[0].as_posix(), test_objects[0].as_posix())
        self.assertEqual(str(test_args[0]), test_objects[0].as_win())
        self.assertEqual(str(test_args[0]), str(test_objects[0]))

        self.assertEqual(test_args[1].as_posix(), test_objects[1].as_posix())
        self.assertEqual(str(pathlib.PureWindowsPath(test_args[1])),
                         test_objects[1].as_win())
        
        self.assertEqual(pathlib.PureWindowsPath(test_args[2]).as_posix(),
                         test_objects[2].as_posix())
        self.assertEqual(str(pathlib.PureWindowsPath(test_args[2])),
                         test_objects[2].as_win())

        test_args = (pathlib.PureWindowsPath('E:\\test'),
                     pathlib.PurePosixPath('inner_folder/'), 'test.txt')

        try:
            test_object = Path(*test_args)
        except Exception as error:
            self.fail(f"There was an error initializing Path object: {error}")

        self.assertEqual(test_object.as_posix(),
                         'E:/test/inner_folder/test.txt')

        test_object = Path('', '/home/debian/test')
        self.assertEqual(test_object.as_posix(), '/home/debian/test')



