import os
import shutil

import pandas as pd

from recsys.dataloaders.helper import reduce_memory_usage


def get_or_create_dir(data_dir):
    os.makedirs(data_dir, exist_ok=True)
    return data_dir


class PathHandler:
    def __init__(self, data_dir):
        """
        :param data_dir:
        """
        self.data_dir = get_or_create_dir(data_dir)

    def get_path(self, *directories, **partitions):
        """
        :param directories:
        :param partitions:
        :return:
        """
        path = os.path.join(self.data_dir, *directories)
        for partition_name, partition_value in partitions.items():
            path = os.path.join(path, f'{partition_name}={partition_value}')
        return path

    def make_path(self, *directories, **partitions):
        """
        :param directories:
        :param partitions:
        :return:
        """
        path = self.get_path(*directories, **partitions)
        os.makedirs(path, exist_ok=True)
        return path

    def remove_path(self, *directories, **partitions):
        """
        :param directories:
        :param partitions:
        :return:
        """
        path = self.get_path(*directories, **partitions)
        shutil.rmtree(path, ignore_errors=True)


class ParquetLoader(PathHandler):
    def __init__(self, data_dir):
        """
        :param data_dir:
        """
        super().__init__(data_dir=data_dir)

    def load_data_frame(self, *directories, columns=None, reduce_memory=False, **partitions):
        """
        :param reduce_memory:
        :param directories:
        :param columns:
        :param partitions:
        :return:
        """
        _path = self.get_path(*directories, **partitions)
        print(f"loading data form {_path}")
        try:
            if not reduce_memory:
                return pd.read_parquet(_path, columns=columns)
            return reduce_memory_usage(pd.read_parquet(_path, columns=columns))

        except FileNotFoundError:
            raise FileNotFoundError(f"No data was found in "
                                    f"the specified location : {_path}")

    def list_partitions(self, *directories, **partitions):
        """
        :param directories:
        :param partitions:
        :return:
        """
        _path = self.get_path(*directories, **partitions)
        if not os.path.isdir(_path):
            raise ValueError(f'the following path is not a directory : {_path}')
        partitions = list()
        for c in os.listdir(_path):
            try:
                part_id = c.split("=")[1]
                partitions.append((part_id,))
            except KeyError:
                pass
        return partitions


class CsvLoader(PathHandler):
    def __init__(self, data_dir):
        """
        :param data_dir:
        """
        super().__init__(data_dir=data_dir)

    def load_data_frame(self, *directories, **kwargs):
        """
        :param directories:
        :param kwargs:
        :return:
        """
        if not directories:
            raise ValueError("No file specified to load")
        file_name = f'{directories[-1]}.csv'
        directories = list(directories[:-1]) + [file_name]
        _path = self.get_path(*directories)
        try:
            return pd.read_csv(_path, **kwargs)
        except FileNotFoundError:
            raise FileNotFoundError(f"No data was found in the specified "
                                    f"location : {_path}")
