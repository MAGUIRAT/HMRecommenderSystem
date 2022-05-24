import os

from recsys.dataloaders.file import PathHandler


class BaseModelHandler(PathHandler):
    def __init__(self, root_dir: str):
        super(BaseModelHandler, self).__init__(root_dir)

    def get_path(self, *directories, model_file: str = None):
        path = super(BaseModelHandler, self).get_path(*directories)
        if model_file is not None:
            return os.path.join(path, model_file)
        return path

    def remove_path(self, *directories, model_file: str = None):
        if model_file is not None:
            os.remove(path=self.get_path(*directories, model_file))
        else:
            super(BaseModelHandler, self).remove_path(*directories)
