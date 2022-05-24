import os

import pandas as pd
from xgboost import Booster

from recsys.engine.models.interface import BaseModelHandler


class XgboostModelHandler(BaseModelHandler):
    def __init__(self, root_dir: str):
        super(XgboostModelHandler, self).__init__(root_dir)
        self.model = None

    def _check_model(self):
        assert self.model is not None, "No model is loaded"

    def load(self, *directories, model_file: str = "model.model", inplace=True):
        model_file = self.get_path(*directories, model_file)
        print(f'loading model from {model_file}')
        model = Booster(model_file=model_file)
        if not inplace:
            return model
        self.model = model

    def predict(self, x, **kwargs):
        self._check_model()
        return self.model.predict(x, **kwargs)


class StackedXgboostModelHandler(XgboostModelHandler):
    def __init__(self, root_dir: str):
        super(StackedXgboostModelHandler, self).__init__(root_dir)
        self.model = None

    def load(self, *directories, inplace=True):
        models_files = list(map(str, os.listdir(self.get_path(*directories))))
        model = dict()
        for model_file in models_files:
            model[model_file.split('.')[0]] = super(StackedXgboostModelHandler, self). \
                load(*directories, model_file=model_file)
        if not inplace:
            return model
        self.model = model

    def predict(self, x, **kwargs):
        super(StackedXgboostModelHandler, self)._check_model()
        prediction = dict()
        for model_name, model_booster in self.model.items():
            prediction[model_name] = model_booster.predict(x, **kwargs)
        return pd.DataFrame(prediction)
