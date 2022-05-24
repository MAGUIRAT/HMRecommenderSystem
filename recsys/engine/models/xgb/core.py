import warnings

import xgboost as xgb
from sklearn.model_selection import GroupShuffleSplit

warnings.simplefilter(action='ignore', category=FutureWarning)


def split_train_test_candidates(candidates, test_size=0.2, n_splits=1):
    gss = GroupShuffleSplit(test_size=test_size, n_splits=n_splits). \
        split(candidates, groups=candidates['customer_id'])
    x_train_indices, x_test_indices = next(gss)
    train_candidates = candidates.iloc[x_train_indices]
    test_candidates = candidates.iloc[x_test_indices]
    return train_candidates, test_candidates


def get_data_from_candidates(candidates, inference=False):
    group = candidates.groupby('customer_id').size().to_frame('size')['size'].to_numpy()
    x = candidates.loc[:, ~candidates.columns.isin(['customer_id', 'has_purchased'])]
    if inference:
        y = None
    else:
        y = candidates.loc[:, "has_purchased"]
    return x, y, group


def get_dmatrix(x, y=None, group=None):
    dmatrix = xgb.DMatrix(x, y)
    dmatrix.set_group(group)
    return dmatrix
