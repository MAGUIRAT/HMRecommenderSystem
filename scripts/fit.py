import os

import xgboost as xgb

from recsys.dataloaders.file import ParquetLoader
from recsys.engine.candidates.core.config import CANDIDATES_DF_DTYPES
from recsys.engine.candidates.core.helper import (
    filter_negative_customers,
    score_candidates
)
from recsys.engine.models.xgb.core import (
    get_dmatrix,
    get_data_from_candidates,
    split_train_test_candidates
)
from recsys.engine.validation.metrics import compute_map_k
from recsys.engine.validation.utils import split_on_date
from scripts.settings import (
    DATA_ROOT_DIR,
    CANDIDATES_TRAIN_PATH,
    TRANSACTIONS_CLEAN_PATH,
    MODELS_PATH,
    MODEL_FILE_NAME
)

parquet_loader = ParquetLoader(DATA_ROOT_DIR)

BASE_BOOSTER_PARAMS = dict(
    tree_method='hist',
    booster='gbtree',
    objective='rank:map',
    eval_metric="map@12-",
    num_parallel_tree=10,
    subsample=0.85,
    colsample_bytree=0.85,
    colsample_bylevel=1,
    colsample_bynode=1,
    learning_rate=0.07,
    min_split_loss=0,
    max_depth=5,
    grow_policy="depthwise",
)

BASE_LEARNING_PARAMS = dict(
    num_boost_round=150,
    early_stopping_rounds=20
)

TEST_SIZE = 0.2

FILTER_NEGATIVE_CUSTOMERS = True

FEATURES = list(CANDIDATES_DF_DTYPES.keys())

REDUCE_MEMORY = True

if __name__ == "__main__":
    candidates = parquet_loader. \
        load_data_frame(*CANDIDATES_TRAIN_PATH,
                        columns=FEATURES,
                        reduce_memory=REDUCE_MEMORY,
                        ). \
        drop("customer_partition_id", axis=1, errors='ignore')

    if FILTER_NEGATIVE_CUSTOMERS:
        candidates = filter_negative_customers(candidates)

    transactions = parquet_loader. \
        load_data_frame(*TRANSACTIONS_CLEAN_PATH,
                        reduce_memory=REDUCE_MEMORY)

    transactions_before, transactions_after = split_on_date(transactions)

    del transactions

    if FILTER_NEGATIVE_CUSTOMERS:
        candidates = filter_negative_customers(candidates=candidates)

    candidates_train, candidates_test = split_train_test_candidates(candidates, test_size=TEST_SIZE)

    del candidates

    x_train, y_train, groups_train = get_data_from_candidates(candidates_train)
    x_test, y_test, groups_test = get_data_from_candidates(candidates_test)

    dmatrix_train = get_dmatrix(x_train, y_train, groups_train)
    dmatrix_test = get_dmatrix(x_test, y_test, groups_test)

    del x_train
    del y_train
    del groups_train
    del x_test
    del y_test
    del groups_test

    model = xgb.train(params=BASE_BOOSTER_PARAMS,
                      dtrain=dmatrix_train,
                      evals=[(dmatrix_train, "train"), (dmatrix_test, "test")],
                      verbose_eval=1,
                      **BASE_LEARNING_PARAMS)

    prediction_train = score_candidates(model=model,
                                        features=dmatrix_train,
                                        candidates=candidates_train)

    del dmatrix_train

    prediction_test = score_candidates(model=model,
                                       features=dmatrix_test,
                                       candidates=candidates_test)

    del dmatrix_test

    actual_train = transactions_after.loc[transactions_after.customer_id.isin(candidates_train.customer_id)].copy()
    actual_test = transactions_after.loc[transactions_after.customer_id.isin(candidates_test.customer_id)].copy()

    del candidates_train
    del candidates_test

    del transactions_before
    del transactions_after

    map_train = compute_map_k(actual=actual_train, predicted=prediction_train, k=12)
    map_test = compute_map_k(actual=actual_test, predicted=prediction_test, k=12)

    del actual_train
    del actual_test
    del prediction_train
    del prediction_test

    print(f"map@12 train: {map_train}")
    print(f"map@12 test: {map_test}")

    candidates = parquet_loader. \
        load_data_frame(*CANDIDATES_TRAIN_PATH,
                        columns=FEATURES,
                        reduce_memory=REDUCE_MEMORY,
                        ). \
        drop("customer_partition_id", axis=1, errors='ignore')

    if FILTER_NEGATIVE_CUSTOMERS:
        candidates = filter_negative_customers(candidates)

    x, y, groups = get_data_from_candidates(candidates)

    del candidates

    dmatrix = get_dmatrix(x, y, groups)

    del x
    del y
    del groups

    model = xgb.train(params=BASE_BOOSTER_PARAMS,
                      dtrain=dmatrix,
                      evals=[(dmatrix, "train"), ],
                      verbose_eval=1,
                      num_boost_round=model.best_iteration + 1,
                      early_stopping_rounds=BASE_LEARNING_PARAMS["early_stopping_rounds"])

    path = parquet_loader.make_path(*MODELS_PATH)
    path = os.path.join(path, MODEL_FILE_NAME)
    model.save_model(path)
