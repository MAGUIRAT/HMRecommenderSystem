import argparse
import multiprocessing as mp
import sys

from recsys.dataloaders.file import ParquetLoader
from recsys.engine.candidates.core.helper import (
    score_candidates,
    rank_candidates
)
from recsys.engine.models.xgb.core import (
    get_data_from_candidates,
    get_dmatrix
)
from recsys.engine.models.xgb.interface import XgboostModelHandler
from scripts.fit import FEATURES
from scripts.settings import (
    DATA_ROOT_DIR,
    MODELS_ROOT_DIR,
    MODELS_PATH,
    MODEL_FILE_NAME,
    CANDIDATES_INFERENCE_PATH,
    RECOMMENDATION_INFERENCE_PATH
)

parquet_loader = ParquetLoader(DATA_ROOT_DIR)

model = XgboostModelHandler(MODELS_ROOT_DIR)

model.load(*MODELS_PATH,
           model_file=MODEL_FILE_NAME,
           inplace=True)


def predict(customer_partition_id):
    sys.stdout.write(f"computed predictions for partition {customer_partition_id} \n")

    candidates = parquet_loader. \
        load_data_frame(*CANDIDATES_INFERENCE_PATH,
                        customer_partition_id=customer_partition_id)

    features = [f for f in FEATURES if f != "has_purchased"]

    candidates = candidates.loc[:, features]

    x, _, groups = get_data_from_candidates(candidates, inference=True)

    dmatrix = get_dmatrix(x=x, group=groups)

    candidates = score_candidates(model=model,
                                  features=dmatrix,
                                  candidates=candidates)

    candidates = rank_candidates(candidates)

    candidates = candidates.groupby("customer_id").head(12)

    path = parquet_loader.make_path(*RECOMMENDATION_INFERENCE_PATH)

    candidates = candidates.assign(customer_partition_id=customer_partition_id)

    candidates.to_parquet(path=path, partition_cols=["customer_partition_id"])

    sys.stdout.flush()


if __name__ == "__main__":
    parquet_loader.remove_path(*RECOMMENDATION_INFERENCE_PATH)

    arg_parser = argparse.ArgumentParser()

    arg_parser.add_argument('--num_cpu',
                            default=1,
                            type=int,
                            help='number of CPUs to use in execution')

    args = arg_parser.parse_args()

    customers_partitions = parquet_loader.list_partitions(*CANDIDATES_INFERENCE_PATH)

    pool = mp.Pool(args.num_cpu)
    pool.starmap(predict, customers_partitions)
    pool.close()
    pool.join()
