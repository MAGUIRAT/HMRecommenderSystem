import argparse
import multiprocessing as mp

from recsys.dataloaders.file import ParquetLoader
from recsys.engine.features.articles.aggregated import compute
from recsys.engine.validation.utils import split_on_date
from scripts.settings import (
    DATA_ROOT_DIR,
    CUSTOMERS_CLEAN_PATH,
    TRANSACTIONS_REPARTITIONED_BY_ARTICLE_CLEAN_PATH,
    ARTICLES_AGGREGATED_FEATURES_TRAIN_PATH,
    ARTICLES_AGGREGATED_FEATURES_INFERENCE_PATH
)

parquet_loader = ParquetLoader(DATA_ROOT_DIR)

customers = parquet_loader.load_data_frame(*CUSTOMERS_CLEAN_PATH)


def compute_and_save_features(article_partition_id):
    print(f'Started computing articles aggregated transactions '
          f'features for partition {article_partition_id}')

    transactions = parquet_loader. \
        load_data_frame(*TRANSACTIONS_REPARTITIONED_BY_ARTICLE_CLEAN_PATH,
                        article_partition_id=article_partition_id)

    transactions_train, _ = split_on_date(transactions)
    features_train = compute(transactions=transactions_train, customers=customers)
    features_inference = compute(transactions=transactions, customers=customers)

    features_train = features_train.assign(article_partition_id=article_partition_id)
    features_inference = features_inference.assign(article_partition_id=article_partition_id)

    train_path = parquet_loader.make_path(*ARTICLES_AGGREGATED_FEATURES_TRAIN_PATH)
    inference_path = parquet_loader.make_path(*ARTICLES_AGGREGATED_FEATURES_INFERENCE_PATH)

    features_inference.to_parquet(path=inference_path, partition_cols=["article_partition_id"])
    features_train.to_parquet(path=train_path, partition_cols=["article_partition_id"])

    print(f'Ended computing articles aggregated transactions features '
          f'for partition {article_partition_id}')


if __name__ == "__main__":
    parquet_loader.remove_path(*ARTICLES_AGGREGATED_FEATURES_TRAIN_PATH)
    parquet_loader.remove_path(*ARTICLES_AGGREGATED_FEATURES_INFERENCE_PATH)

    arg_parser = argparse.ArgumentParser()

    arg_parser.add_argument('--num_cpu',
                            default=10,
                            type=int,
                            help='number of CPUs to use in execution')

    args = arg_parser.parse_args()

    customers_partitions = parquet_loader. \
        list_partitions(*TRANSACTIONS_REPARTITIONED_BY_ARTICLE_CLEAN_PATH)

    pool = mp.Pool(args.num_cpu)
    pool.starmap(compute_and_save_features, customers_partitions)
    pool.close()
    pool.join()
