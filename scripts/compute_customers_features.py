import argparse
import multiprocessing as mp

from recsys.dataloaders.file import ParquetLoader
from recsys.engine.features.customers.aggregated import compute
from recsys.engine.validation.utils import split_on_date
from scripts.settings import (
    DATA_ROOT_DIR,
    CUSTOMERS_CLEAN_PATH,
    TRANSACTIONS_CLEAN_PATH,
    ARTICLES_CLEAN_PATH,
    CUSTOMERS_AGGREGATED_FEATURES_TRAIN_PATH,
    CUSTOMERS_AGGREGATED_FEATURES_INFERENCE_PATH
)

parquet_loader = ParquetLoader(DATA_ROOT_DIR)

articles = parquet_loader.load_data_frame(*ARTICLES_CLEAN_PATH)


def save(customer_partition_id):
    print(f'Started computing customers aggregated transactions features '
          f'for partition {customer_partition_id}')

    transactions = parquet_loader.load_data_frame(*TRANSACTIONS_CLEAN_PATH,
                                                  customer_partition_id=customer_partition_id)

    customers = parquet_loader.load_data_frame(*CUSTOMERS_CLEAN_PATH,
                                               customer_partition_id=customer_partition_id)

    transactions_train, _ = split_on_date(transactions)

    features_train = compute(transactions=transactions_train,
                             customers=customers,
                             articles=articles)

    features_inference = compute(transactions=transactions,
                                 customers=customers,
                                 articles=articles)

    features_train = features_train. \
        assign(customer_partition_id=customer_partition_id)

    features_inference = features_inference. \
        assign(customer_partition_id=customer_partition_id)

    train_path = parquet_loader. \
        make_path(*CUSTOMERS_AGGREGATED_FEATURES_TRAIN_PATH)

    inference_path = parquet_loader. \
        make_path(*CUSTOMERS_AGGREGATED_FEATURES_INFERENCE_PATH)

    features_train. \
        to_parquet(path=train_path, partition_cols=["customer_partition_id"])

    features_inference. \
        to_parquet(path=inference_path, partition_cols=["customer_partition_id"])

    print(f'Ended computing customers aggregated transactions features '
          f'for partition {customer_partition_id}')


if __name__ == "__main__":
    parquet_loader.remove_path(*CUSTOMERS_AGGREGATED_FEATURES_TRAIN_PATH)
    parquet_loader.remove_path(*CUSTOMERS_AGGREGATED_FEATURES_INFERENCE_PATH)

    arg_parser = argparse.ArgumentParser()

    arg_parser.add_argument('--num_cpu',
                            default=10,
                            type=int,
                            help='number of CPUs to use in execution')

    args = arg_parser.parse_args()

    customers_partitions = parquet_loader.list_partitions(*TRANSACTIONS_CLEAN_PATH)

    pool = mp.Pool(args.num_cpu)
    pool.starmap(save, customers_partitions)
    pool.close()
    pool.join()
