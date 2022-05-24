import argparse
import multiprocessing as mp
import sys

from recsys.dataloaders.file import ParquetLoader
from recsys.engine.candidates.core.config import (
    CONTENT_COLUMNS,
    NBR_OF_TOP_TRENDING_ARTICLES,
    PERIOD_OF_TOP_TRENDING_ARTICLES,
    CANDIDATES_DF_DTYPES
)
from recsys.engine.candidates.core.embed import (
    embed_articles_content,
    embed_customers_content,
    embed_content_features
)
from recsys.engine.candidates.core.generate import (
    get_trending_articles,
    generate_previously_bought_articles_candidates
)
from recsys.engine.candidates.core.helper import (
    get_static_candidates,
    add_target_to_candidates,
    combine_candidates,
    consolidate_candidates
)
from recsys.engine.validation.metrics import (
    compute_recall
)
from recsys.engine.validation.utils import split_on_date
from scripts.settings import (
    DATA_ROOT_DIR,
    TRANSACTIONS_CLEAN_PATH,
    ARTICLES_CLEAN_PATH,
    ARTICLES_AGGREGATED_FEATURES_TRAIN_PATH,
    CUSTOMERS_AGGREGATED_FEATURES_TRAIN_PATH,
    CANDIDATES_TRAIN_PATH
)

parquet_loader = ParquetLoader(DATA_ROOT_DIR)

all_transactions = parquet_loader.load_data_frame(*TRANSACTIONS_CLEAN_PATH)
articles = parquet_loader.load_data_frame(*ARTICLES_CLEAN_PATH)
articles_aggregated_features = parquet_loader. \
    load_data_frame(*ARTICLES_AGGREGATED_FEATURES_TRAIN_PATH)

all_transactions_before, all_transactions_after = split_on_date(all_transactions)

articles_content_feature = embed_articles_content(articles=articles, content_columns=CONTENT_COLUMNS)

all_trending_articles_before = get_trending_articles(transactions=all_transactions_before,
                                                     n_articles=NBR_OF_TOP_TRENDING_ARTICLES,
                                                     freq=PERIOD_OF_TOP_TRENDING_ARTICLES)

all_trending_articles_before = all_trending_articles_before.loc[:, ["article_id"]]


def generate_and_embed_candidates(customer_partition_id):
    sys.stdout.write(f"embedding candidates for partition {customer_partition_id} \n")

    transactions = parquet_loader. \
        load_data_frame(*TRANSACTIONS_CLEAN_PATH,
                        customer_partition_id=customer_partition_id)

    customers_aggregated_features = parquet_loader. \
        load_data_frame(*CUSTOMERS_AGGREGATED_FEATURES_TRAIN_PATH,
                        customer_partition_id=customer_partition_id
                        )

    transaction_before, transactions_after = split_on_date(transactions)

    customers_content_features = embed_customers_content(transactions=transaction_before,
                                                         articles=articles,
                                                         content_columns=CONTENT_COLUMNS)

    customers = transactions_after.loc[:, ["customer_id"]].drop_duplicates()

    trending_candidates = get_static_candidates(customers=customers,
                                                articles=all_trending_articles_before)

    previously_purchased_candidates = \
        generate_previously_bought_articles_candidates(transactions=transaction_before)

    candidates = combine_candidates(trending_candidates, previously_purchased_candidates)

    candidates = consolidate_candidates(candidates, transactions=transactions_after)

    candidates = embed_content_features(candidates=candidates,
                                        customers_content=customers_content_features,
                                        articles_content=articles_content_feature)

    candidates = candidates.merge(
        right=previously_purchased_candidates,
        on=["customer_id", "article_id"],
        how="left"
    )

    candidates.loc[:, "nbr_purchases_same_article"] = \
        candidates.loc[:, "nbr_purchases_same_article"].fillna(0)

    candidates.loc[:, "weeks_since_last_purchase_same_article"] = \
        candidates.loc[:, "weeks_since_last_purchase_same_article"].fillna(-1)

    candidates = candidates.merge(
        right=articles_aggregated_features,
        on="article_id",
        how="left",
        sort=False
    )

    candidates = candidates.merge(
        right=customers_aggregated_features,
        on="customer_id",
        how="left",
        sort=False
    )

    candidates = add_target_to_candidates(candidates=candidates,
                                          transactions=transactions_after)

    for col, dtype in CANDIDATES_DF_DTYPES.items():
        if 'int' in dtype:
            candidates.loc[:, col] = candidates.loc[:, col].fillna(0)

    candidates = candidates.astype(CANDIDATES_DF_DTYPES)

    candidates = candidates.loc[:, list(CANDIDATES_DF_DTYPES.keys())]

    recall = compute_recall(actual=transactions_after, predicted=candidates)

    sys.stdout.write(f"candidates recall {recall} \n")

    path = parquet_loader.make_path(*CANDIDATES_TRAIN_PATH)

    candidates = candidates.assign(customer_partition_id=customer_partition_id)

    candidates.to_parquet(path=path, partition_cols=["customer_partition_id"])

    sys.stdout.flush()


if __name__ == "__main__":
    parquet_loader.remove_path(*CANDIDATES_TRAIN_PATH)

    arg_parser = argparse.ArgumentParser()

    arg_parser.add_argument('--num_cpu',
                            default=3,
                            type=int,
                            help='number of CPUs to use in execution')

    args = arg_parser.parse_args()

    customers_partitions = parquet_loader.list_partitions(*TRANSACTIONS_CLEAN_PATH)

    pool = mp.Pool(args.num_cpu)
    pool.starmap(generate_and_embed_candidates, customers_partitions)
    pool.close()
    pool.join()
