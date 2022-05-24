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
    combine_candidates
)
from scripts.settings import (
    DATA_ROOT_DIR,
    TRANSACTIONS_CLEAN_PATH,
    ARTICLES_CLEAN_PATH,
    CUSTOMERS_CLEAN_PATH,
    ARTICLES_AGGREGATED_FEATURES_INFERENCE_PATH,
    CUSTOMERS_AGGREGATED_FEATURES_INFERENCE_PATH,
    CANDIDATES_INFERENCE_PATH
)

parquet_loader = ParquetLoader(DATA_ROOT_DIR)

all_transactions = parquet_loader. \
    load_data_frame(*TRANSACTIONS_CLEAN_PATH
                    )

articles = parquet_loader. \
    load_data_frame(*ARTICLES_CLEAN_PATH
                    )

articles_aggregated_features = parquet_loader. \
    load_data_frame(*ARTICLES_AGGREGATED_FEATURES_INFERENCE_PATH
                    )

articles_content_feature = embed_articles_content(articles=articles,
                                                  content_columns=CONTENT_COLUMNS)

all_trending_articles = get_trending_articles(transactions=all_transactions,
                                              n_articles=NBR_OF_TOP_TRENDING_ARTICLES,
                                              freq=PERIOD_OF_TOP_TRENDING_ARTICLES)

all_trending_articles = all_trending_articles.loc[:, ["article_id"]]


def generate_and_embed_candidates(customer_partition_id):
    sys.stdout.write(f"embedding candidates for partition {customer_partition_id} \n")

    customers = parquet_loader.load_data_frame(*CUSTOMERS_CLEAN_PATH,
                                               columns=["customer_id"],
                                               customer_partition_id=customer_partition_id
                                               )

    transactions = parquet_loader. \
        load_data_frame(*TRANSACTIONS_CLEAN_PATH,
                        customer_partition_id=customer_partition_id,
                        )

    customers_aggregated_features = parquet_loader. \
        load_data_frame(*CUSTOMERS_AGGREGATED_FEATURES_INFERENCE_PATH,
                        customer_partition_id=customer_partition_id,

                        )

    customers_content_features = embed_customers_content(transactions=transactions,
                                                         articles=articles,
                                                         content_columns=CONTENT_COLUMNS)

    trending_candidates = get_static_candidates(customers=customers,
                                                articles=all_trending_articles)

    previously_purchased_candidates = \
        generate_previously_bought_articles_candidates(transactions=transactions)

    candidates = combine_candidates(trending_candidates, previously_purchased_candidates)

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

    dtypes = dict()

    for col, dtype in CANDIDATES_DF_DTYPES.items():
        if col != "has_purchased":
            dtypes[col] = dtype
            if 'int' in dtype:
                candidates.loc[:, col] = candidates.loc[:, col].fillna(0)

    candidates = candidates.astype(dtypes)

    candidates = candidates.loc[:, list(dtypes.keys())]

    path = parquet_loader.make_path(*CANDIDATES_INFERENCE_PATH)

    candidates = candidates.assign(customer_partition_id=customer_partition_id)

    candidates.to_parquet(path=path, partition_cols=["customer_partition_id"])

    sys.stdout.flush()


if __name__ == "__main__":
    parquet_loader.remove_path(*CANDIDATES_INFERENCE_PATH)

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
