from recsys.dataloaders.file import ParquetLoader
from scripts.settings import (
    DATA_ROOT_DIR,
    TRANSACTIONS_CLEAN_PATH,
    TRANSACTIONS_REPARTITIONED_BY_ARTICLE_CLEAN_PATH
)

parquet_loader = ParquetLoader(DATA_ROOT_DIR)


def repartition_transactions_by_article():
    print("Started repartitioning transactions by article")
    transactions = parquet_loader.load_data_frame(*TRANSACTIONS_CLEAN_PATH)
    transactions = transactions.drop(columns=['customer_partition_id'])
    path = parquet_loader.make_path(*TRANSACTIONS_REPARTITIONED_BY_ARTICLE_CLEAN_PATH)
    transactions.to_parquet(path=path, partition_cols=["article_partition_id"])
    print("End repartitioning transactions by article")


if __name__ == "__main__":
    parquet_loader.remove_path(*TRANSACTIONS_REPARTITIONED_BY_ARTICLE_CLEAN_PATH)
    repartition_transactions_by_article()
