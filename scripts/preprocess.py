from pandas import to_datetime

from recsys.dataloaders.file import (
    ParquetLoader,
    CsvLoader
)
from scripts.settings import (
    DATA_ROOT_DIR,
    ARTICLES_RAW_PATH,
    CUSTOMERS_RAW_PATH,
    TRANSACTIONS_RAW_PATH,
    ARTICLES_CLEAN_PATH,
    CUSTOMERS_CLEAN_PATH,
    TRANSACTIONS_CLEAN_PATH,
    NBR_OF_ARTICLES_PARTITIONS,
    NBR_OF_CUSTOMERS_PARTITIONS
)

ARTICLES_COLUMNS_TO_DROP = (
    "product_code",
    "product_type_no",
    "graphical_appearance_no",
    "colour_group_code",
    "perceived_colour_value_id",
    "perceived_colour_master_id",
    "department_no",
    "index_code",
    "index_group_no",
    "section_no",
    "garment_group_no"
)

csv_loader = CsvLoader(DATA_ROOT_DIR)
parquet_loader = ParquetLoader(DATA_ROOT_DIR)


def consolidate_customers():
    print("Started consolidating customers")
    customers = csv_loader.load_data_frame(*CUSTOMERS_RAW_PATH)
    customers.loc[:, "customer_id"] = customers.loc[:, "customer_id"].astype("string")
    customers.loc[:, "postal_code"] = customers.loc[:, "postal_code"].astype("string")

    customers.loc[:, "fashion_news_frequency"] = customers.loc[:, "fashion_news_frequency"].astype("string")
    customers.loc[:, "club_member_status"] = customers.loc[:, "club_member_status"].astype("string")

    customers.loc[:, "FN"] = customers.loc[:, "FN"].astype("float64")
    customers.loc[:, "Active"] = customers.loc[:, "Active"].astype("float64")
    customers.loc[:, "age"] = customers.loc[:, "age"].astype("float64")

    customers.loc[customers.FN.isnull(), "FN"] = 0
    customers.loc[customers.Active.isnull(), "Active"] = 0
    customers.loc[customers.club_member_status.isnull(), "club_member_status"] = "none"

    customers.loc[customers.fashion_news_frequency.isin(["None", "NONE"]), "fashion_news_frequency"] = "none"
    customers.loc[customers.fashion_news_frequency.isnull(), "fashion_news_frequency"] = "none"

    assert customers.duplicated(subset=["customer_id"]).sum() == 0

    customers = customers.sample(frac=1)
    customers = customers.rename(columns=dict(customer_id="original_customer_id"))
    customers.loc[:, "customer_id"] = range(customers.shape[0])
    customers.loc[:, "customer_id"] = customers.loc[:, "customer_id"].astype(int)
    customers.loc[:, "customer_partition_id"] = customers.loc[:, "customer_id"] % NBR_OF_CUSTOMERS_PARTITIONS + 1

    path = csv_loader.make_path(*CUSTOMERS_CLEAN_PATH)
    customers.to_parquet(path=path, partition_cols=["customer_partition_id"])

    print("Ended consolidating customers")


def consolidate_articles():
    print("Started consolidating articles")

    articles = csv_loader.load_data_frame(*ARTICLES_RAW_PATH,
                                          dtype=dict(article_id="string"))

    articles = articles.drop(columns=list(ARTICLES_COLUMNS_TO_DROP))
    article_info_columns = [col for col in articles.columns]

    for col in article_info_columns:
        articles.loc[:, col] = articles.loc[:, col].astype("string")

    articles.loc[:, "detail_desc"] = articles.loc[:, "detail_desc"].fillna("")

    assert articles.duplicated(subset=["article_id"]).sum() == 0

    articles = articles.sample(frac=1)
    articles = articles.rename(columns=dict(article_id="original_article_id"))
    articles.loc[:, "article_id"] = range(articles.shape[0])
    articles.loc[:, "article_id"] = articles.loc[:, "article_id"].astype(int)
    articles.loc[:, "article_partition_id"] = articles.loc[:, "article_id"] % NBR_OF_ARTICLES_PARTITIONS + 1

    path = csv_loader.make_path(*ARTICLES_CLEAN_PATH)
    articles.to_parquet(path=path, partition_cols=["article_partition_id"])

    print("Ended consolidating articles")


def consolidate_transactions():
    print("Started consolidating transactions")

    transactions = csv_loader.load_data_frame(*TRANSACTIONS_RAW_PATH,
                                              dtype=dict(article_id="string"))

    transactions.loc[:, "t_dat"] = to_datetime(transactions.loc[:, "t_dat"])
    transactions.loc[:, "customer_id"] = transactions.loc[:, "customer_id"].astype("string")
    transactions.loc[:, "article_id"] = transactions.loc[:, "article_id"].astype("string")
    transactions.loc[:, "price"] = transactions.loc[:, "price"].astype("float64")
    transactions.loc[:, "sales_channel_id"] = transactions.loc[:, "sales_channel_id"].astype("int64")

    transactions = transactions.rename(columns=dict(customer_id="original_customer_id",
                                                    article_id="original_article_id"))

    articles_cols = [
        "original_article_id",
        "article_id",
        "article_partition_id"
    ]

    customers_cols = [
        "original_customer_id",
        "customer_id",
        "customer_partition_id"
    ]

    articles = parquet_loader.load_data_frame(*ARTICLES_CLEAN_PATH,
                                              columns=articles_cols)

    customers = parquet_loader.load_data_frame(*CUSTOMERS_CLEAN_PATH,
                                               columns=customers_cols)

    transactions = transactions.merge(
        right=customers,
        on="original_customer_id",
        how="left")

    transactions = transactions.merge(
        right=articles,
        on="original_article_id",
        how="left")

    transactions = transactions.drop(columns=['original_customer_id',
                                              "original_article_id"])

    path = parquet_loader.get_path(*TRANSACTIONS_CLEAN_PATH)

    transactions = transactions.loc[transactions.t_dat >= to_datetime("2020-09-01")]
    transactions.to_parquet(path=path, partition_cols=["customer_partition_id"])

    print("Ended consolidating transactions")


if __name__ == "__main__":
    parquet_loader.remove_path(*ARTICLES_CLEAN_PATH)
    parquet_loader.remove_path(*CUSTOMERS_CLEAN_PATH)
    parquet_loader.remove_path(*TRANSACTIONS_CLEAN_PATH)

    consolidate_articles()
    consolidate_customers()
    consolidate_transactions()
