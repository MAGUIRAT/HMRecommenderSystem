import pandas as pd

from recsys.dataloaders.file import ParquetLoader, CsvLoader
from scripts.settings import (
    ARTICLES_CLEAN_PATH,
    CUSTOMERS_CLEAN_PATH
)

parquet_loader = ParquetLoader()
csv_loader = CsvLoader()

articles = parquet_loader.load_data_frame(*ARTICLES_CLEAN_PATH,
                                          columns=["article_id", "original_article_id"])

customers = parquet_loader.load_data_frame(*CUSTOMERS_CLEAN_PATH,
                                           columns=["customer_id", "original_customer_id"])


def recover_original_customer_id(df):
    assert "customer_id" in df.columns
    df = df.merge(
        right=customers,
        on="customer_id",
        how="left",
        sort=False
    )
    df = df.drop("customer_id", axis=1)
    df = df.rename(columns=dict(original_customer_id="customer_id"))
    return df


def recover_original_article_id(df):
    assert "article_id" in df.columns
    df = df.merge(
        right=articles,
        on="article_id",
        how="left",
        sort=False
    )
    df = df.drop("article_id", axis=1)
    df = df.rename(columns=dict(original_article_id="article_id"))
    return df


def get_submission(prediction):
    assert "score" in prediction.columns
    prediction = prediction.copy()
    prediction = recover_original_article_id(prediction)
    prediction = recover_original_customer_id(prediction)
    prediction = prediction.sort_values(by="score", ascending=False)
    prediction.loc[:, "article_id"] = " " + prediction.loc[:, "article_id"]
    prediction = prediction. \
        groupby("customer_id"). \
        article_id. \
        sum(). \
        reset_index()
    missing_prediction = prediction.loc[:, "article_id"] == "0"
    prediction.loc[missing_prediction, "article_id"] = pd.NA
    prediction = prediction.rename(columns=dict(article_id="prediction"))
    return prediction
