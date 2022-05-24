import pandas as pd

from recsys.engine.candidates.core.helper import get_static_candidates


def generate_previously_bought_articles_candidates(transactions):
    candidates = transactions.copy()

    transactions_end_date = candidates.loc[:, "t_dat"].max()
    candidates.loc[:, "nbr_purchases_same_article"] = 1

    group_keys = ["customer_id", "article_id"]
    agg_dict = dict(t_dat="max", nbr_purchases_same_article="sum")
    renamed_dict = dict(t_dat="weeks_since_last_purchase_same_article")

    candidates = candidates. \
        groupby(group_keys). \
        agg(agg_dict). \
        reset_index(). \
        rename(columns=renamed_dict)

    candidates.loc[:, "weeks_since_last_purchase_same_article"] = \
        (transactions_end_date - candidates.loc[:, "weeks_since_last_purchase_same_article"])

    candidates.loc[:, "weeks_since_last_purchase_same_article"] = \
        (candidates.loc[:, "weeks_since_last_purchase_same_article"].dt.days // 7)

    return candidates


def get_trending_articles(transactions, freq="2W", n_articles=500):
    group_keys = ["article_id", pd.Grouper(key="t_dat", freq=freq)]
    agg_dict = dict(customer_id="size")
    renamed_dict = dict(customer_id="nbr_purchases")

    articles_ts = transactions. \
        groupby(group_keys). \
        agg(agg_dict). \
        reset_index(). \
        rename(columns=renamed_dict)

    articles_ts.loc[:, "total_nbr_purchases"] = \
        articles_ts.groupby("t_dat")["nbr_purchases"].transform("sum")

    articles_ts.loc[:, "purchases_percentage"] = \
        100 * articles_ts.loc[:, "nbr_purchases"] / \
        articles_ts.loc[:, "total_nbr_purchases"]

    articles_ts = articles_ts.sort_values(
        by=["t_dat", "purchases_percentage"],
        ascending=(True, False)
    )
    articles_ts.loc[:, "cumulative_purchases_percentage"] = \
        articles_ts.groupby("t_dat")["purchases_percentage"].transform("cumsum")

    articles_ts = articles_ts.loc[articles_ts.cumulative_purchases_percentage <= 95]
    articles_ts = articles_ts.loc[articles_ts.t_dat == articles_ts.t_dat.max()]
    return articles_ts.head(n_articles)


def generate_trending_articles_candidates(transactions, customers, **kwargs):
    customers = customers.drop_duplicates(subset="customer_id")
    customers = customers.loc[:, ["customer_id"]]
    articles = get_trending_articles(transactions=transactions, **kwargs)
    candidates = get_static_candidates(customers=customers, articles=articles)
    return candidates.loc[:, ["customer_id", "article_id"]]
