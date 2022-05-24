import pandas as pd


def get_static_candidates(customers: pd.DataFrame, articles: pd.DataFrame):
    """
    :param customers:
    :param articles:
    :return:
    """
    assert "customer_id" in customers.columns
    assert "article_id" in articles.columns
    return customers.merge(
        right=articles,
        how="cross")


def add_target_to_candidates(candidates: pd.DataFrame, transactions: pd.DataFrame):
    """
    :param candidates:
    :param transactions:
    :return:
    """
    for col in ("customer_id", "article_id"):
        for df in (candidates, transactions):
            assert col in df.columns
    transactions = transactions.copy()
    transactions = transactions.drop_duplicates(subset=["customer_id", "article_id"])
    transactions = transactions.assign(has_purchased=1)
    transactions = transactions.loc[:, ["customer_id", "article_id", "has_purchased"]]
    candidates = candidates.merge(
        right=transactions,
        on=["customer_id", "article_id"],
        how="left",
        sort=False
    )
    candidates.loc[:, "has_purchased"] = candidates.loc[:, "has_purchased"].fillna(0)
    return candidates


def combine_candidates(*candidates):
    for df in candidates:
        assert "customer_id" in df.columns
        assert "article_id" in df.columns

    candidates = pd.concat(objs=list(candidates), axis=0, ignore_index=True)
    candidates = candidates.drop_duplicates(subset=["customer_id", "article_id"])
    return candidates


def consolidate_candidates(candidates, transactions):
    assert "customer_id" in candidates.columns
    assert "customer_id" in transactions.columns
    return candidates.loc[candidates.customer_id.isin(transactions.customer_id)].copy()


def filter_negative_customers(candidates: pd.DataFrame):
    """
    :param candidates:
    :return:
    """
    assert "has_purchased" in candidates.columns
    assert "article_id" in candidates.columns
    assert "customer_id" in candidates.columns
    mask = candidates.groupby("customer_id")["has_purchased"].transform("sum") > 0
    return candidates.loc[mask].copy()


def score_candidates(model,
                     features,
                     candidates: pd.DataFrame, **kwargs):
    """
    :param model:
    :param features:
    :param candidates:
    :param kwargs:
    :return:
    """
    assert "customer_id" in candidates.columns
    assert "article_id" in candidates.columns

    prediction = pd.DataFrame(
        data=model.predict(features, **kwargs),
        columns=["score"]
    )

    candidates = candidates.loc[:, ["customer_id", "article_id"]].copy()
    candidates = candidates.reset_index(drop=True)

    return pd.concat(
        objs=[candidates, prediction],
        axis=1)


def rank_candidates(candidates: pd.DataFrame):
    assert "score" in candidates.columns
    return candidates.sort_values(by=["customer_id", "score"], ascending=(True, False))
