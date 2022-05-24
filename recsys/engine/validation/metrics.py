import numpy as np
import pandas as pd


def apk(actual: list, predicted: list, k: int = 12):
    """
    :param actual:
    :param predicted:
    :param k:
    :return:
    """
    if len(predicted) > k:
        predicted = predicted[:k]
    score = 0.0
    num_hits = 0.0
    for i, p in enumerate(predicted):
        if p in actual and p not in predicted[:i]:
            num_hits += 1.0
            score += num_hits / (i + 1.0)
    if not actual:
        return 0.0
    return score / min(len(actual), k)


def compute_map_k(actual: pd.DataFrame, predicted: pd.DataFrame, k: int = 12):
    """
    :param actual:
    :param predicted:
    :param k:
    :return:
    """
    assert "score" in predicted.columns
    predicted = predicted.loc[predicted.customer_id.isin(actual.customer_id)].copy()
    actual = actual.copy()
    actual = actual.sort_values(by="customer_id", ascending=True)
    predicted = predicted.sort_values(by=["customer_id", "score"], ascending=(True, False))
    predicted = predicted.groupby("customer_id").article_id.apply(list)
    actual = actual.groupby("customer_id").article_id.apply(list)
    return np.mean([apk(a, p, k) for a, p in zip(actual, predicted)])


def compute_recall(actual: pd.DataFrame, predicted: pd.DataFrame, k: int = None):
    """
    :param actual:
    :param predicted:
    :param k:
    :return:
    """
    predicted = predicted.copy()
    if k is not None:
        predicted = predicted.groupby("customer_id").head(k)
    predicted = predicted.merge(
        right=actual,
        on=["customer_id", "article_id"],
        how="inner",
        sort=False
    )
    return predicted.shape[0] / actual.shape[0]
