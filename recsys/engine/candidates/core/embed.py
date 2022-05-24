import pandas as pd


def embed_articles_content(articles, content_columns):
    articles_content = articles.loc[:, content_columns]
    articles_content = articles_content.stack().rename("value").reset_index()
    renamed_dict = dict(level_0="index", level_1="variable")
    articles_content = articles_content.rename(columns=renamed_dict)
    articles_content = articles_content.merge(
        right=articles.loc[:, ["article_id"]].reset_index(drop=False),
        on="index",
        how="outer"
    )
    articles_content = articles_content.drop("index", axis=1)
    return articles_content


def embed_customers_content(transactions, articles, content_columns):
    transactions = transactions.merge(
        right=articles.loc[:, ["article_id"] + content_columns],
        on="article_id",
        how="left",
        sort=False
    )

    transactions_end_date = transactions.t_dat.max()

    customers_content = list()

    for col in content_columns:
        group_keys = ["customer_id", col]
        agg_dict = dict(article_id="size", t_dat="max")
        renamed_dict = dict(article_id="nbr_purchases", t_dat="weeks_since_last_purchase")

        df = transactions. \
            groupby(group_keys). \
            agg(agg_dict). \
            reset_index(). \
            rename(columns=renamed_dict)

        df.loc[:, "weeks_since_last_purchase"] = (transactions_end_date - df.loc[:, "weeks_since_last_purchase"])
        df.loc[:, "weeks_since_last_purchase"] = (df.loc[:, "weeks_since_last_purchase"].dt.days // 7)

        df.loc[:, "variable"] = col
        df.loc[:, "value"] = df.loc[:, col]
        df = df.drop(col, axis=1)
        customers_content.append(df)

    customers_content = pd.concat(customers_content)

    return customers_content


def embed_content_features(candidates, articles_content, customers_content):
    content_columns = list(articles_content.variable.unique())

    candidates = candidates.merge(
        right=articles_content,
        on="article_id",
        how="left",
        sort=False
    )

    candidates = candidates.merge(
        right=customers_content,
        on=["customer_id", "variable", "value"],
        how="left",
        sort=False
    )

    candidates.loc[:, "nbr_purchases"] = candidates.loc[:, "nbr_purchases"].fillna(0)
    candidates.loc[:, "weeks_since_last_purchase"] = candidates.loc[:, "weeks_since_last_purchase"].fillna(-1)

    candidates_1 = candidates.pivot(index=["customer_id", "article_id"],
                                    columns=["variable"],
                                    values=["nbr_purchases"])

    candidates_2 = candidates.pivot(index=["customer_id", "article_id"],
                                    columns=["variable"],
                                    values=["weeks_since_last_purchase"])

    candidates_1 = candidates_1.reset_index()

    candidates_1.columns = ["customer_id", "article_id"] + content_columns
    renamed_dict = {col: "nbr_purchases_same_" + col for col in content_columns}
    candidates_1 = candidates_1.rename(columns=renamed_dict)

    candidates_2 = candidates_2.reset_index()

    candidates_2.columns = ["customer_id", "article_id"] + content_columns
    renamed_dict = {col: "weeks_since_same_" + col for col in content_columns}
    candidates_2 = candidates_2.rename(columns=renamed_dict)

    candidates = candidates_1.merge(
        right=candidates_2,
        on=["customer_id", "article_id"],
        how="outer",
        sort=False
    )

    return candidates
