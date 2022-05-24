import pandas as pd

from recsys.engine.features.utils import percentile


def compute(transactions, customers):
    articles_purchase_dates = transactions.copy()

    articles_purchase_dates.loc[:, "purchase_start_date"] = articles_purchase_dates.loc[:, "t_dat"]
    articles_purchase_dates.loc[:, "purchase_end_date"] = articles_purchase_dates.loc[:, "t_dat"]
    articles_purchase_dates.loc[:, "purchase_nbr_of_dates"] = articles_purchase_dates.loc[:, "t_dat"]

    agg_dict = dict(purchase_start_date="min",
                    purchase_end_date="max",
                    purchase_nbr_of_dates="nunique")

    articles_purchase_dates = articles_purchase_dates. \
        groupby("article_id"). \
        agg(agg_dict). \
        reset_index()

    articles_purchase_dates.loc[:, "lifetime_days"] = articles_purchase_dates.loc[:, "purchase_end_date"] - \
                                                      articles_purchase_dates.loc[:, "purchase_start_date"]

    articles_purchase_dates.loc[:, "lifetime_days"] = articles_purchase_dates.loc[:, "lifetime_days"]. \
        dt.days.astype(int)

    articles_price_features = transactions.copy()

    price_cols = [
        "min_price",
        "max_price",
        "avg_price",
        "median_price",
        "q_25_price",
        "q_75_price"
    ]

    for col in price_cols:
        articles_price_features.loc[:, col] = articles_price_features.loc[:, "price"]

    agg_dict = {
        "min_price": "min",
        "max_price": "max",
        "avg_price": "mean",
        "median_price": "median",
        "q_25_price": percentile("price", 25),
        "q_75_price": percentile("price", 75),
    }

    articles_price_features = articles_price_features. \
        groupby("article_id"). \
        agg(agg_dict). \
        reset_index()

    articles_customers_features = transactions.copy()

    articles_customers_features = articles_customers_features.merge(
        right=customers,
        on="customer_id",
        how="left"
    )

    age_cols = [
        "min_customers_age",
        "max_customers_age",
        "avg_customers_age",
        "median_customers_age",
        "q_25_customers_age",
        "q_75_customers_age"
    ]
    for col in age_cols:
        articles_customers_features.loc[:, col] = articles_customers_features.loc[:, "age"]

    articles_customers_features.loc[:, "nbr_of_store_purchases"] = \
        articles_customers_features.loc[:, "sales_channel_id"] == 1

    articles_customers_features.loc[:, "nbr_of_online_purchases"] = \
        articles_customers_features.loc[:, "sales_channel_id"] == 2

    active_customers_mask = articles_customers_features.loc[:, "Active"] == 1
    fn_customers_mask = articles_customers_features.loc[:, "FN"] == 1
    regular_club_member_status_mask = articles_customers_features.loc[:, "club_member_status"]. \
        str.lower().isin(["regularly", "monthly"])

    articles_customers_features.loc[active_customers_mask, "nbr_of_active_customers"] = \
        articles_customers_features.loc[active_customers_mask, "customer_id"]

    articles_customers_features.loc[~active_customers_mask, "nbr_of_inactive_customers"] = \
        articles_customers_features.loc[~active_customers_mask, "customer_id"]

    articles_customers_features.loc[fn_customers_mask, "nbr_of_subscribed_customers"] = \
        articles_customers_features.loc[fn_customers_mask, "customer_id"]

    articles_customers_features.loc[~fn_customers_mask, "nbr_of_non_subscribed_customers"] = \
        articles_customers_features.loc[~fn_customers_mask, "customer_id"]

    articles_customers_features.loc[regular_club_member_status_mask, "nbr_of_regular_club_member_customers"] = \
        articles_customers_features.loc[regular_club_member_status_mask, "customer_id"]

    articles_customers_features.loc[~regular_club_member_status_mask, "nbr_of_non_regular_club_member_customers"] = \
        articles_customers_features.loc[~regular_club_member_status_mask, "customer_id"]

    articles_customers_features.loc[:, "nbr_of_purchases"] = articles_customers_features.loc[:, "customer_id"]
    articles_customers_features.loc[:, "nbr_of_customers"] = articles_customers_features.loc[:, "customer_id"]

    agg_dict = {
        "min_customers_age": "min",
        "max_customers_age": "max",
        "avg_customers_age": "mean",
        "median_customers_age": "median",
        "q_25_customers_age": percentile("age", 25),
        "q_75_customers_age": percentile("age", 75),

        "nbr_of_online_purchases": "sum",
        "nbr_of_store_purchases": "sum",

        "nbr_of_active_customers": "nunique",
        "nbr_of_inactive_customers": "nunique",
        "nbr_of_subscribed_customers": "nunique",
        "nbr_of_non_subscribed_customers": "nunique",
        "nbr_of_regular_club_member_customers": "nunique",
        "nbr_of_non_regular_club_member_customers": "nunique",

        "nbr_of_purchases": "count",
        "nbr_of_customers": "nunique"

    }

    articles_customers_features = articles_customers_features. \
        groupby("article_id"). \
        agg(agg_dict). \
        reset_index()

    null_q25_price = articles_customers_features.loc[:, "q_25_customers_age"].isnull()
    null_q75_price = articles_customers_features.loc[:, "q_75_customers_age"].isnull()

    articles_customers_features.loc[null_q25_price, "q_25_customers_age"] = \
        articles_customers_features.loc[null_q25_price, "avg_customers_age"]

    articles_customers_features.loc[null_q75_price, "q_75_customers_age"] = \
        articles_customers_features.loc[null_q75_price, "avg_customers_age"]

    articles_seasonal_features = transactions.copy()

    articles_seasonal_features.loc[:, "month"] = articles_seasonal_features.loc[:, "t_dat"].dt.month

    for month in range(1, 13):
        col = f"nbr_of_purchases_in_month_{month}"
        articles_seasonal_features.loc[:, col] = articles_seasonal_features.loc[:, "month"] == month

    month_cols = [f'nbr_of_purchases_in_month_{month}' for month in range(1, 13)]
    agg_dict = dict.fromkeys(month_cols, "sum")

    articles_seasonal_features = articles_seasonal_features. \
        groupby("article_id"). \
        agg(agg_dict). \
        reset_index()

    group_keys = ["article_id", pd.Grouper(key="t_dat", freq="M")]
    agg_dict = dict(customer_id="count")
    renamed_dict = dict(customer_id="monthly_avg_nbr_purchases")

    articles_monthly_avg_purchases = transactions. \
        groupby(group_keys). \
        agg(agg_dict). \
        reset_index(). \
        rename(columns=renamed_dict)

    articles_monthly_avg_purchases = articles_monthly_avg_purchases. \
        groupby("article_id"). \
        monthly_avg_nbr_purchases. \
        mean(). \
        reset_index()

    articles_seasonal_features = articles_seasonal_features.merge(
        right=articles_monthly_avg_purchases,
        on="article_id",
        how="outer",
        sort=False
    )

    articles_aggregated_features = articles_purchase_dates.merge(
        right=articles_price_features,
        on="article_id",
        how="outer",
        sort=False
    )

    articles_aggregated_features = articles_aggregated_features.merge(
        right=articles_customers_features,
        on="article_id",
        how="outer",
        sort=False
    )

    articles_aggregated_features = articles_aggregated_features.merge(
        right=articles_seasonal_features,
        on="article_id",
        how="outer",
        sort=False
    )

    articles_aggregated_features.loc[:, "percentage_of_online_purchases"] = \
        100 * articles_aggregated_features.loc[:, "nbr_of_online_purchases"] / \
        articles_aggregated_features.loc[:, "nbr_of_purchases"]

    articles_aggregated_features.loc[:, "percentage_of_store_purchases"] = \
        100 * articles_aggregated_features.loc[:, "nbr_of_store_purchases"] / \
        articles_aggregated_features.loc[:, "nbr_of_purchases"]

    articles_aggregated_features.loc[:, "percentage_of_active_customers"] = \
        100 * articles_aggregated_features.loc[:, "nbr_of_active_customers"] / \
        articles_aggregated_features.loc[:, "nbr_of_customers"]

    articles_aggregated_features.loc[:, "percentage_of_inactive_customers"] = \
        100 * articles_aggregated_features.loc[:, "nbr_of_inactive_customers"] / \
        articles_aggregated_features.loc[:, "nbr_of_customers"]

    articles_aggregated_features.loc[:, "percentage_of_subscribed_customers"] = \
        100 * articles_aggregated_features.loc[:, "nbr_of_subscribed_customers"] / \
        articles_aggregated_features.loc[:, "nbr_of_customers"]

    articles_aggregated_features.loc[:, "percentage_of_non_subscribed_customers"] = \
        100 * articles_aggregated_features.loc[:, "nbr_of_non_subscribed_customers"] / \
        articles_aggregated_features.loc[:, "nbr_of_customers"]

    articles_aggregated_features.loc[:, "percentage_of_regular_club_member_customers"] = \
        100 * articles_aggregated_features.loc[:, "nbr_of_regular_club_member_customers"] / \
        articles_aggregated_features.loc[:, "nbr_of_customers"]

    articles_aggregated_features.loc[:, "percentage_of_non_regular_club_member_customers"] = \
        100 * articles_aggregated_features.loc[:, "nbr_of_non_regular_club_member_customers"] / \
        articles_aggregated_features.loc[:, "nbr_of_customers"]

    articles_aggregated_features.loc[:, "percentage_of_active_customers"] = \
        100 * articles_aggregated_features.loc[:, "nbr_of_active_customers"] / \
        articles_aggregated_features.loc[:, "nbr_of_customers"]

    for month in range(1, 13):
        articles_aggregated_features.loc[:, f"percentage_of_purchases_in_month_{month}"] = \
            100 * articles_aggregated_features.loc[:, f"nbr_of_purchases_in_month_{month}"] / \
            articles_aggregated_features.loc[:, "nbr_of_purchases"]

    return articles_aggregated_features
