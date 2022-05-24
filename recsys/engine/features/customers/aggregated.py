from recsys.engine.features.utils import percentile


def compute(transactions, customers, articles):
    customers.loc[:, "is_active"] = customers.loc[:, "Active"] == 1

    customers.loc[:, "club_member_status"] = customers.loc[:, "club_member_status"].astype("object")
    customers.loc[:, "fashion_news_frequency"] = customers.loc[:, "fashion_news_frequency"].astype("object")

    active_club_member_status_mask = customers.loc[:, "club_member_status"] == "ACTIVE"
    pre_create_club_member_status_mask = customers.loc[:, "club_member_status"] == "PRE-CREATE"
    other_club_member_status_mask = ~customers.loc[:, "club_member_status"].isin(["ACTIVE", "PRE-CREATE"])

    customers.loc[active_club_member_status_mask, "club_member_status"] = 2
    customers.loc[pre_create_club_member_status_mask, "club_member_status"] = 1
    customers.loc[other_club_member_status_mask, "club_member_status"] = 0

    regular_fashion_news_mask = customers.loc[:, "fashion_news_frequency"].isin(["Monthly", "Regularly"])

    customers.loc[regular_fashion_news_mask, "fashion_news_frequency"] = 1
    customers.loc[~regular_fashion_news_mask, "fashion_news_frequency"] = 0

    customers.loc[:, "club_member_status"] = customers.loc[:, "club_member_status"].astype(int)
    customers.loc[:, "fashion_news_frequency"] = customers.loc[:, "fashion_news_frequency"].astype(int)
    customers.loc[:, "Active"] = customers.loc[:, "fashion_news_frequency"].astype(int)
    customers.loc[:, "FN"] = customers.loc[:, "fashion_news_frequency"].astype(int)

    customers = customers.loc[:, ["customer_id", "FN", "Active",
                                  "club_member_status", "fashion_news_frequency",
                                  "age"]]

    customers_purchases_info = transactions.copy()

    customers_purchases_info = customers_purchases_info.merge(right=articles,
                                                              on="article_id",
                                                              how="left",
                                                              sort=False)

    customers_purchases_info.loc[:, "article_id_1"] = customers_purchases_info.loc[:, "article_id"]
    customers_purchases_info.loc[:, "article_id_2"] = customers_purchases_info.loc[:, "article_id"]

    customers_purchases_info.loc[:, "price_1"] = customers_purchases_info.loc[:, "price"]
    customers_purchases_info.loc[:, "price_2"] = customers_purchases_info.loc[:, "price"]
    customers_purchases_info.loc[:, "price_3"] = customers_purchases_info.loc[:, "price"]
    customers_purchases_info.loc[:, "price_4"] = customers_purchases_info.loc[:, "price"]
    customers_purchases_info.loc[:, "price_5"] = customers_purchases_info.loc[:, "price"]
    customers_purchases_info.loc[:, "price_6"] = customers_purchases_info.loc[:, "price"]
    customers_purchases_info.loc[:, "price_7"] = customers_purchases_info.loc[:, "price"]

    agg_dict = dict(article_id_1="count",
                    article_id_2="nunique",

                    price_1="min",
                    price_2="max",
                    price_3="mean",
                    price_4="median",
                    price_5="std",
                    price_6=percentile("price", 25),
                    price_7=percentile("price", 75),

                    prod_name="nunique",
                    product_type_name="nunique",
                    product_group_name="nunique",
                    graphical_appearance_name="nunique",
                    colour_group_name="nunique",
                    perceived_colour_value_name="nunique",
                    perceived_colour_master_name="nunique",
                    department_name="nunique",
                    index_name="nunique",
                    index_group_name="nunique",
                    section_name="nunique",
                    garment_group_name="nunique")

    renamed_dict = dict(article_id_1="nbr_purchases",
                        article_id_2="nbr_articles",

                        price_1="min_purchase_price",
                        price_2="max_purchase_price",
                        price_3="avg_purchase_price",
                        price_4="median_purchase_price",
                        price_5="std_purchase_price",
                        price_6="q_25_purchase_price",
                        price_7="q_75_purchase_price",

                        prod_name="nbr_purchase_product_name",
                        product_type_name="nbr_purchase_product_type",
                        product_group_name="nbr_purchase_product_group",
                        graphical_appearance_name="nbr_purchase_graphical_appearance_name",
                        colour_group_name="nbr_purchase_color_group_name",
                        perceived_colour_value_name="nbr_purchase_perceived_colour_value_name",
                        perceived_colour_master_name="nbr_purchase_perceived_colour_master_name",
                        department_name="nbr_purchase_department_name",
                        index_name="nbr_purchase_index_name",
                        index_group_name="nbr_purchase_index_group_name",
                        section_name="nbr_purchase_section_name",
                        garment_group_name="nbr_purchase_garment_group_name")

    customers_purchases_info = customers_purchases_info.groupby('customer_id'). \
        agg(agg_dict).reset_index(). \
        rename(columns=renamed_dict)

    customers_seniority = transactions.copy()

    customers_seniority.loc[:, "t_dat_1"] = customers_seniority.loc[:, "t_dat"]
    customers_seniority.loc[:, "t_dat_2"] = customers_seniority.loc[:, "t_dat"]
    customers_seniority.loc[:, "t_dat_3"] = customers_seniority.loc[:, "t_dat"]

    agg_dict = dict(t_dat_1="min",
                    t_dat_2="max",
                    t_dat_3="nunique")

    renamed_dict = dict(t_dat_1="purchase_start_date",
                        t_dat_2="purchase_end_date",
                        t_dat_3="nbr_purchase_dates")

    customers_seniority = customers_seniority.groupby('customer_id'). \
        agg(agg_dict).reset_index(). \
        rename(columns=renamed_dict)

    customers_seniority.loc[:, "purchase_seniority"] = customers_seniority.loc[:, "purchase_end_date"] - \
                                                       customers_seniority.loc[:, "purchase_start_date"]

    customers_seniority.loc[:, "purchase_seniority"] = customers_seniority.loc[:, "purchase_seniority"].dt.days.astype(
        int)

    customers_purchase_dates = transactions.copy()

    customers_purchase_dates = customers_purchase_dates.loc[:, ["customer_id", "t_dat"]]
    customers_purchase_dates = customers_purchase_dates.drop_duplicates()
    customers_purchase_dates = customers_purchase_dates.sort_values(by="t_dat", ascending=True)

    customers_purchase_dates.loc[:, "t_dat_diff"] = customers_purchase_dates.groupby("customer_id").t_dat.diff()
    customers_purchase_dates.loc[:, "t_dat_diff"] = customers_purchase_dates.loc[:, "t_dat_diff"].dt.days.astype(float)

    mask = customers_purchase_dates.loc[:, "t_dat_diff"].notnull()

    customers_purchase_dates = customers_purchase_dates.loc[mask]

    customers_purchase_dates.loc[:, "t_dat_diff_1"] = customers_purchase_dates.loc[:, "t_dat_diff"]
    customers_purchase_dates.loc[:, "t_dat_diff_2"] = customers_purchase_dates.loc[:, "t_dat_diff"]
    customers_purchase_dates.loc[:, "t_dat_diff_3"] = customers_purchase_dates.loc[:, "t_dat_diff"]
    customers_purchase_dates.loc[:, "t_dat_diff_4"] = customers_purchase_dates.loc[:, "t_dat_diff"]
    customers_purchase_dates.loc[:, "t_dat_diff_5"] = customers_purchase_dates.loc[:, "t_dat_diff"]
    customers_purchase_dates.loc[:, "t_dat_diff_6"] = customers_purchase_dates.loc[:, "t_dat_diff"]
    customers_purchase_dates.loc[:, "t_dat_diff_7"] = customers_purchase_dates.loc[:, "t_dat_diff"]

    agg_dict = dict(t_dat_diff_1="min",
                    t_dat_diff_2="max",
                    t_dat_diff_3="mean",
                    t_dat_diff_4="median",
                    t_dat_diff_5="std",
                    t_dat_diff_6=percentile("t_dat_diff", 25),
                    t_dat_diff_7=percentile("t_dat_diff", 75))

    renamed_dict = dict(t_dat_diff_1="min_purchase_delay",
                        t_dat_diff_2="max_purchase_delay",
                        t_dat_diff_3="avg_purchase_delay",
                        t_dat_diff_4="median_purchase_delay",
                        t_dat_diff_5="std_purchase_delay",
                        t_dat_diff_6="q_25_purchase_delay",
                        t_dat_diff_7="q_75_purchase_delay")

    customers_purchase_dates = customers_purchase_dates.groupby('customer_id'). \
        agg(agg_dict).reset_index(). \
        rename(columns=renamed_dict)

    customers_aggregated_features = customers_seniority.copy()

    customers_aggregated_features = customers_aggregated_features.merge(right=customers_purchase_dates,
                                                                        on="customer_id",
                                                                        how="outer",
                                                                        sort=False)

    customers_aggregated_features = customers_aggregated_features.merge(right=customers_purchases_info,
                                                                        on="customer_id",
                                                                        how="outer",
                                                                        sort=False)

    customers_aggregated_features = customers_aggregated_features.merge(
        right=customers,
        on="customer_id",
        how="right",
        sort=False
    )
    return customers_aggregated_features
