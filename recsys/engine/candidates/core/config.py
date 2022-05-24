CONTENT_COLUMNS = ['prod_name',
                   'product_type_name',
                   'product_group_name',
                   'graphical_appearance_name',
                   'colour_group_name',
                   'perceived_colour_value_name',
                   'perceived_colour_master_name',
                   'department_name',
                   'index_name',
                   'index_group_name',
                   'section_name',
                   'garment_group_name']

NBR_OF_TOP_TRENDING_ARTICLES = 500

PERIOD_OF_TOP_TRENDING_ARTICLES = "2W"

CANDIDATES_DF_DTYPES = \
    {'customer_id': 'int32',
     'article_id': 'int32',
     'nbr_purchases_same_prod_name': 'int16',
     'nbr_purchases_same_product_type_name': 'int16',
     'nbr_purchases_same_product_group_name': 'int16',
     'nbr_purchases_same_graphical_appearance_name': 'int16',
     'nbr_purchases_same_colour_group_name': 'int16',
     'nbr_purchases_same_perceived_colour_value_name': 'int16',
     'nbr_purchases_same_perceived_colour_master_name': 'int16',
     'nbr_purchases_same_department_name': 'int16',
     'nbr_purchases_same_index_name': 'int16',
     'nbr_purchases_same_index_group_name': 'int16',
     'nbr_purchases_same_section_name': 'int16',
     'nbr_purchases_same_garment_group_name': 'int16',
     'nbr_purchases_same_article': 'int16',

     'weeks_since_same_prod_name': 'int16',
     'weeks_since_same_product_type_name': 'int16',
     'weeks_since_same_product_group_name': 'int16',
     'weeks_since_same_graphical_appearance_name': 'int16',
     'weeks_since_same_colour_group_name': 'int16',
     'weeks_since_same_perceived_colour_value_name': 'int16',
     'weeks_since_same_perceived_colour_master_name': 'int16',
     'weeks_since_same_department_name': 'int16',
     'weeks_since_same_index_name': 'int16',
     'weeks_since_same_index_group_name': 'int16',
     'weeks_since_same_section_name': 'int16',
     'weeks_since_same_garment_group_name': 'int16',
     'weeks_since_last_purchase_same_article': 'int16',

     'nbr_purchase_product_name': "int16",
     'nbr_purchase_product_type': "int16",
     'nbr_purchase_product_group': "int16",
     'nbr_purchase_graphical_appearance_name': "int16",
     'nbr_purchase_color_group_name': "int16",
     'nbr_purchase_perceived_colour_value_name': "int16",
     'nbr_purchase_perceived_colour_master_name': "int16",
     'nbr_purchase_department_name': "int16",
     'nbr_purchase_index_name': "int16",
     'nbr_purchase_index_group_name': "int16",
     'nbr_purchase_section_name': "int16",
     'nbr_purchase_garment_group_name': "int16",

     'lifetime_days': 'int16',

     'min_price': 'float32',
     'max_price': 'float32',
     'avg_price': 'float32',
     'q_25_price': 'float32',
     'q_75_price': 'float32',

     'min_customers_age': 'float32',
     'max_customers_age': 'float32',
     'avg_customers_age': 'float32',
     'q_25_customers_age': 'float32',
     'q_75_customers_age': 'float32',

     'nbr_of_purchases': 'int32',
     'nbr_of_customers': 'int32',

     'monthly_avg_nbr_purchases': 'float32',
     'percentage_of_purchases_in_month_9': 'float32',

     'percentage_of_online_purchases': 'float32',
     'percentage_of_active_customers': 'float32',
     'percentage_of_subscribed_customers': 'float32',
     'percentage_of_regular_club_member_customers': 'float32',

     'min_purchase_delay': 'int16',
     'max_purchase_delay': 'int16',
     'avg_purchase_delay': 'int16',

     'nbr_purchases': 'int16',
     'nbr_articles': 'int16',

     'min_purchase_price': 'float32',
     'max_purchase_price': 'float32',
     'avg_purchase_price': 'float32',
     'q_25_purchase_price': 'float32',
     'q_75_purchase_price': 'float32',

     'FN': 'int8',
     'Active': 'int8',
     'club_member_status': 'int8',
     'fashion_news_frequency': 'int8',
     'age': 'float32',

     'has_purchased': 'int16'
     }
