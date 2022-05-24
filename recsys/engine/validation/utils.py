import pandas as pd
from pandas import to_datetime

VALIDATION_START_DATE = pd.to_datetime("2020-09-16")


def parse_date(date):
    if isinstance(date, str):
        date = to_datetime(date)
    return date


def split_on_date(df, split_date=VALIDATION_START_DATE):
    mask = df.loc[:, "t_dat"] < parse_date(split_date)
    return df.loc[mask], df.loc[~mask]


def sample(df, col):
    return df.loc[df[col].isin(df[col].sample())]
