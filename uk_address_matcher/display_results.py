import pandas as pd
from splink.linker import Linker
from splink.splink_dataframe import SplinkDataFrame


def display_columns(df, suffix):
    cols = list(df.columns)
    cols_with_suffix = [
        col
        for col in cols
        if col.endswith(suffix)
        and not col.startswith("bf")
        and not col.startswith("gamma")
    ]
    df_narrow = df[cols_with_suffix]
    df_narrow.columns = [
        col[: -len(suffix)] if col.endswith(suffix) else col
        for col in df_narrow.columns
    ]
    return df_narrow


def display_l_r(df):
    a = display_columns(df, "_l")
    b = display_columns(df, "_r")
    return pd.concat([a, b])


def format_token_rel_freq(data):
    formatted_tokens = " ".join(
        [f"{item['tok']} {item['rel_freq']:.3e}" for item in data]
    )
    return formatted_tokens


def display_token_rel_freq(df, column):
    data = list(df[column].iloc[0])
    formatted_tokens = format_token_rel_freq(data)
    return formatted_tokens
