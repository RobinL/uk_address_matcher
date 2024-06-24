import pandas as pd
from splink.linker import Linker
from splink.splink_dataframe import SplinkDataFrame


def distinguishability(
    linker: Linker,
    df_predict: SplinkDataFrame,
    unique_id_l: str = None,
    human_readable=False,
):

    if human_readable:
        select_cols = """
            unique_id_l,
            unique_id_r,
            source_dataset_l,
            source_dataset_r,
            original_address_concat_l,
            original_address_concat_r,
            postcode_l,
            postcode_r,
            match_probability,
            match_weight,
            disting as distinguishability,
            """
    else:
        select_cols = "*"

    if unique_id_l is not None:
        where_condition = f"where unique_id_l = '{unique_id_l}'"
    else:
        where_condition = ""
    sql = f"""
    WITH results_with_rn AS (
            SELECT
                unique_id_l,
                *,
                ROW_NUMBER() OVER (PARTITION BY unique_id_l ORDER BY match_weight DESC) AS rn
            FROM {df_predict.physical_name}
            QUALIFY rn <= 5
        ),
        second_place_match_weight AS (
            SELECT
                *,
                LEAD(match_weight) OVER (PARTITION BY unique_id_l ORDER BY match_weight DESC) AS previous_match_weight,
            FROM results_with_rn

        ),
        distinguishability as (
            SELECT
                *,
                match_weight - coalesce(previous_match_weight, null) as disting
                from second_place_match_weight
        )
        select
            {select_cols}

        from distinguishability
        {where_condition}
        order by unique_id_l

    """
    return linker.query_sql(sql)


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
