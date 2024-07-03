import importlib.resources as pkg_resources
import json
import re
import time
from typing import List

from duckdb import DuckDBPyConnection, DuckDBPyRelation
from splink.duckdb.linker import DuckDBLinker


def get_pretrained_linker(
    df_addresses_to_match: DuckDBPyRelation,
    df_addresses_to_search_within: DuckDBPyRelation,
    *,
    con: DuckDBPyConnection,
    precomputed_numeric_tf_table: DuckDBPyRelation = None,
    additional_columns_to_retain: List[str] = None,
    salting_multiplier: int = None,
    include_full_postcode_block=False,
):
    # Load the settings file
    with pkg_resources.path(
        "uk_address_matcher.data", "splink_model.json"
    ) as settings_path:
        settings_as_dict = json.load(open(settings_path))

    if additional_columns_to_retain is not None:
        settings_as_dict["additional_columns_to_retain"] = additional_columns_to_retain

    new_rules = []
    for rule in settings_as_dict["blocking_rules_to_generate_predictions"]:
        if not include_full_postcode_block:
            if (
                isinstance(rule, dict)
                and rule["blocking_rule"] == "l.postcode = r.postcode"
            ):
                continue
        if isinstance(rule, str):
            new_rules.append(
                {"blocking_rule": rule, "salting_partitions": salting_multiplier}
            )
        if isinstance(rule, dict):
            salt = 1 if salting_multiplier is None else salting_multiplier
            new_rules.append(
                {
                    "blocking_rule": rule["blocking_rule"],
                    "salting_partitions": rule["salting_partitions"] * salt,
                }
            )
        settings_as_dict["blocking_rules_to_generate_predictions"] = new_rules

    sql = f"""
    select * exclude (source_dataset),
    '0_' || source_dataset as source_dataset
    from df_addresses_to_match
    """
    df_addresses_to_match_fix = con.sql(sql)
    con.register("df_addresses_to_match_fix", df_addresses_to_match_fix)

    sql = f"""
    select * exclude (source_dataset),
    '1_' || source_dataset as source_dataset
    from df_addresses_to_search_within
    """
    df_addresses_to_search_within_fix = con.sql(sql)
    con.register("df_addresses_to_search_within_fix", df_addresses_to_search_within_fix)

    # Initialize the linker
    linker = DuckDBLinker(
        ["df_addresses_to_match_fix", "df_addresses_to_search_within_fix"],
        settings_dict=settings_as_dict,
        connection=con,
    )

    # Load the default term frequency table if none is provided
    if precomputed_numeric_tf_table is None:
        with pkg_resources.path(
            "uk_address_matcher.data", "numeric_token_frequencies.parquet"
        ) as default_tf_path:
            precomputed_numeric_tf_table = con.read_parquet(str(default_tf_path))

    if precomputed_numeric_tf_table is not None:
        for i in range(1, 4):
            df_sql = f"""
                select
                    numeric_token as numeric_token_{i},
                    tf_numeric_token as tf_numeric_token_{i}
                from precomputed_numeric_tf_table"""

            df = con.sql(df_sql).df()
            linker.register_term_frequency_lookup(
                df, f"numeric_token_{i}", overwrite=True
            )

    return linker


def _performance_predict(
    *,
    df_addresses_to_match: DuckDBPyRelation,
    df_addresses_to_search_within: DuckDBPyRelation,
    con: DuckDBPyConnection,
    precomputed_numeric_tf_table: DuckDBPyRelation = None,
    match_weight_threshold: None,
    additional_columns_to_retain: List[str] = None,
    output_all_cols: bool = True,
    include_full_postcode_block=True,
    full_block=False,
    print_timings=False,
):
    # Load the settings file
    with pkg_resources.path(
        "uk_address_matcher.data", "splink_model.json"
    ) as settings_path:
        settings_as_dict = json.load(open(settings_path))

    dfs_pd = [df_addresses_to_match.df(), df_addresses_to_search_within.df()]
    left_source_dataset = dfs_pd[0].iloc[0]["source_dataset"]
    right_source_dataset = dfs_pd[1].iloc[0]["source_dataset"]
    dfs_pd[0]["numeric_1_alt"] = dfs_pd[0]["numeric_1_alt"].astype(str)
    dfs_pd[1]["numeric_1_alt"] = dfs_pd[0]["numeric_1_alt"].astype(str)

    # Initialize the linker
    linker = DuckDBLinker(dfs_pd, settings_dict=settings_as_dict, connection=con)

    # Load the default term frequency table if none is provided
    if precomputed_numeric_tf_table is None:
        with pkg_resources.path(
            "uk_address_matcher.data", "numeric_token_frequencies.parquet"
        ) as default_tf_path:
            precomputed_numeric_tf_table = con.read_parquet(str(default_tf_path))

    if precomputed_numeric_tf_table is not None:
        for i in range(1, 4):
            df_sql = f"""
                select
                    numeric_token as numeric_token_{i},
                    tf_numeric_token as tf_numeric_token_{i}
                from precomputed_numeric_tf_table"""

            df = con.sql(df_sql).df()
            linker.register_term_frequency_lookup(
                df, f"numeric_token_{i}", overwrite=True
            )
    start_time = time.time()
    tf_table = linker._initialise_df_concat_with_tf()
    end_time = time.time()
    elapsed_time = end_time - start_time
    if print_timings:
        print(f"Initialise df_concat_with_tf took {elapsed_time:.2f} seconds")

    if include_full_postcode_block:
        pc_blocking_rule = """
        UNION ALL
        select
                "l"."source_dataset" AS "source_dataset_l", "r"."source_dataset" AS "source_dataset_r", "l"."unique_id" AS "unique_id_l", "r"."unique_id" AS "unique_id_r"
                , '24' as match_key

                from __splink__df_concat_with_tf_left as l
                inner join __splink__df_concat_with_tf_right as r
                on
                (l.postcode = r.postcode)
                where 1=1
                AND NOT (coalesce((l.numeric_token_1 = r.numeric_token_1 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1) and list_extract(l.unusual_tokens_arr, 2) = list_extract(r.unusual_tokens_arr, 2) and split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_2 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_2 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 2) = split_part(r.postcode, ' ', 2)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_1 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 2) and list_extract(l.unusual_tokens_arr, 2) = list_extract(r.unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_1 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 2) and split_part(l.postcode, ' ', 2) = split_part(r.postcode, ' ', 2)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_1 and l.postcode = r.postcode),false) OR coalesce((l.numeric_token_1 = r.numeric_token_2 and l.postcode = r.postcode),false) OR coalesce((list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 2) and l.postcode = r.postcode),false) OR coalesce((list_extract(l.very_unusual_tokens_arr, 1) = list_extract(r.very_unusual_tokens_arr, 1) and l.numeric_token_1 = r.numeric_token_1),false) OR coalesce((list_extract(l.very_unusual_tokens_arr, 1) = list_extract(r.very_unusual_tokens_arr, 2) and l.numeric_token_1 = r.numeric_token_1),false) OR coalesce((l.numeric_token_2 = r.numeric_token_2 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_1 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 2) = split_part(r.postcode, ' ', 2)),false) OR coalesce((l.numeric_token_2 = r.numeric_token_2 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 2) = split_part(r.postcode, ' ', 2)),false) OR coalesce((l.numeric_token_2 = r.numeric_token_2 and l.postcode = r.postcode),false) OR coalesce((l.numeric_token_1 = r.numeric_1_alt and l.postcode = r.postcode),false) OR coalesce((l.numeric_1_alt = r.numeric_token_1 and l.postcode = r.postcode),false) OR coalesce((l.numeric_token_1 = r.numeric_1_alt and l.numeric_token_2 = r.numeric_token_2 and split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1)),false) OR coalesce((l.numeric_1_alt = r.numeric_token_1 and l.numeric_token_2 = r.numeric_token_2 and split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1)),false) OR coalesce((l.numeric_token_1 = r.numeric_1_alt and l.numeric_token_2 = r.numeric_token_2 and split_part(l.postcode, ' ', 2) = split_part(r.postcode, ' ', 2)),false) OR coalesce((l.numeric_1_alt = r.numeric_token_1 and l.numeric_token_2 = r.numeric_token_2 and split_part(l.postcode, ' ', 2) = split_part(r.postcode, ' ', 2)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_1 and l.numeric_token_2 = r.numeric_token_2 and split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_1 and l.numeric_token_2 = r.numeric_token_2 and split_part(l.postcode, ' ', 2) = split_part(r.postcode, ' ', 2)),false) OR coalesce((list_extract(l.extremely_unusual_tokens_arr, 1) = list_extract(r.extremely_unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1)),false) OR coalesce((list_extract(l.extremely_unusual_tokens_arr, 1) = list_extract(r.extremely_unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 2) = split_part(r.postcode, ' ', 2)),false))
        """
    else:
        pc_blocking_rule = ""

    sql = f"""
    create or replace table blocked_pairs as (
    WITH __splink__df_concat_with_tf as (select * from {tf_table.physical_name}),
    __splink__df_concat_with_tf_left as (
            select * from __splink__df_concat_with_tf
            where source_dataset = '{left_source_dataset}'
            ),
    __splink__df_concat_with_tf_right as (
            select * from __splink__df_concat_with_tf
            where source_dataset = '{right_source_dataset}'
            ),
    __splink__df_blocked as (
            -- start_blocking
            select
            "l"."source_dataset" AS "source_dataset_l", "r"."source_dataset" AS "source_dataset_r", "l"."unique_id" AS "unique_id_l", "r"."unique_id" AS "unique_id_r"
            , '0' as match_key

            from __splink__df_concat_with_tf_left as l
            inner join __splink__df_concat_with_tf_right as r
            on
            (l.numeric_token_1 = r.numeric_token_1 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1) and list_extract(l.unusual_tokens_arr, 2) = list_extract(r.unusual_tokens_arr, 2) and split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1))
             where 1=1

             UNION ALL
            select
            "l"."source_dataset" AS "source_dataset_l", "r"."source_dataset" AS "source_dataset_r", "l"."unique_id" AS "unique_id_l", "r"."unique_id" AS "unique_id_r"
            , '1' as match_key

            from __splink__df_concat_with_tf_left as l
            inner join __splink__df_concat_with_tf_right as r
            on
            (l.numeric_token_1 = r.numeric_token_2 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1))
             where 1=1
            AND NOT (coalesce((l.numeric_token_1 = r.numeric_token_1 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1) and list_extract(l.unusual_tokens_arr, 2) = list_extract(r.unusual_tokens_arr, 2) and split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1)),false))
             UNION ALL
            select
            "l"."source_dataset" AS "source_dataset_l", "r"."source_dataset" AS "source_dataset_r", "l"."unique_id" AS "unique_id_l", "r"."unique_id" AS "unique_id_r"
            , '2' as match_key

            from __splink__df_concat_with_tf_left as l
            inner join __splink__df_concat_with_tf_right as r
            on
            (l.numeric_token_1 = r.numeric_token_2 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 2) = split_part(r.postcode, ' ', 2))
             where 1=1
            AND NOT (coalesce((l.numeric_token_1 = r.numeric_token_1 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1) and list_extract(l.unusual_tokens_arr, 2) = list_extract(r.unusual_tokens_arr, 2) and split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_2 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1)),false))
             UNION ALL
            select
            "l"."source_dataset" AS "source_dataset_l", "r"."source_dataset" AS "source_dataset_r", "l"."unique_id" AS "unique_id_l", "r"."unique_id" AS "unique_id_r"
            , '3' as match_key

            from __splink__df_concat_with_tf_left as l
            inner join __splink__df_concat_with_tf_right as r
            on
            (l.numeric_token_1 = r.numeric_token_1 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 2) and list_extract(l.unusual_tokens_arr, 2) = list_extract(r.unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1))
             where 1=1
            AND NOT (coalesce((l.numeric_token_1 = r.numeric_token_1 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1) and list_extract(l.unusual_tokens_arr, 2) = list_extract(r.unusual_tokens_arr, 2) and split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_2 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_2 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 2) = split_part(r.postcode, ' ', 2)),false))
             UNION ALL
            select
            "l"."source_dataset" AS "source_dataset_l", "r"."source_dataset" AS "source_dataset_r", "l"."unique_id" AS "unique_id_l", "r"."unique_id" AS "unique_id_r"
            , '4' as match_key

            from __splink__df_concat_with_tf_left as l
            inner join __splink__df_concat_with_tf_right as r
            on
            (l.numeric_token_1 = r.numeric_token_1 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 2) and split_part(l.postcode, ' ', 2) = split_part(r.postcode, ' ', 2))
             where 1=1
            AND NOT (coalesce((l.numeric_token_1 = r.numeric_token_1 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1) and list_extract(l.unusual_tokens_arr, 2) = list_extract(r.unusual_tokens_arr, 2) and split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_2 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_2 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 2) = split_part(r.postcode, ' ', 2)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_1 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 2) and list_extract(l.unusual_tokens_arr, 2) = list_extract(r.unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1)),false))
             UNION ALL
            select
            "l"."source_dataset" AS "source_dataset_l", "r"."source_dataset" AS "source_dataset_r", "l"."unique_id" AS "unique_id_l", "r"."unique_id" AS "unique_id_r"
            , '5' as match_key

            from __splink__df_concat_with_tf_left as l
            inner join __splink__df_concat_with_tf_right as r
            on
            (l.numeric_token_1 = r.numeric_token_1 and l.postcode = r.postcode)
             where 1=1
            AND NOT (coalesce((l.numeric_token_1 = r.numeric_token_1 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1) and list_extract(l.unusual_tokens_arr, 2) = list_extract(r.unusual_tokens_arr, 2) and split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_2 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_2 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 2) = split_part(r.postcode, ' ', 2)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_1 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 2) and list_extract(l.unusual_tokens_arr, 2) = list_extract(r.unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_1 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 2) and split_part(l.postcode, ' ', 2) = split_part(r.postcode, ' ', 2)),false))
             UNION ALL
            select
            "l"."source_dataset" AS "source_dataset_l", "r"."source_dataset" AS "source_dataset_r", "l"."unique_id" AS "unique_id_l", "r"."unique_id" AS "unique_id_r"
            , '6' as match_key

            from __splink__df_concat_with_tf_left as l
            inner join __splink__df_concat_with_tf_right as r
            on
            (l.numeric_token_1 = r.numeric_token_2 and l.postcode = r.postcode)
             where 1=1
            AND NOT (coalesce((l.numeric_token_1 = r.numeric_token_1 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1) and list_extract(l.unusual_tokens_arr, 2) = list_extract(r.unusual_tokens_arr, 2) and split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_2 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_2 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 2) = split_part(r.postcode, ' ', 2)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_1 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 2) and list_extract(l.unusual_tokens_arr, 2) = list_extract(r.unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_1 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 2) and split_part(l.postcode, ' ', 2) = split_part(r.postcode, ' ', 2)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_1 and l.postcode = r.postcode),false))
             UNION ALL
            select
            "l"."source_dataset" AS "source_dataset_l", "r"."source_dataset" AS "source_dataset_r", "l"."unique_id" AS "unique_id_l", "r"."unique_id" AS "unique_id_r"
            , '7' as match_key

            from __splink__df_concat_with_tf_left as l
            inner join __splink__df_concat_with_tf_right as r
            on
            (list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 2) and l.postcode = r.postcode) and false --HACK BECAUSE CREATES LOTS OF PAIRS BUT FEW MATCHES
             where 1=1
            AND NOT (coalesce((l.numeric_token_1 = r.numeric_token_1 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1) and list_extract(l.unusual_tokens_arr, 2) = list_extract(r.unusual_tokens_arr, 2) and split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_2 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_2 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 2) = split_part(r.postcode, ' ', 2)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_1 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 2) and list_extract(l.unusual_tokens_arr, 2) = list_extract(r.unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_1 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 2) and split_part(l.postcode, ' ', 2) = split_part(r.postcode, ' ', 2)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_1 and l.postcode = r.postcode),false) OR coalesce((l.numeric_token_1 = r.numeric_token_2 and l.postcode = r.postcode),false))
             UNION ALL
            select
            "l"."source_dataset" AS "source_dataset_l", "r"."source_dataset" AS "source_dataset_r", "l"."unique_id" AS "unique_id_l", "r"."unique_id" AS "unique_id_r"
            , '8' as match_key

            from __splink__df_concat_with_tf_left as l
            inner join __splink__df_concat_with_tf_right as r
            on
            (list_extract(l.very_unusual_tokens_arr, 1) = list_extract(r.very_unusual_tokens_arr, 1) and l.numeric_token_1 = r.numeric_token_1) and false --HACK BECAUSE CREATES LOTS OF PAIRS BUT FEW MATCHES
             where 1=1
            AND NOT (coalesce((l.numeric_token_1 = r.numeric_token_1 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1) and list_extract(l.unusual_tokens_arr, 2) = list_extract(r.unusual_tokens_arr, 2) and split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_2 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_2 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 2) = split_part(r.postcode, ' ', 2)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_1 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 2) and list_extract(l.unusual_tokens_arr, 2) = list_extract(r.unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_1 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 2) and split_part(l.postcode, ' ', 2) = split_part(r.postcode, ' ', 2)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_1 and l.postcode = r.postcode),false) OR coalesce((l.numeric_token_1 = r.numeric_token_2 and l.postcode = r.postcode),false) OR coalesce((list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 2) and l.postcode = r.postcode),false))
             UNION ALL
            select
            "l"."source_dataset" AS "source_dataset_l", "r"."source_dataset" AS "source_dataset_r", "l"."unique_id" AS "unique_id_l", "r"."unique_id" AS "unique_id_r"
            , '9' as match_key

            from __splink__df_concat_with_tf_left as l
            inner join __splink__df_concat_with_tf_right as r
            on
            (list_extract(l.very_unusual_tokens_arr, 1) = list_extract(r.very_unusual_tokens_arr, 2) and l.numeric_token_1 = r.numeric_token_1)
             where 1=1
            AND NOT (coalesce((l.numeric_token_1 = r.numeric_token_1 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1) and list_extract(l.unusual_tokens_arr, 2) = list_extract(r.unusual_tokens_arr, 2) and split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_2 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_2 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 2) = split_part(r.postcode, ' ', 2)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_1 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 2) and list_extract(l.unusual_tokens_arr, 2) = list_extract(r.unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_1 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 2) and split_part(l.postcode, ' ', 2) = split_part(r.postcode, ' ', 2)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_1 and l.postcode = r.postcode),false) OR coalesce((l.numeric_token_1 = r.numeric_token_2 and l.postcode = r.postcode),false) OR coalesce((list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 2) and l.postcode = r.postcode),false) OR coalesce((list_extract(l.very_unusual_tokens_arr, 1) = list_extract(r.very_unusual_tokens_arr, 1) and l.numeric_token_1 = r.numeric_token_1),false))
             UNION ALL
            select
            "l"."source_dataset" AS "source_dataset_l", "r"."source_dataset" AS "source_dataset_r", "l"."unique_id" AS "unique_id_l", "r"."unique_id" AS "unique_id_r"
            , '10' as match_key

            from __splink__df_concat_with_tf_left as l
            inner join __splink__df_concat_with_tf_right as r
            on
            (l.numeric_token_2 = r.numeric_token_2 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1))
             where 1=1
            AND NOT (coalesce((l.numeric_token_1 = r.numeric_token_1 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1) and list_extract(l.unusual_tokens_arr, 2) = list_extract(r.unusual_tokens_arr, 2) and split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_2 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_2 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 2) = split_part(r.postcode, ' ', 2)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_1 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 2) and list_extract(l.unusual_tokens_arr, 2) = list_extract(r.unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_1 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 2) and split_part(l.postcode, ' ', 2) = split_part(r.postcode, ' ', 2)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_1 and l.postcode = r.postcode),false) OR coalesce((l.numeric_token_1 = r.numeric_token_2 and l.postcode = r.postcode),false) OR coalesce((list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 2) and l.postcode = r.postcode),false) OR coalesce((list_extract(l.very_unusual_tokens_arr, 1) = list_extract(r.very_unusual_tokens_arr, 1) and l.numeric_token_1 = r.numeric_token_1),false) OR coalesce((list_extract(l.very_unusual_tokens_arr, 1) = list_extract(r.very_unusual_tokens_arr, 2) and l.numeric_token_1 = r.numeric_token_1),false))
             UNION ALL
            select
            "l"."source_dataset" AS "source_dataset_l", "r"."source_dataset" AS "source_dataset_r", "l"."unique_id" AS "unique_id_l", "r"."unique_id" AS "unique_id_r"
            , '11' as match_key

            from __splink__df_concat_with_tf_left as l
            inner join __splink__df_concat_with_tf_right as r
            on
            (l.numeric_token_1 = r.numeric_token_1 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 2) = split_part(r.postcode, ' ', 2))
             where 1=1
            AND NOT (coalesce((l.numeric_token_1 = r.numeric_token_1 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1) and list_extract(l.unusual_tokens_arr, 2) = list_extract(r.unusual_tokens_arr, 2) and split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_2 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_2 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 2) = split_part(r.postcode, ' ', 2)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_1 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 2) and list_extract(l.unusual_tokens_arr, 2) = list_extract(r.unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_1 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 2) and split_part(l.postcode, ' ', 2) = split_part(r.postcode, ' ', 2)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_1 and l.postcode = r.postcode),false) OR coalesce((l.numeric_token_1 = r.numeric_token_2 and l.postcode = r.postcode),false) OR coalesce((list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 2) and l.postcode = r.postcode),false) OR coalesce((list_extract(l.very_unusual_tokens_arr, 1) = list_extract(r.very_unusual_tokens_arr, 1) and l.numeric_token_1 = r.numeric_token_1),false) OR coalesce((list_extract(l.very_unusual_tokens_arr, 1) = list_extract(r.very_unusual_tokens_arr, 2) and l.numeric_token_1 = r.numeric_token_1),false) OR coalesce((l.numeric_token_2 = r.numeric_token_2 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1)),false))
             UNION ALL
            select
            "l"."source_dataset" AS "source_dataset_l", "r"."source_dataset" AS "source_dataset_r", "l"."unique_id" AS "unique_id_l", "r"."unique_id" AS "unique_id_r"
            , '12' as match_key

            from __splink__df_concat_with_tf_left as l
            inner join __splink__df_concat_with_tf_right as r
            on
            (l.numeric_token_2 = r.numeric_token_2 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 2) = split_part(r.postcode, ' ', 2))
             where 1=1
            AND NOT (coalesce((l.numeric_token_1 = r.numeric_token_1 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1) and list_extract(l.unusual_tokens_arr, 2) = list_extract(r.unusual_tokens_arr, 2) and split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_2 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_2 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 2) = split_part(r.postcode, ' ', 2)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_1 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 2) and list_extract(l.unusual_tokens_arr, 2) = list_extract(r.unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_1 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 2) and split_part(l.postcode, ' ', 2) = split_part(r.postcode, ' ', 2)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_1 and l.postcode = r.postcode),false) OR coalesce((l.numeric_token_1 = r.numeric_token_2 and l.postcode = r.postcode),false) OR coalesce((list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 2) and l.postcode = r.postcode),false) OR coalesce((list_extract(l.very_unusual_tokens_arr, 1) = list_extract(r.very_unusual_tokens_arr, 1) and l.numeric_token_1 = r.numeric_token_1),false) OR coalesce((list_extract(l.very_unusual_tokens_arr, 1) = list_extract(r.very_unusual_tokens_arr, 2) and l.numeric_token_1 = r.numeric_token_1),false) OR coalesce((l.numeric_token_2 = r.numeric_token_2 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_1 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 2) = split_part(r.postcode, ' ', 2)),false))
             UNION ALL
            select
            "l"."source_dataset" AS "source_dataset_l", "r"."source_dataset" AS "source_dataset_r", "l"."unique_id" AS "unique_id_l", "r"."unique_id" AS "unique_id_r"
            , '13' as match_key

            from __splink__df_concat_with_tf_left as l
            inner join __splink__df_concat_with_tf_right as r
            on
            (l.numeric_token_2 = r.numeric_token_2 and l.postcode = r.postcode)
             where 1=1
            AND NOT (coalesce((l.numeric_token_1 = r.numeric_token_1 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1) and list_extract(l.unusual_tokens_arr, 2) = list_extract(r.unusual_tokens_arr, 2) and split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_2 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_2 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 2) = split_part(r.postcode, ' ', 2)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_1 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 2) and list_extract(l.unusual_tokens_arr, 2) = list_extract(r.unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_1 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 2) and split_part(l.postcode, ' ', 2) = split_part(r.postcode, ' ', 2)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_1 and l.postcode = r.postcode),false) OR coalesce((l.numeric_token_1 = r.numeric_token_2 and l.postcode = r.postcode),false) OR coalesce((list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 2) and l.postcode = r.postcode),false) OR coalesce((list_extract(l.very_unusual_tokens_arr, 1) = list_extract(r.very_unusual_tokens_arr, 1) and l.numeric_token_1 = r.numeric_token_1),false) OR coalesce((list_extract(l.very_unusual_tokens_arr, 1) = list_extract(r.very_unusual_tokens_arr, 2) and l.numeric_token_1 = r.numeric_token_1),false) OR coalesce((l.numeric_token_2 = r.numeric_token_2 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_1 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 2) = split_part(r.postcode, ' ', 2)),false) OR coalesce((l.numeric_token_2 = r.numeric_token_2 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 2) = split_part(r.postcode, ' ', 2)),false))
             UNION ALL
            select
            "l"."source_dataset" AS "source_dataset_l", "r"."source_dataset" AS "source_dataset_r", "l"."unique_id" AS "unique_id_l", "r"."unique_id" AS "unique_id_r"
            , '14' as match_key

            from __splink__df_concat_with_tf_left as l
            inner join __splink__df_concat_with_tf_right as r
            on
            (l.numeric_token_1 = r.numeric_1_alt and l.postcode = r.postcode)
             where 1=1
            AND NOT (coalesce((l.numeric_token_1 = r.numeric_token_1 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1) and list_extract(l.unusual_tokens_arr, 2) = list_extract(r.unusual_tokens_arr, 2) and split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_2 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_2 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 2) = split_part(r.postcode, ' ', 2)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_1 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 2) and list_extract(l.unusual_tokens_arr, 2) = list_extract(r.unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_1 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 2) and split_part(l.postcode, ' ', 2) = split_part(r.postcode, ' ', 2)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_1 and l.postcode = r.postcode),false) OR coalesce((l.numeric_token_1 = r.numeric_token_2 and l.postcode = r.postcode),false) OR coalesce((list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 2) and l.postcode = r.postcode),false) OR coalesce((list_extract(l.very_unusual_tokens_arr, 1) = list_extract(r.very_unusual_tokens_arr, 1) and l.numeric_token_1 = r.numeric_token_1),false) OR coalesce((list_extract(l.very_unusual_tokens_arr, 1) = list_extract(r.very_unusual_tokens_arr, 2) and l.numeric_token_1 = r.numeric_token_1),false) OR coalesce((l.numeric_token_2 = r.numeric_token_2 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_1 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 2) = split_part(r.postcode, ' ', 2)),false) OR coalesce((l.numeric_token_2 = r.numeric_token_2 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 2) = split_part(r.postcode, ' ', 2)),false) OR coalesce((l.numeric_token_2 = r.numeric_token_2 and l.postcode = r.postcode),false))
             UNION ALL
            select
            "l"."source_dataset" AS "source_dataset_l", "r"."source_dataset" AS "source_dataset_r", "l"."unique_id" AS "unique_id_l", "r"."unique_id" AS "unique_id_r"
            , '15' as match_key

            from __splink__df_concat_with_tf_left as l
            inner join __splink__df_concat_with_tf_right as r
            on
            (l.numeric_1_alt = r.numeric_token_1 and l.postcode = r.postcode)
             where 1=1
            AND NOT (coalesce((l.numeric_token_1 = r.numeric_token_1 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1) and list_extract(l.unusual_tokens_arr, 2) = list_extract(r.unusual_tokens_arr, 2) and split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_2 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_2 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 2) = split_part(r.postcode, ' ', 2)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_1 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 2) and list_extract(l.unusual_tokens_arr, 2) = list_extract(r.unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_1 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 2) and split_part(l.postcode, ' ', 2) = split_part(r.postcode, ' ', 2)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_1 and l.postcode = r.postcode),false) OR coalesce((l.numeric_token_1 = r.numeric_token_2 and l.postcode = r.postcode),false) OR coalesce((list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 2) and l.postcode = r.postcode),false) OR coalesce((list_extract(l.very_unusual_tokens_arr, 1) = list_extract(r.very_unusual_tokens_arr, 1) and l.numeric_token_1 = r.numeric_token_1),false) OR coalesce((list_extract(l.very_unusual_tokens_arr, 1) = list_extract(r.very_unusual_tokens_arr, 2) and l.numeric_token_1 = r.numeric_token_1),false) OR coalesce((l.numeric_token_2 = r.numeric_token_2 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_1 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 2) = split_part(r.postcode, ' ', 2)),false) OR coalesce((l.numeric_token_2 = r.numeric_token_2 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 2) = split_part(r.postcode, ' ', 2)),false) OR coalesce((l.numeric_token_2 = r.numeric_token_2 and l.postcode = r.postcode),false) OR coalesce((l.numeric_token_1 = r.numeric_1_alt and l.postcode = r.postcode),false))
             UNION ALL
            select
            "l"."source_dataset" AS "source_dataset_l", "r"."source_dataset" AS "source_dataset_r", "l"."unique_id" AS "unique_id_l", "r"."unique_id" AS "unique_id_r"
            , '16' as match_key

            from __splink__df_concat_with_tf_left as l
            inner join __splink__df_concat_with_tf_right as r
            on
            (l.numeric_token_1 = r.numeric_1_alt and l.numeric_token_2 = r.numeric_token_2 and split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1))
             where 1=1
            AND NOT (coalesce((l.numeric_token_1 = r.numeric_token_1 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1) and list_extract(l.unusual_tokens_arr, 2) = list_extract(r.unusual_tokens_arr, 2) and split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_2 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_2 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 2) = split_part(r.postcode, ' ', 2)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_1 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 2) and list_extract(l.unusual_tokens_arr, 2) = list_extract(r.unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_1 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 2) and split_part(l.postcode, ' ', 2) = split_part(r.postcode, ' ', 2)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_1 and l.postcode = r.postcode),false) OR coalesce((l.numeric_token_1 = r.numeric_token_2 and l.postcode = r.postcode),false) OR coalesce((list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 2) and l.postcode = r.postcode),false) OR coalesce((list_extract(l.very_unusual_tokens_arr, 1) = list_extract(r.very_unusual_tokens_arr, 1) and l.numeric_token_1 = r.numeric_token_1),false) OR coalesce((list_extract(l.very_unusual_tokens_arr, 1) = list_extract(r.very_unusual_tokens_arr, 2) and l.numeric_token_1 = r.numeric_token_1),false) OR coalesce((l.numeric_token_2 = r.numeric_token_2 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_1 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 2) = split_part(r.postcode, ' ', 2)),false) OR coalesce((l.numeric_token_2 = r.numeric_token_2 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 2) = split_part(r.postcode, ' ', 2)),false) OR coalesce((l.numeric_token_2 = r.numeric_token_2 and l.postcode = r.postcode),false) OR coalesce((l.numeric_token_1 = r.numeric_1_alt and l.postcode = r.postcode),false) OR coalesce((l.numeric_1_alt = r.numeric_token_1 and l.postcode = r.postcode),false))
             UNION ALL
            select
            "l"."source_dataset" AS "source_dataset_l", "r"."source_dataset" AS "source_dataset_r", "l"."unique_id" AS "unique_id_l", "r"."unique_id" AS "unique_id_r"
            , '17' as match_key

            from __splink__df_concat_with_tf_left as l
            inner join __splink__df_concat_with_tf_right as r
            on
            (l.numeric_1_alt = r.numeric_token_1 and l.numeric_token_2 = r.numeric_token_2 and split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1))
             where 1=1
            AND NOT (coalesce((l.numeric_token_1 = r.numeric_token_1 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1) and list_extract(l.unusual_tokens_arr, 2) = list_extract(r.unusual_tokens_arr, 2) and split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_2 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_2 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 2) = split_part(r.postcode, ' ', 2)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_1 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 2) and list_extract(l.unusual_tokens_arr, 2) = list_extract(r.unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_1 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 2) and split_part(l.postcode, ' ', 2) = split_part(r.postcode, ' ', 2)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_1 and l.postcode = r.postcode),false) OR coalesce((l.numeric_token_1 = r.numeric_token_2 and l.postcode = r.postcode),false) OR coalesce((list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 2) and l.postcode = r.postcode),false) OR coalesce((list_extract(l.very_unusual_tokens_arr, 1) = list_extract(r.very_unusual_tokens_arr, 1) and l.numeric_token_1 = r.numeric_token_1),false) OR coalesce((list_extract(l.very_unusual_tokens_arr, 1) = list_extract(r.very_unusual_tokens_arr, 2) and l.numeric_token_1 = r.numeric_token_1),false) OR coalesce((l.numeric_token_2 = r.numeric_token_2 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_1 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 2) = split_part(r.postcode, ' ', 2)),false) OR coalesce((l.numeric_token_2 = r.numeric_token_2 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 2) = split_part(r.postcode, ' ', 2)),false) OR coalesce((l.numeric_token_2 = r.numeric_token_2 and l.postcode = r.postcode),false) OR coalesce((l.numeric_token_1 = r.numeric_1_alt and l.postcode = r.postcode),false) OR coalesce((l.numeric_1_alt = r.numeric_token_1 and l.postcode = r.postcode),false) OR coalesce((l.numeric_token_1 = r.numeric_1_alt and l.numeric_token_2 = r.numeric_token_2 and split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1)),false))
             UNION ALL
            select
            "l"."source_dataset" AS "source_dataset_l", "r"."source_dataset" AS "source_dataset_r", "l"."unique_id" AS "unique_id_l", "r"."unique_id" AS "unique_id_r"
            , '18' as match_key

            from __splink__df_concat_with_tf_left as l
            inner join __splink__df_concat_with_tf_right as r
            on
            (l.numeric_token_1 = r.numeric_1_alt and l.numeric_token_2 = r.numeric_token_2 and split_part(l.postcode, ' ', 2) = split_part(r.postcode, ' ', 2))
             where 1=1
            AND NOT (coalesce((l.numeric_token_1 = r.numeric_token_1 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1) and list_extract(l.unusual_tokens_arr, 2) = list_extract(r.unusual_tokens_arr, 2) and split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_2 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_2 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 2) = split_part(r.postcode, ' ', 2)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_1 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 2) and list_extract(l.unusual_tokens_arr, 2) = list_extract(r.unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_1 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 2) and split_part(l.postcode, ' ', 2) = split_part(r.postcode, ' ', 2)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_1 and l.postcode = r.postcode),false) OR coalesce((l.numeric_token_1 = r.numeric_token_2 and l.postcode = r.postcode),false) OR coalesce((list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 2) and l.postcode = r.postcode),false) OR coalesce((list_extract(l.very_unusual_tokens_arr, 1) = list_extract(r.very_unusual_tokens_arr, 1) and l.numeric_token_1 = r.numeric_token_1),false) OR coalesce((list_extract(l.very_unusual_tokens_arr, 1) = list_extract(r.very_unusual_tokens_arr, 2) and l.numeric_token_1 = r.numeric_token_1),false) OR coalesce((l.numeric_token_2 = r.numeric_token_2 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_1 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 2) = split_part(r.postcode, ' ', 2)),false) OR coalesce((l.numeric_token_2 = r.numeric_token_2 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 2) = split_part(r.postcode, ' ', 2)),false) OR coalesce((l.numeric_token_2 = r.numeric_token_2 and l.postcode = r.postcode),false) OR coalesce((l.numeric_token_1 = r.numeric_1_alt and l.postcode = r.postcode),false) OR coalesce((l.numeric_1_alt = r.numeric_token_1 and l.postcode = r.postcode),false) OR coalesce((l.numeric_token_1 = r.numeric_1_alt and l.numeric_token_2 = r.numeric_token_2 and split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1)),false) OR coalesce((l.numeric_1_alt = r.numeric_token_1 and l.numeric_token_2 = r.numeric_token_2 and split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1)),false))
             UNION ALL
            select
            "l"."source_dataset" AS "source_dataset_l", "r"."source_dataset" AS "source_dataset_r", "l"."unique_id" AS "unique_id_l", "r"."unique_id" AS "unique_id_r"
            , '19' as match_key

            from __splink__df_concat_with_tf_left as l
            inner join __splink__df_concat_with_tf_right as r
            on
            (l.numeric_1_alt = r.numeric_token_1 and l.numeric_token_2 = r.numeric_token_2 and split_part(l.postcode, ' ', 2) = split_part(r.postcode, ' ', 2))
             where 1=1
            AND NOT (coalesce((l.numeric_token_1 = r.numeric_token_1 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1) and list_extract(l.unusual_tokens_arr, 2) = list_extract(r.unusual_tokens_arr, 2) and split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_2 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_2 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 2) = split_part(r.postcode, ' ', 2)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_1 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 2) and list_extract(l.unusual_tokens_arr, 2) = list_extract(r.unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_1 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 2) and split_part(l.postcode, ' ', 2) = split_part(r.postcode, ' ', 2)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_1 and l.postcode = r.postcode),false) OR coalesce((l.numeric_token_1 = r.numeric_token_2 and l.postcode = r.postcode),false) OR coalesce((list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 2) and l.postcode = r.postcode),false) OR coalesce((list_extract(l.very_unusual_tokens_arr, 1) = list_extract(r.very_unusual_tokens_arr, 1) and l.numeric_token_1 = r.numeric_token_1),false) OR coalesce((list_extract(l.very_unusual_tokens_arr, 1) = list_extract(r.very_unusual_tokens_arr, 2) and l.numeric_token_1 = r.numeric_token_1),false) OR coalesce((l.numeric_token_2 = r.numeric_token_2 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_1 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 2) = split_part(r.postcode, ' ', 2)),false) OR coalesce((l.numeric_token_2 = r.numeric_token_2 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 2) = split_part(r.postcode, ' ', 2)),false) OR coalesce((l.numeric_token_2 = r.numeric_token_2 and l.postcode = r.postcode),false) OR coalesce((l.numeric_token_1 = r.numeric_1_alt and l.postcode = r.postcode),false) OR coalesce((l.numeric_1_alt = r.numeric_token_1 and l.postcode = r.postcode),false) OR coalesce((l.numeric_token_1 = r.numeric_1_alt and l.numeric_token_2 = r.numeric_token_2 and split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1)),false) OR coalesce((l.numeric_1_alt = r.numeric_token_1 and l.numeric_token_2 = r.numeric_token_2 and split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1)),false) OR coalesce((l.numeric_token_1 = r.numeric_1_alt and l.numeric_token_2 = r.numeric_token_2 and split_part(l.postcode, ' ', 2) = split_part(r.postcode, ' ', 2)),false))
             UNION ALL
            select
            "l"."source_dataset" AS "source_dataset_l", "r"."source_dataset" AS "source_dataset_r", "l"."unique_id" AS "unique_id_l", "r"."unique_id" AS "unique_id_r"
            , '20' as match_key

            from __splink__df_concat_with_tf_left as l
            inner join __splink__df_concat_with_tf_right as r
            on
            (l.numeric_token_1 = r.numeric_token_1 and l.numeric_token_2 = r.numeric_token_2 and split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1))
             where 1=1
            AND NOT (coalesce((l.numeric_token_1 = r.numeric_token_1 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1) and list_extract(l.unusual_tokens_arr, 2) = list_extract(r.unusual_tokens_arr, 2) and split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_2 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_2 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 2) = split_part(r.postcode, ' ', 2)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_1 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 2) and list_extract(l.unusual_tokens_arr, 2) = list_extract(r.unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_1 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 2) and split_part(l.postcode, ' ', 2) = split_part(r.postcode, ' ', 2)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_1 and l.postcode = r.postcode),false) OR coalesce((l.numeric_token_1 = r.numeric_token_2 and l.postcode = r.postcode),false) OR coalesce((list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 2) and l.postcode = r.postcode),false) OR coalesce((list_extract(l.very_unusual_tokens_arr, 1) = list_extract(r.very_unusual_tokens_arr, 1) and l.numeric_token_1 = r.numeric_token_1),false) OR coalesce((list_extract(l.very_unusual_tokens_arr, 1) = list_extract(r.very_unusual_tokens_arr, 2) and l.numeric_token_1 = r.numeric_token_1),false) OR coalesce((l.numeric_token_2 = r.numeric_token_2 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_1 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 2) = split_part(r.postcode, ' ', 2)),false) OR coalesce((l.numeric_token_2 = r.numeric_token_2 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 2) = split_part(r.postcode, ' ', 2)),false) OR coalesce((l.numeric_token_2 = r.numeric_token_2 and l.postcode = r.postcode),false) OR coalesce((l.numeric_token_1 = r.numeric_1_alt and l.postcode = r.postcode),false) OR coalesce((l.numeric_1_alt = r.numeric_token_1 and l.postcode = r.postcode),false) OR coalesce((l.numeric_token_1 = r.numeric_1_alt and l.numeric_token_2 = r.numeric_token_2 and split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1)),false) OR coalesce((l.numeric_1_alt = r.numeric_token_1 and l.numeric_token_2 = r.numeric_token_2 and split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1)),false) OR coalesce((l.numeric_token_1 = r.numeric_1_alt and l.numeric_token_2 = r.numeric_token_2 and split_part(l.postcode, ' ', 2) = split_part(r.postcode, ' ', 2)),false) OR coalesce((l.numeric_1_alt = r.numeric_token_1 and l.numeric_token_2 = r.numeric_token_2 and split_part(l.postcode, ' ', 2) = split_part(r.postcode, ' ', 2)),false))
             UNION ALL
            select
            "l"."source_dataset" AS "source_dataset_l", "r"."source_dataset" AS "source_dataset_r", "l"."unique_id" AS "unique_id_l", "r"."unique_id" AS "unique_id_r"
            , '21' as match_key

            from __splink__df_concat_with_tf_left as l
            inner join __splink__df_concat_with_tf_right as r
            on
            (l.numeric_token_1 = r.numeric_token_1 and l.numeric_token_2 = r.numeric_token_2 and split_part(l.postcode, ' ', 2) = split_part(r.postcode, ' ', 2))
             where 1=1
            AND NOT (coalesce((l.numeric_token_1 = r.numeric_token_1 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1) and list_extract(l.unusual_tokens_arr, 2) = list_extract(r.unusual_tokens_arr, 2) and split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_2 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_2 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 2) = split_part(r.postcode, ' ', 2)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_1 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 2) and list_extract(l.unusual_tokens_arr, 2) = list_extract(r.unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_1 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 2) and split_part(l.postcode, ' ', 2) = split_part(r.postcode, ' ', 2)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_1 and l.postcode = r.postcode),false) OR coalesce((l.numeric_token_1 = r.numeric_token_2 and l.postcode = r.postcode),false) OR coalesce((list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 2) and l.postcode = r.postcode),false) OR coalesce((list_extract(l.very_unusual_tokens_arr, 1) = list_extract(r.very_unusual_tokens_arr, 1) and l.numeric_token_1 = r.numeric_token_1),false) OR coalesce((list_extract(l.very_unusual_tokens_arr, 1) = list_extract(r.very_unusual_tokens_arr, 2) and l.numeric_token_1 = r.numeric_token_1),false) OR coalesce((l.numeric_token_2 = r.numeric_token_2 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_1 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 2) = split_part(r.postcode, ' ', 2)),false) OR coalesce((l.numeric_token_2 = r.numeric_token_2 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 2) = split_part(r.postcode, ' ', 2)),false) OR coalesce((l.numeric_token_2 = r.numeric_token_2 and l.postcode = r.postcode),false) OR coalesce((l.numeric_token_1 = r.numeric_1_alt and l.postcode = r.postcode),false) OR coalesce((l.numeric_1_alt = r.numeric_token_1 and l.postcode = r.postcode),false) OR coalesce((l.numeric_token_1 = r.numeric_1_alt and l.numeric_token_2 = r.numeric_token_2 and split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1)),false) OR coalesce((l.numeric_1_alt = r.numeric_token_1 and l.numeric_token_2 = r.numeric_token_2 and split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1)),false) OR coalesce((l.numeric_token_1 = r.numeric_1_alt and l.numeric_token_2 = r.numeric_token_2 and split_part(l.postcode, ' ', 2) = split_part(r.postcode, ' ', 2)),false) OR coalesce((l.numeric_1_alt = r.numeric_token_1 and l.numeric_token_2 = r.numeric_token_2 and split_part(l.postcode, ' ', 2) = split_part(r.postcode, ' ', 2)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_1 and l.numeric_token_2 = r.numeric_token_2 and split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1)),false))
             UNION ALL
            select
            "l"."source_dataset" AS "source_dataset_l", "r"."source_dataset" AS "source_dataset_r", "l"."unique_id" AS "unique_id_l", "r"."unique_id" AS "unique_id_r"
            , '22' as match_key

            from __splink__df_concat_with_tf_left as l
            inner join __splink__df_concat_with_tf_right as r
            on
            (list_extract(l.extremely_unusual_tokens_arr, 1) = list_extract(r.extremely_unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1))
             where 1=1
            AND NOT (coalesce((l.numeric_token_1 = r.numeric_token_1 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1) and list_extract(l.unusual_tokens_arr, 2) = list_extract(r.unusual_tokens_arr, 2) and split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_2 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_2 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 2) = split_part(r.postcode, ' ', 2)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_1 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 2) and list_extract(l.unusual_tokens_arr, 2) = list_extract(r.unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_1 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 2) and split_part(l.postcode, ' ', 2) = split_part(r.postcode, ' ', 2)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_1 and l.postcode = r.postcode),false) OR coalesce((l.numeric_token_1 = r.numeric_token_2 and l.postcode = r.postcode),false) OR coalesce((list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 2) and l.postcode = r.postcode),false) OR coalesce((list_extract(l.very_unusual_tokens_arr, 1) = list_extract(r.very_unusual_tokens_arr, 1) and l.numeric_token_1 = r.numeric_token_1),false) OR coalesce((list_extract(l.very_unusual_tokens_arr, 1) = list_extract(r.very_unusual_tokens_arr, 2) and l.numeric_token_1 = r.numeric_token_1),false) OR coalesce((l.numeric_token_2 = r.numeric_token_2 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_1 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 2) = split_part(r.postcode, ' ', 2)),false) OR coalesce((l.numeric_token_2 = r.numeric_token_2 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 2) = split_part(r.postcode, ' ', 2)),false) OR coalesce((l.numeric_token_2 = r.numeric_token_2 and l.postcode = r.postcode),false) OR coalesce((l.numeric_token_1 = r.numeric_1_alt and l.postcode = r.postcode),false) OR coalesce((l.numeric_1_alt = r.numeric_token_1 and l.postcode = r.postcode),false) OR coalesce((l.numeric_token_1 = r.numeric_1_alt and l.numeric_token_2 = r.numeric_token_2 and split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1)),false) OR coalesce((l.numeric_1_alt = r.numeric_token_1 and l.numeric_token_2 = r.numeric_token_2 and split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1)),false) OR coalesce((l.numeric_token_1 = r.numeric_1_alt and l.numeric_token_2 = r.numeric_token_2 and split_part(l.postcode, ' ', 2) = split_part(r.postcode, ' ', 2)),false) OR coalesce((l.numeric_1_alt = r.numeric_token_1 and l.numeric_token_2 = r.numeric_token_2 and split_part(l.postcode, ' ', 2) = split_part(r.postcode, ' ', 2)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_1 and l.numeric_token_2 = r.numeric_token_2 and split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_1 and l.numeric_token_2 = r.numeric_token_2 and split_part(l.postcode, ' ', 2) = split_part(r.postcode, ' ', 2)),false))
             UNION ALL
            select
            "l"."source_dataset" AS "source_dataset_l", "r"."source_dataset" AS "source_dataset_r", "l"."unique_id" AS "unique_id_l", "r"."unique_id" AS "unique_id_r"
            , '23' as match_key

            from __splink__df_concat_with_tf_left as l
            inner join __splink__df_concat_with_tf_right as r
            on
            (list_extract(l.extremely_unusual_tokens_arr, 1) = list_extract(r.extremely_unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 2) = split_part(r.postcode, ' ', 2))
             where 1=1
            AND NOT (coalesce((l.numeric_token_1 = r.numeric_token_1 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1) and list_extract(l.unusual_tokens_arr, 2) = list_extract(r.unusual_tokens_arr, 2) and split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_2 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_2 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 2) = split_part(r.postcode, ' ', 2)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_1 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 2) and list_extract(l.unusual_tokens_arr, 2) = list_extract(r.unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_1 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 2) and split_part(l.postcode, ' ', 2) = split_part(r.postcode, ' ', 2)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_1 and l.postcode = r.postcode),false) OR coalesce((l.numeric_token_1 = r.numeric_token_2 and l.postcode = r.postcode),false) OR coalesce((list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 2) and l.postcode = r.postcode),false) OR coalesce((list_extract(l.very_unusual_tokens_arr, 1) = list_extract(r.very_unusual_tokens_arr, 1) and l.numeric_token_1 = r.numeric_token_1),false) OR coalesce((list_extract(l.very_unusual_tokens_arr, 1) = list_extract(r.very_unusual_tokens_arr, 2) and l.numeric_token_1 = r.numeric_token_1),false) OR coalesce((l.numeric_token_2 = r.numeric_token_2 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_1 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 2) = split_part(r.postcode, ' ', 2)),false) OR coalesce((l.numeric_token_2 = r.numeric_token_2 and list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 2) = split_part(r.postcode, ' ', 2)),false) OR coalesce((l.numeric_token_2 = r.numeric_token_2 and l.postcode = r.postcode),false) OR coalesce((l.numeric_token_1 = r.numeric_1_alt and l.postcode = r.postcode),false) OR coalesce((l.numeric_1_alt = r.numeric_token_1 and l.postcode = r.postcode),false) OR coalesce((l.numeric_token_1 = r.numeric_1_alt and l.numeric_token_2 = r.numeric_token_2 and split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1)),false) OR coalesce((l.numeric_1_alt = r.numeric_token_1 and l.numeric_token_2 = r.numeric_token_2 and split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1)),false) OR coalesce((l.numeric_token_1 = r.numeric_1_alt and l.numeric_token_2 = r.numeric_token_2 and split_part(l.postcode, ' ', 2) = split_part(r.postcode, ' ', 2)),false) OR coalesce((l.numeric_1_alt = r.numeric_token_1 and l.numeric_token_2 = r.numeric_token_2 and split_part(l.postcode, ' ', 2) = split_part(r.postcode, ' ', 2)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_1 and l.numeric_token_2 = r.numeric_token_2 and split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1)),false) OR coalesce((l.numeric_token_1 = r.numeric_token_1 and l.numeric_token_2 = r.numeric_token_2 and split_part(l.postcode, ' ', 2) = split_part(r.postcode, ' ', 2)),false) OR coalesce((list_extract(l.extremely_unusual_tokens_arr, 1) = list_extract(r.extremely_unusual_tokens_arr, 1) and split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1)),false))
            {pc_blocking_rule}
            -- end_blocking
            )

    select *, ceiling(random() * 8) as _salt_block from __splink__df_blocked)
    """
    if full_block:
        # replace text between the -- start_blocking  line and the -- end_blocking line
        # with 1=1
        replace = """

        select
            "l"."source_dataset" AS "source_dataset_l", "r"."source_dataset" AS "source_dataset_r", "l"."unique_id" AS "unique_id_l", "r"."unique_id" AS "unique_id_r"
            , '1' as match_key

            from __splink__df_concat_with_tf_left as l
            inner join __splink__df_concat_with_tf_right as r
            on 1=1
        """
        sql = re.sub(
            r"(?s)-- start_blocking.*?-- end_blocking",
            replace,
            sql,
        )
    start_time = time.time()
    linker._con.sql(sql)
    end_time = time.time()
    elapsed_time = end_time - start_time
    if print_timings:
        print(f"Time taken to block: {elapsed_time:.2f} seconds")

    if additional_columns_to_retain:
        additional_cols_expr = ", ".join(
            [
                f"l.{col} as {col}_l, r.{col} as {col}_r"
                for col in additional_columns_to_retain
            ]
        )
        additional_cols_expr_2 = ", ".join(
            [f"{col}_l, {col}_r" for col in additional_columns_to_retain]
        )
    else:
        additional_cols_expr = ""
        additional_cols_expr_2 = ""

    if output_all_cols:
        final_select_expr = "*"
    else:
        final_select_expr = "match_probability, match_weight, concat_ws(' ', original_address_concat_l, postcode_l) as address_l, concat_ws(' ', original_address_concat_r, postcode_r) as address_r, unique_id_l, unique_id_r,  source_dataset_l, source_dataset_r"

    if match_weight_threshold:
        match_weight_condition = f"case when match_weight > {match_weight_threshold} then 1 else 0 end as match_weight_cond"
    else:
        match_weight_condition = "1 as match_weight_cond"

    if match_weight_threshold:
        qualify_expr = f"""

        QUALIFY
        rank() OVER (PARTITION BY unique_id_l ORDER BY match_weight_cond desc) <= 1
        and  max(match_weight) over (partition by unique_id_l) > {match_weight_threshold}
        """
    else:
        qualify_expr = ""

    sql = f"""
    create or replace table predictions as (
    WITH __splink__df_concat_with_tf as (select * from {tf_table.physical_name}),
    __splink__df_concat_with_tf_left as (
            select * from __splink__df_concat_with_tf
            where source_dataset = '{left_source_dataset}'

            ),
    __splink__df_concat_with_tf_right as (
            select * from __splink__df_concat_with_tf
            where source_dataset = '{right_source_dataset}'

            ),
    __splink__df_blocked as (
    select  "l"."source_dataset" AS "source_dataset_l", "r"."source_dataset" AS "source_dataset_r", "l"."unique_id" AS "unique_id_l", "r"."unique_id" AS "unique_id_r", "l"."flat_positional" AS "flat_positional_l", "r"."flat_positional" AS "flat_positional_r", "l"."numeric_token_1" AS "numeric_token_1_l", "r"."numeric_token_1" AS "numeric_token_1_r", "l"."tf_numeric_token_1" AS "tf_numeric_token_1_l", "r"."tf_numeric_token_1" AS "tf_numeric_token_1_r", "l"."numeric_1_alt" AS "numeric_1_alt_l", "r"."numeric_1_alt" AS "numeric_1_alt_r", "l"."numeric_token_2" AS "numeric_token_2_l", "r"."numeric_token_2" AS "numeric_token_2_r", "l"."tf_numeric_token_2" AS "tf_numeric_token_2_l", "r"."tf_numeric_token_2" AS "tf_numeric_token_2_r", "l"."numeric_token_3" AS "numeric_token_3_l", "r"."numeric_token_3" AS "numeric_token_3_r", "l"."tf_numeric_token_3" AS "tf_numeric_token_3_l", "r"."tf_numeric_token_3" AS "tf_numeric_token_3_r", "l"."token_rel_freq_arr" AS "token_rel_freq_arr_l", "r"."token_rel_freq_arr" AS "token_rel_freq_arr_r", "l"."common_end_tokens" AS "common_end_tokens_l", "r"."common_end_tokens" AS "common_end_tokens_r", "l"."original_address_concat" AS "original_address_concat_l", "r"."original_address_concat" AS "original_address_concat_r", "l"."postcode" AS "postcode_l", "r"."postcode" AS "postcode_r", "l"."extremely_unusual_tokens_arr" AS "extremely_unusual_tokens_arr_l", "r"."extremely_unusual_tokens_arr" AS "extremely_unusual_tokens_arr_r", "l"."very_unusual_tokens_arr" AS "very_unusual_tokens_arr_l", "r"."very_unusual_tokens_arr" AS "very_unusual_tokens_arr_r", "l"."unusual_tokens_arr" AS "unusual_tokens_arr_l", "r"."unusual_tokens_arr" AS "unusual_tokens_arr_r", b.match_key as match_key,
    {additional_cols_expr}
    from blocked_pairs as b inner join __splink__df_concat_with_tf_left as l on b.unique_id_l = l.unique_id and b.source_dataset_l = l.source_dataset inner join __splink__df_concat_with_tf_right as r on b.unique_id_r = r.unique_id and b.source_dataset_r = r.source_dataset
    where b._salt_block = 1
    UNION ALL
    select  "l"."source_dataset" AS "source_dataset_l", "r"."source_dataset" AS "source_dataset_r", "l"."unique_id" AS "unique_id_l", "r"."unique_id" AS "unique_id_r", "l"."flat_positional" AS "flat_positional_l", "r"."flat_positional" AS "flat_positional_r", "l"."numeric_token_1" AS "numeric_token_1_l", "r"."numeric_token_1" AS "numeric_token_1_r", "l"."tf_numeric_token_1" AS "tf_numeric_token_1_l", "r"."tf_numeric_token_1" AS "tf_numeric_token_1_r", "l"."numeric_1_alt" AS "numeric_1_alt_l", "r"."numeric_1_alt" AS "numeric_1_alt_r", "l"."numeric_token_2" AS "numeric_token_2_l", "r"."numeric_token_2" AS "numeric_token_2_r", "l"."tf_numeric_token_2" AS "tf_numeric_token_2_l", "r"."tf_numeric_token_2" AS "tf_numeric_token_2_r", "l"."numeric_token_3" AS "numeric_token_3_l", "r"."numeric_token_3" AS "numeric_token_3_r", "l"."tf_numeric_token_3" AS "tf_numeric_token_3_l", "r"."tf_numeric_token_3" AS "tf_numeric_token_3_r", "l"."token_rel_freq_arr" AS "token_rel_freq_arr_l", "r"."token_rel_freq_arr" AS "token_rel_freq_arr_r", "l"."common_end_tokens" AS "common_end_tokens_l", "r"."common_end_tokens" AS "common_end_tokens_r", "l"."original_address_concat" AS "original_address_concat_l", "r"."original_address_concat" AS "original_address_concat_r", "l"."postcode" AS "postcode_l", "r"."postcode" AS "postcode_r", "l"."extremely_unusual_tokens_arr" AS "extremely_unusual_tokens_arr_l", "r"."extremely_unusual_tokens_arr" AS "extremely_unusual_tokens_arr_r", "l"."very_unusual_tokens_arr" AS "very_unusual_tokens_arr_l", "r"."very_unusual_tokens_arr" AS "very_unusual_tokens_arr_r", "l"."unusual_tokens_arr" AS "unusual_tokens_arr_l", "r"."unusual_tokens_arr" AS "unusual_tokens_arr_r", b.match_key as match_key,
    {additional_cols_expr}
    from blocked_pairs as b inner join __splink__df_concat_with_tf_left as l on b.unique_id_l = l.unique_id and b.source_dataset_l = l.source_dataset inner join __splink__df_concat_with_tf_right as r on b.unique_id_r = r.unique_id and b.source_dataset_r = r.source_dataset
    where b._salt_block = 2
    UNION ALL
    select  "l"."source_dataset" AS "source_dataset_l", "r"."source_dataset" AS "source_dataset_r", "l"."unique_id" AS "unique_id_l", "r"."unique_id" AS "unique_id_r", "l"."flat_positional" AS "flat_positional_l", "r"."flat_positional" AS "flat_positional_r", "l"."numeric_token_1" AS "numeric_token_1_l", "r"."numeric_token_1" AS "numeric_token_1_r", "l"."tf_numeric_token_1" AS "tf_numeric_token_1_l", "r"."tf_numeric_token_1" AS "tf_numeric_token_1_r", "l"."numeric_1_alt" AS "numeric_1_alt_l", "r"."numeric_1_alt" AS "numeric_1_alt_r", "l"."numeric_token_2" AS "numeric_token_2_l", "r"."numeric_token_2" AS "numeric_token_2_r", "l"."tf_numeric_token_2" AS "tf_numeric_token_2_l", "r"."tf_numeric_token_2" AS "tf_numeric_token_2_r", "l"."numeric_token_3" AS "numeric_token_3_l", "r"."numeric_token_3" AS "numeric_token_3_r", "l"."tf_numeric_token_3" AS "tf_numeric_token_3_l", "r"."tf_numeric_token_3" AS "tf_numeric_token_3_r", "l"."token_rel_freq_arr" AS "token_rel_freq_arr_l", "r"."token_rel_freq_arr" AS "token_rel_freq_arr_r", "l"."common_end_tokens" AS "common_end_tokens_l", "r"."common_end_tokens" AS "common_end_tokens_r", "l"."original_address_concat" AS "original_address_concat_l", "r"."original_address_concat" AS "original_address_concat_r", "l"."postcode" AS "postcode_l", "r"."postcode" AS "postcode_r", "l"."extremely_unusual_tokens_arr" AS "extremely_unusual_tokens_arr_l", "r"."extremely_unusual_tokens_arr" AS "extremely_unusual_tokens_arr_r", "l"."very_unusual_tokens_arr" AS "very_unusual_tokens_arr_l", "r"."very_unusual_tokens_arr" AS "very_unusual_tokens_arr_r", "l"."unusual_tokens_arr" AS "unusual_tokens_arr_l", "r"."unusual_tokens_arr" AS "unusual_tokens_arr_r", b.match_key as match_key,
    {additional_cols_expr}
    from blocked_pairs as b inner join __splink__df_concat_with_tf_left as l on b.unique_id_l = l.unique_id and b.source_dataset_l = l.source_dataset inner join __splink__df_concat_with_tf_right as r on b.unique_id_r = r.unique_id and b.source_dataset_r = r.source_dataset
    where b._salt_block = 3
    UNION ALL
    select  "l"."source_dataset" AS "source_dataset_l", "r"."source_dataset" AS "source_dataset_r", "l"."unique_id" AS "unique_id_l", "r"."unique_id" AS "unique_id_r", "l"."flat_positional" AS "flat_positional_l", "r"."flat_positional" AS "flat_positional_r", "l"."numeric_token_1" AS "numeric_token_1_l", "r"."numeric_token_1" AS "numeric_token_1_r", "l"."tf_numeric_token_1" AS "tf_numeric_token_1_l", "r"."tf_numeric_token_1" AS "tf_numeric_token_1_r", "l"."numeric_1_alt" AS "numeric_1_alt_l", "r"."numeric_1_alt" AS "numeric_1_alt_r", "l"."numeric_token_2" AS "numeric_token_2_l", "r"."numeric_token_2" AS "numeric_token_2_r", "l"."tf_numeric_token_2" AS "tf_numeric_token_2_l", "r"."tf_numeric_token_2" AS "tf_numeric_token_2_r", "l"."numeric_token_3" AS "numeric_token_3_l", "r"."numeric_token_3" AS "numeric_token_3_r", "l"."tf_numeric_token_3" AS "tf_numeric_token_3_l", "r"."tf_numeric_token_3" AS "tf_numeric_token_3_r", "l"."token_rel_freq_arr" AS "token_rel_freq_arr_l", "r"."token_rel_freq_arr" AS "token_rel_freq_arr_r", "l"."common_end_tokens" AS "common_end_tokens_l", "r"."common_end_tokens" AS "common_end_tokens_r", "l"."original_address_concat" AS "original_address_concat_l", "r"."original_address_concat" AS "original_address_concat_r", "l"."postcode" AS "postcode_l", "r"."postcode" AS "postcode_r", "l"."extremely_unusual_tokens_arr" AS "extremely_unusual_tokens_arr_l", "r"."extremely_unusual_tokens_arr" AS "extremely_unusual_tokens_arr_r", "l"."very_unusual_tokens_arr" AS "very_unusual_tokens_arr_l", "r"."very_unusual_tokens_arr" AS "very_unusual_tokens_arr_r", "l"."unusual_tokens_arr" AS "unusual_tokens_arr_l", "r"."unusual_tokens_arr" AS "unusual_tokens_arr_r", b.match_key as match_key,
    {additional_cols_expr}
    from blocked_pairs as b inner join __splink__df_concat_with_tf_left as l on b.unique_id_l = l.unique_id and b.source_dataset_l = l.source_dataset inner join __splink__df_concat_with_tf_right as r on b.unique_id_r = r.unique_id and b.source_dataset_r = r.source_dataset
    where b._salt_block = 4
    UNION ALL
    select  "l"."source_dataset" AS "source_dataset_l", "r"."source_dataset" AS "source_dataset_r", "l"."unique_id" AS "unique_id_l", "r"."unique_id" AS "unique_id_r", "l"."flat_positional" AS "flat_positional_l", "r"."flat_positional" AS "flat_positional_r", "l"."numeric_token_1" AS "numeric_token_1_l", "r"."numeric_token_1" AS "numeric_token_1_r", "l"."tf_numeric_token_1" AS "tf_numeric_token_1_l", "r"."tf_numeric_token_1" AS "tf_numeric_token_1_r", "l"."numeric_1_alt" AS "numeric_1_alt_l", "r"."numeric_1_alt" AS "numeric_1_alt_r", "l"."numeric_token_2" AS "numeric_token_2_l", "r"."numeric_token_2" AS "numeric_token_2_r", "l"."tf_numeric_token_2" AS "tf_numeric_token_2_l", "r"."tf_numeric_token_2" AS "tf_numeric_token_2_r", "l"."numeric_token_3" AS "numeric_token_3_l", "r"."numeric_token_3" AS "numeric_token_3_r", "l"."tf_numeric_token_3" AS "tf_numeric_token_3_l", "r"."tf_numeric_token_3" AS "tf_numeric_token_3_r", "l"."token_rel_freq_arr" AS "token_rel_freq_arr_l", "r"."token_rel_freq_arr" AS "token_rel_freq_arr_r", "l"."common_end_tokens" AS "common_end_tokens_l", "r"."common_end_tokens" AS "common_end_tokens_r", "l"."original_address_concat" AS "original_address_concat_l", "r"."original_address_concat" AS "original_address_concat_r", "l"."postcode" AS "postcode_l", "r"."postcode" AS "postcode_r", "l"."extremely_unusual_tokens_arr" AS "extremely_unusual_tokens_arr_l", "r"."extremely_unusual_tokens_arr" AS "extremely_unusual_tokens_arr_r", "l"."very_unusual_tokens_arr" AS "very_unusual_tokens_arr_l", "r"."very_unusual_tokens_arr" AS "very_unusual_tokens_arr_r", "l"."unusual_tokens_arr" AS "unusual_tokens_arr_l", "r"."unusual_tokens_arr" AS "unusual_tokens_arr_r", b.match_key as match_key,
    {additional_cols_expr}
    from blocked_pairs as b inner join __splink__df_concat_with_tf_left as l on b.unique_id_l = l.unique_id and b.source_dataset_l = l.source_dataset inner join __splink__df_concat_with_tf_right as r on b.unique_id_r = r.unique_id and b.source_dataset_r = r.source_dataset
    where b._salt_block = 5
    UNION ALL
    select  "l"."source_dataset" AS "source_dataset_l", "r"."source_dataset" AS "source_dataset_r", "l"."unique_id" AS "unique_id_l", "r"."unique_id" AS "unique_id_r", "l"."flat_positional" AS "flat_positional_l", "r"."flat_positional" AS "flat_positional_r", "l"."numeric_token_1" AS "numeric_token_1_l", "r"."numeric_token_1" AS "numeric_token_1_r", "l"."tf_numeric_token_1" AS "tf_numeric_token_1_l", "r"."tf_numeric_token_1" AS "tf_numeric_token_1_r", "l"."numeric_1_alt" AS "numeric_1_alt_l", "r"."numeric_1_alt" AS "numeric_1_alt_r", "l"."numeric_token_2" AS "numeric_token_2_l", "r"."numeric_token_2" AS "numeric_token_2_r", "l"."tf_numeric_token_2" AS "tf_numeric_token_2_l", "r"."tf_numeric_token_2" AS "tf_numeric_token_2_r", "l"."numeric_token_3" AS "numeric_token_3_l", "r"."numeric_token_3" AS "numeric_token_3_r", "l"."tf_numeric_token_3" AS "tf_numeric_token_3_l", "r"."tf_numeric_token_3" AS "tf_numeric_token_3_r", "l"."token_rel_freq_arr" AS "token_rel_freq_arr_l", "r"."token_rel_freq_arr" AS "token_rel_freq_arr_r", "l"."common_end_tokens" AS "common_end_tokens_l", "r"."common_end_tokens" AS "common_end_tokens_r", "l"."original_address_concat" AS "original_address_concat_l", "r"."original_address_concat" AS "original_address_concat_r", "l"."postcode" AS "postcode_l", "r"."postcode" AS "postcode_r", "l"."extremely_unusual_tokens_arr" AS "extremely_unusual_tokens_arr_l", "r"."extremely_unusual_tokens_arr" AS "extremely_unusual_tokens_arr_r", "l"."very_unusual_tokens_arr" AS "very_unusual_tokens_arr_l", "r"."very_unusual_tokens_arr" AS "very_unusual_tokens_arr_r", "l"."unusual_tokens_arr" AS "unusual_tokens_arr_l", "r"."unusual_tokens_arr" AS "unusual_tokens_arr_r", b.match_key as match_key,
    {additional_cols_expr}
    from blocked_pairs as b inner join __splink__df_concat_with_tf_left as l on b.unique_id_l = l.unique_id and b.source_dataset_l = l.source_dataset inner join __splink__df_concat_with_tf_right as r on b.unique_id_r = r.unique_id and b.source_dataset_r = r.source_dataset
    where b._salt_block = 6
    UNION ALL
    select  "l"."source_dataset" AS "source_dataset_l", "r"."source_dataset" AS "source_dataset_r", "l"."unique_id" AS "unique_id_l", "r"."unique_id" AS "unique_id_r", "l"."flat_positional" AS "flat_positional_l", "r"."flat_positional" AS "flat_positional_r", "l"."numeric_token_1" AS "numeric_token_1_l", "r"."numeric_token_1" AS "numeric_token_1_r", "l"."tf_numeric_token_1" AS "tf_numeric_token_1_l", "r"."tf_numeric_token_1" AS "tf_numeric_token_1_r", "l"."numeric_1_alt" AS "numeric_1_alt_l", "r"."numeric_1_alt" AS "numeric_1_alt_r", "l"."numeric_token_2" AS "numeric_token_2_l", "r"."numeric_token_2" AS "numeric_token_2_r", "l"."tf_numeric_token_2" AS "tf_numeric_token_2_l", "r"."tf_numeric_token_2" AS "tf_numeric_token_2_r", "l"."numeric_token_3" AS "numeric_token_3_l", "r"."numeric_token_3" AS "numeric_token_3_r", "l"."tf_numeric_token_3" AS "tf_numeric_token_3_l", "r"."tf_numeric_token_3" AS "tf_numeric_token_3_r", "l"."token_rel_freq_arr" AS "token_rel_freq_arr_l", "r"."token_rel_freq_arr" AS "token_rel_freq_arr_r", "l"."common_end_tokens" AS "common_end_tokens_l", "r"."common_end_tokens" AS "common_end_tokens_r", "l"."original_address_concat" AS "original_address_concat_l", "r"."original_address_concat" AS "original_address_concat_r", "l"."postcode" AS "postcode_l", "r"."postcode" AS "postcode_r", "l"."extremely_unusual_tokens_arr" AS "extremely_unusual_tokens_arr_l", "r"."extremely_unusual_tokens_arr" AS "extremely_unusual_tokens_arr_r", "l"."very_unusual_tokens_arr" AS "very_unusual_tokens_arr_l", "r"."very_unusual_tokens_arr" AS "very_unusual_tokens_arr_r", "l"."unusual_tokens_arr" AS "unusual_tokens_arr_l", "r"."unusual_tokens_arr" AS "unusual_tokens_arr_r", b.match_key as match_key,
    {additional_cols_expr}
    from blocked_pairs as b inner join __splink__df_concat_with_tf_left as l on b.unique_id_l = l.unique_id and b.source_dataset_l = l.source_dataset inner join __splink__df_concat_with_tf_right as r on b.unique_id_r = r.unique_id and b.source_dataset_r = r.source_dataset
    where b._salt_block = 7
    UNION ALL
    select  "l"."source_dataset" AS "source_dataset_l", "r"."source_dataset" AS "source_dataset_r", "l"."unique_id" AS "unique_id_l", "r"."unique_id" AS "unique_id_r", "l"."flat_positional" AS "flat_positional_l", "r"."flat_positional" AS "flat_positional_r", "l"."numeric_token_1" AS "numeric_token_1_l", "r"."numeric_token_1" AS "numeric_token_1_r", "l"."tf_numeric_token_1" AS "tf_numeric_token_1_l", "r"."tf_numeric_token_1" AS "tf_numeric_token_1_r", "l"."numeric_1_alt" AS "numeric_1_alt_l", "r"."numeric_1_alt" AS "numeric_1_alt_r", "l"."numeric_token_2" AS "numeric_token_2_l", "r"."numeric_token_2" AS "numeric_token_2_r", "l"."tf_numeric_token_2" AS "tf_numeric_token_2_l", "r"."tf_numeric_token_2" AS "tf_numeric_token_2_r", "l"."numeric_token_3" AS "numeric_token_3_l", "r"."numeric_token_3" AS "numeric_token_3_r", "l"."tf_numeric_token_3" AS "tf_numeric_token_3_l", "r"."tf_numeric_token_3" AS "tf_numeric_token_3_r", "l"."token_rel_freq_arr" AS "token_rel_freq_arr_l", "r"."token_rel_freq_arr" AS "token_rel_freq_arr_r", "l"."common_end_tokens" AS "common_end_tokens_l", "r"."common_end_tokens" AS "common_end_tokens_r", "l"."original_address_concat" AS "original_address_concat_l", "r"."original_address_concat" AS "original_address_concat_r", "l"."postcode" AS "postcode_l", "r"."postcode" AS "postcode_r", "l"."extremely_unusual_tokens_arr" AS "extremely_unusual_tokens_arr_l", "r"."extremely_unusual_tokens_arr" AS "extremely_unusual_tokens_arr_r", "l"."very_unusual_tokens_arr" AS "very_unusual_tokens_arr_l", "r"."very_unusual_tokens_arr" AS "very_unusual_tokens_arr_r", "l"."unusual_tokens_arr" AS "unusual_tokens_arr_l", "r"."unusual_tokens_arr" AS "unusual_tokens_arr_r", b.match_key as match_key,
    {additional_cols_expr}
    from blocked_pairs as b inner join __splink__df_concat_with_tf_left as l on b.unique_id_l = l.unique_id and b.source_dataset_l = l.source_dataset inner join __splink__df_concat_with_tf_right as r on b.unique_id_r = r.unique_id and b.source_dataset_r = r.source_dataset
    where b._salt_block = 8
    ),
    __reusable as (
    select
    *,
    list_reduce(
            list_prepend(
                1.0,
                list_transform(
                    array_filter(
                        token_rel_freq_arr_l,
                        y -> array_contains(
                            array_intersect(
                                list_transform(token_rel_freq_arr_l, x -> x.tok),
                                list_transform(token_rel_freq_arr_r, x -> x.tok)
                            ),
                            y.tok
                        )
                    ),
                    x -> x.rel_freq
                )
            ),
            (p, q) -> p * q
        )
        *
        list_reduce(
            list_prepend(
                1.0,
                list_transform(
                    list_concat(
                        array_filter(
                            token_rel_freq_arr_l,
                                y -> not array_contains(
                                        list_transform(token_rel_freq_arr_r, x -> x.tok),
                                        y.tok
                                    )
                        ),
                        array_filter(
                            token_rel_freq_arr_r,
                                y -> not array_contains(
                                        list_transform(token_rel_freq_arr_l, x -> x.tok),
                                        y.tok
                                    )
                        )
                    ),

                    x -> x.rel_freq
                )
            ),
            (p, q) -> p / q^0.33

        )
        as complex_tok
    from __splink__df_blocked
    ),
    __splink__df_comparison_vectors as (
        select "source_dataset_l","source_dataset_r","unique_id_l","unique_id_r","flat_positional_l","flat_positional_r",CASE WHEN "flat_positional_l" IS NULL AND "flat_positional_r" IS NULL THEN -1 WHEN "flat_positional_l" = "flat_positional_r" THEN 1 ELSE 0 END as gamma_flat_positional,"numeric_token_1_l","numeric_token_1_r","numeric_1_alt_l","numeric_1_alt_r","numeric_token_2_l","numeric_token_2_r",CASE WHEN "numeric_token_1_l" IS NULL OR "numeric_token_1_r" IS NULL THEN -1 WHEN "numeric_token_1_l" = "numeric_token_1_r" THEN 4 WHEN "numeric_1_alt_l" = "numeric_token_1_r" OR "numeric_token_1_l" = "numeric_1_alt_r" OR "numeric_1_alt_l" = "numeric_1_alt_r" THEN 3 WHEN "numeric_token_2_l" = "numeric_token_1_r" THEN 2 WHEN "numeric_token_1_l" IS NULL OR "numeric_token_1_r" IS NULL THEN 1 ELSE 0 END as gamma_numeric_token_1,"tf_numeric_token_1_l","tf_numeric_token_1_r",CASE WHEN "numeric_token_2_l" IS NULL AND "numeric_token_2_r" IS NULL THEN -1 WHEN "numeric_token_2_l" = "numeric_token_2_r" THEN 3 WHEN "numeric_token_1_l" = "numeric_token_2_r" THEN 2 WHEN "numeric_token_2_l" IS NULL OR "numeric_token_2_r" IS NULL THEN 1 ELSE 0 END as gamma_numeric_token_2,"tf_numeric_token_2_l","tf_numeric_token_2_r","numeric_token_3_l","numeric_token_3_r",CASE WHEN "numeric_token_3_l" IS NULL AND "numeric_token_3_r" IS NULL THEN -1 WHEN "numeric_token_3_l" = "numeric_token_3_r" THEN 3 WHEN "numeric_token_2_l" = "numeric_token_3_r" THEN 2 WHEN "numeric_token_3_l" IS NULL OR "numeric_token_3_r" IS NULL THEN 1 ELSE 0 END as gamma_numeric_token_3,"tf_numeric_token_3_l","tf_numeric_token_3_r","token_rel_freq_arr_l","token_rel_freq_arr_r",CASE WHEN "token_rel_freq_arr_l" IS NULL OR "token_rel_freq_arr_r" IS NULL or length("token_rel_freq_arr_l") = 0 or length("token_rel_freq_arr_r") = 0 THEN -1 WHEN
        complex_tok
        < 1e-20 THEN 10 WHEN
        complex_tok
        < 1e-18 THEN 9 WHEN
        complex_tok
        < 1e-16 THEN 8 WHEN
        complex_tok
        < 1e-14 THEN 7 WHEN
        complex_tok
        < 1e-12 THEN 6 WHEN
        complex_tok
        < 1e-10 THEN 5 WHEN
        complex_tok
        < 1e-8 THEN 4 WHEN
        complex_tok
        < 1e-6 THEN 3 WHEN
        complex_tok
        < 1e-4 THEN 2 WHEN
        complex_tok
        < 1e-2 THEN 1 ELSE 0 END as gamma_token_rel_freq_arr,"common_end_tokens_l","common_end_tokens_r",CASE WHEN "common_end_tokens_l" IS NULL OR "common_end_tokens_r" IS NULL or length("common_end_tokens_l") = 0 or length("common_end_tokens_r") = 0 THEN -1 WHEN
        list_reduce(
            list_prepend(
                1.0,
                list_transform(
                    array_filter(
                        common_end_tokens_l,
                        y -> array_contains(
                            array_intersect(
                                list_transform(common_end_tokens_l, x -> x.tok),
                                list_transform(common_end_tokens_r, x -> x.tok)
                            ),
                            y.tok
                        )
                    ),
                    x -> x.rel_freq
                )
            ),
            (p, q) -> p * q
        )
        < 1e-2 THEN 1 ELSE 0 END as gamma_common_end_tokens,"original_address_concat_l","original_address_concat_r",CASE WHEN "original_address_concat_l" IS NULL OR "original_address_concat_r" IS NULL THEN -1 WHEN regexp_replace(regexp_replace(original_address_concat_l, '[[:punct:]]', '', 'g'), '\s+', ' ', 'g') = regexp_replace(regexp_replace(original_address_concat_r, '[[:punct:]]', '', 'g'), '\s+', ' ', 'g') THEN 3 WHEN levenshtein(original_address_concat_l, original_address_concat_r) < 3 THEN 2 WHEN levenshtein(original_address_concat_l, original_address_concat_r) < 10 THEN 1 ELSE 0 END as gamma_original_address_concat,"postcode_l","postcode_r",CASE WHEN "postcode_l" IS NULL AND "postcode_r" IS NULL THEN -1 WHEN postcode_l = postcode_r THEN 5 WHEN levenshtein(postcode_l, postcode_r) <= 1 THEN 4 WHEN levenshtein(postcode_l, postcode_r) <= 2 THEN 3 WHEN split_part(postcode_l, ' ', 1) = split_part(postcode_r, ' ', 1) THEN 2 WHEN split_part(postcode_l, ' ', 2) = split_part(postcode_r, ' ', 2) THEN 1 ELSE 0 END as gamma_postcode,"extremely_unusual_tokens_arr_l","extremely_unusual_tokens_arr_r","very_unusual_tokens_arr_l","very_unusual_tokens_arr_r","unusual_tokens_arr_l","unusual_tokens_arr_r",match_key,
        {additional_cols_expr_2}
        from __reusable
        ),
    __splink__df_match_weight_parts as (
        select "source_dataset_l","source_dataset_r","unique_id_l","unique_id_r","flat_positional_l","flat_positional_r",gamma_flat_positional,CASE
    WHEN
    gamma_flat_positional = -1
    THEN cast(1.0 as float8)

    WHEN
    gamma_flat_positional = 1
    THEN cast(95.0 as float8)

    WHEN
    gamma_flat_positional = 0
    THEN cast(0.03125 as float8)
    END as bf_flat_positional ,"numeric_token_1_l","numeric_token_1_r","numeric_1_alt_l","numeric_1_alt_r","numeric_token_2_l","numeric_token_2_r",gamma_numeric_token_1,"tf_numeric_token_1_l","tf_numeric_token_1_r",CASE
    WHEN
    gamma_numeric_token_1 = -1
    THEN cast(1.0 as float8)

    WHEN
    gamma_numeric_token_1 = 4
    THEN cast(95.0 as float8)

    WHEN
    gamma_numeric_token_1 = 3
    THEN cast(90.0 as float8)

    WHEN
    gamma_numeric_token_1 = 2
    THEN cast(3.0 as float8)

    WHEN
    gamma_numeric_token_1 = 1
    THEN cast(0.0625 as float8)

    WHEN
    gamma_numeric_token_1 = 0
    THEN cast(0.00390625 as float8)
    END as bf_numeric_token_1 ,CASE WHEN  gamma_numeric_token_1 = -1 then cast(1 as float8) WHEN  gamma_numeric_token_1 = 4 then
        (CASE WHEN coalesce("tf_numeric_token_1_l", "tf_numeric_token_1_r") is not null
        THEN
        POW(
            cast(0.01 as float8) /
        (CASE
            WHEN coalesce("tf_numeric_token_1_l", "tf_numeric_token_1_r") >= coalesce("tf_numeric_token_1_r", "tf_numeric_token_1_l")
            THEN coalesce("tf_numeric_token_1_l", "tf_numeric_token_1_r")
            ELSE coalesce("tf_numeric_token_1_r", "tf_numeric_token_1_l")
        END)
        ,
            cast(1.0 as float8)
        )
        ELSE cast(1 as float8)
        END) WHEN  gamma_numeric_token_1 = 3 then
        (CASE WHEN coalesce("tf_numeric_token_1_l", "tf_numeric_token_1_r") is not null
        THEN
        POW(
            cast(0.01 as float8) /
        (CASE
            WHEN coalesce("tf_numeric_token_1_l", "tf_numeric_token_1_r") >= coalesce("tf_numeric_token_1_r", "tf_numeric_token_1_l")
            THEN coalesce("tf_numeric_token_1_l", "tf_numeric_token_1_r")
            ELSE coalesce("tf_numeric_token_1_r", "tf_numeric_token_1_l")
        END)
        ,
            cast(0.5 as float8)
        )
        ELSE cast(1 as float8)
        END) WHEN  gamma_numeric_token_1 = 2 then cast(1 as float8) WHEN  gamma_numeric_token_1 = 1 then cast(1 as float8) WHEN  gamma_numeric_token_1 = 0 then cast(1 as float8) END as bf_tf_adj_numeric_token_1 ,gamma_numeric_token_2,"tf_numeric_token_2_l","tf_numeric_token_2_r",CASE
    WHEN
    gamma_numeric_token_2 = -1
    THEN cast(1.0 as float8)

    WHEN
    gamma_numeric_token_2 = 3
    THEN cast(800.0 as float8)

    WHEN
    gamma_numeric_token_2 = 2
    THEN cast(1.0 as float8)

    WHEN
    gamma_numeric_token_2 = 1
    THEN cast(0.0625 as float8)

    WHEN
    gamma_numeric_token_2 = 0
    THEN cast(0.00390625 as float8)
    END as bf_numeric_token_2 ,CASE WHEN  gamma_numeric_token_2 = -1 then cast(1 as float8) WHEN  gamma_numeric_token_2 = 3 then
        (CASE WHEN coalesce("tf_numeric_token_2_l", "tf_numeric_token_2_r") is not null
        THEN
        POW(
            cast(0.001 as float8) /
        (CASE
            WHEN coalesce("tf_numeric_token_2_l", "tf_numeric_token_2_r") >= coalesce("tf_numeric_token_2_r", "tf_numeric_token_2_l")
            THEN coalesce("tf_numeric_token_2_l", "tf_numeric_token_2_r")
            ELSE coalesce("tf_numeric_token_2_r", "tf_numeric_token_2_l")
        END)
        ,
            cast(1.0 as float8)
        )
        ELSE cast(1 as float8)
        END) WHEN  gamma_numeric_token_2 = 2 then cast(1 as float8) WHEN  gamma_numeric_token_2 = 1 then cast(1 as float8) WHEN  gamma_numeric_token_2 = 0 then cast(1 as float8) END as bf_tf_adj_numeric_token_2 ,"numeric_token_3_l","numeric_token_3_r",gamma_numeric_token_3,"tf_numeric_token_3_l","tf_numeric_token_3_r",CASE
    WHEN
    gamma_numeric_token_3 = -1
    THEN cast(1.0 as float8)

    WHEN
    gamma_numeric_token_3 = 3
    THEN cast(5999.999999999999 as float8)

    WHEN
    gamma_numeric_token_3 = 2
    THEN cast(120.0 as float8)

    WHEN
    gamma_numeric_token_3 = 1
    THEN cast(0.0625 as float8)

    WHEN
    gamma_numeric_token_3 = 0
    THEN cast(0.00390625 as float8)
    END as bf_numeric_token_3 ,CASE WHEN  gamma_numeric_token_3 = -1 then cast(1 as float8) WHEN  gamma_numeric_token_3 = 3 then
        (CASE WHEN coalesce("tf_numeric_token_3_l", "tf_numeric_token_3_r") is not null
        THEN
        POW(
            cast(0.0001 as float8) /
        (CASE
            WHEN coalesce("tf_numeric_token_3_l", "tf_numeric_token_3_r") >= coalesce("tf_numeric_token_3_r", "tf_numeric_token_3_l")
            THEN coalesce("tf_numeric_token_3_l", "tf_numeric_token_3_r")
            ELSE coalesce("tf_numeric_token_3_r", "tf_numeric_token_3_l")
        END)
        ,
            cast(1.0 as float8)
        )
        ELSE cast(1 as float8)
        END) WHEN  gamma_numeric_token_3 = 2 then
        (CASE WHEN coalesce("tf_numeric_token_3_l", "tf_numeric_token_3_r") is not null
        THEN
        POW(
            cast(0.0001 as float8) /
        (CASE
            WHEN coalesce("tf_numeric_token_3_l", "tf_numeric_token_3_r") >= coalesce("tf_numeric_token_3_r", "tf_numeric_token_3_l")
            THEN coalesce("tf_numeric_token_3_l", "tf_numeric_token_3_r")
            ELSE coalesce("tf_numeric_token_3_r", "tf_numeric_token_3_l")
        END)
        ,
            cast(1.0 as float8)
        )
        ELSE cast(1 as float8)
        END) WHEN  gamma_numeric_token_3 = 1 then cast(1 as float8) WHEN  gamma_numeric_token_3 = 0 then cast(1 as float8) END as bf_tf_adj_numeric_token_3 ,"token_rel_freq_arr_l","token_rel_freq_arr_r",gamma_token_rel_freq_arr,CASE
    WHEN
    gamma_token_rel_freq_arr = -1
    THEN cast(1.0 as float8)

    WHEN
    gamma_token_rel_freq_arr = 10
    THEN cast(8192.0 as float8)

    WHEN
    gamma_token_rel_freq_arr = 9
    THEN cast(5792.618751480198 as float8)

    WHEN
    gamma_token_rel_freq_arr = 8
    THEN cast(4096.0 as float8)

    WHEN
    gamma_token_rel_freq_arr = 7
    THEN cast(2896.309375740099 as float8)

    WHEN
    gamma_token_rel_freq_arr = 6
    THEN cast(2048.0 as float8)

    WHEN
    gamma_token_rel_freq_arr = 5
    THEN cast(512.0 as float8)

    WHEN
    gamma_token_rel_freq_arr = 4
    THEN cast(128.0 as float8)

    WHEN
    gamma_token_rel_freq_arr = 3
    THEN cast(32.0 as float8)

    WHEN
    gamma_token_rel_freq_arr = 2
    THEN cast(8.0 as float8)

    WHEN
    gamma_token_rel_freq_arr = 1
    THEN cast(2.0 as float8)

    WHEN
    gamma_token_rel_freq_arr = 0
    THEN cast(0.00390625 as float8)
    END as bf_token_rel_freq_arr ,"common_end_tokens_l","common_end_tokens_r",gamma_common_end_tokens,CASE
    WHEN
    gamma_common_end_tokens = -1
    THEN cast(1.0 as float8)

    WHEN
    gamma_common_end_tokens = 1
    THEN cast(4.0 as float8)

    WHEN
    gamma_common_end_tokens = 0
    THEN cast(0.25 as float8)
    END as bf_common_end_tokens ,"original_address_concat_l","original_address_concat_r",gamma_original_address_concat,CASE
    WHEN
    gamma_original_address_concat = -1
    THEN cast(1.0 as float8)

    WHEN
    gamma_original_address_concat = 3
    THEN cast(3.0 as float8)

    WHEN
    gamma_original_address_concat = 2
    THEN cast(2.75 as float8)

    WHEN
    gamma_original_address_concat = 1
    THEN cast(2.5 as float8)

    WHEN
    gamma_original_address_concat = 0
    THEN cast(0.25 as float8)
    END as bf_original_address_concat ,"postcode_l","postcode_r",gamma_postcode,CASE
    WHEN
    gamma_postcode = -1
    THEN cast(1.0 as float8)

    WHEN
    gamma_postcode = 5
    THEN cast(3000000.0 as float8)

    WHEN
    gamma_postcode = 4
    THEN cast(1000000.0 as float8)

    WHEN
    gamma_postcode = 3
    THEN cast(500000.0 as float8)

    WHEN
    gamma_postcode = 2
    THEN cast(3000.0 as float8)

    WHEN
    gamma_postcode = 1
    THEN cast(2000.0 as float8)

    WHEN
    gamma_postcode = 0
    THEN cast(0.015625 as float8)
    END as bf_postcode ,"extremely_unusual_tokens_arr_l","extremely_unusual_tokens_arr_r","very_unusual_tokens_arr_l","very_unusual_tokens_arr_r","unusual_tokens_arr_l","unusual_tokens_arr_r",match_key,
    {additional_cols_expr_2}
        from __splink__df_comparison_vectors
        ),
    s_predictions as (
    select
    log2(cast(3.0000000900000026e-08 as float8) * bf_flat_positional * bf_numeric_token_1 * bf_tf_adj_numeric_token_1 * bf_numeric_token_2 * bf_tf_adj_numeric_token_2 * bf_numeric_token_3 * bf_tf_adj_numeric_token_3 * bf_token_rel_freq_arr * bf_common_end_tokens * bf_original_address_concat * bf_postcode) as match_weight,
    (cast(3.0000000900000026e-08 as float8) * bf_flat_positional * bf_numeric_token_1 * bf_tf_adj_numeric_token_1 * bf_numeric_token_2 * bf_tf_adj_numeric_token_2 * bf_numeric_token_3 * bf_tf_adj_numeric_token_3 * bf_token_rel_freq_arr * bf_common_end_tokens * bf_original_address_concat * bf_postcode)/(1+(cast(3.0000000900000026e-08 as float8) * bf_flat_positional * bf_numeric_token_1 * bf_tf_adj_numeric_token_1 * bf_numeric_token_2 * bf_tf_adj_numeric_token_2 * bf_numeric_token_3 * bf_tf_adj_numeric_token_3 * bf_token_rel_freq_arr * bf_common_end_tokens * bf_original_address_concat * bf_postcode)) as match_probability,
    "source_dataset_l","source_dataset_r","unique_id_l","unique_id_r","flat_positional_l","flat_positional_r",gamma_flat_positional,bf_flat_positional,"numeric_token_1_l","numeric_token_1_r","numeric_1_alt_l","numeric_1_alt_r","numeric_token_2_l","numeric_token_2_r",gamma_numeric_token_1,"tf_numeric_token_1_l","tf_numeric_token_1_r",bf_numeric_token_1,bf_tf_adj_numeric_token_1,gamma_numeric_token_2,"tf_numeric_token_2_l","tf_numeric_token_2_r",bf_numeric_token_2,bf_tf_adj_numeric_token_2,"numeric_token_3_l","numeric_token_3_r",gamma_numeric_token_3,"tf_numeric_token_3_l","tf_numeric_token_3_r",bf_numeric_token_3,bf_tf_adj_numeric_token_3,"token_rel_freq_arr_l","token_rel_freq_arr_r",gamma_token_rel_freq_arr,bf_token_rel_freq_arr,"common_end_tokens_l","common_end_tokens_r",gamma_common_end_tokens,bf_common_end_tokens,"original_address_concat_l","original_address_concat_r",gamma_original_address_concat,bf_original_address_concat,"postcode_l","postcode_r",gamma_postcode,bf_postcode,"extremely_unusual_tokens_arr_l","extremely_unusual_tokens_arr_r","very_unusual_tokens_arr_l","very_unusual_tokens_arr_r","unusual_tokens_arr_l","unusual_tokens_arr_r",match_key,
    {additional_cols_expr_2}
    from __splink__df_match_weight_parts

    ),
    __splink__df_predictions as (
     select *, {match_weight_condition} from s_predictions
     )
    SELECT
        {final_select_expr}
    FROM __splink__df_predictions
    {qualify_expr}


    )
        """
    start_time = time.time()
    linker._con.sql(sql)
    end_time = time.time()
    elapsed_time = end_time - start_time
    if print_timings:
        print(f"Time taken to predict: {elapsed_time:.2f} seconds")
    return linker, con.sql("select * from predictions")
