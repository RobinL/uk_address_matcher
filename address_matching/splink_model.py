# Going to largely follow
# https://github.com/moj-analytical-services/splink/discussions/2022
import importlib.resources as pkg_resources
import json
from typing import List

import duckdb
import splink.duckdb.comparison_level_library as cll
import splink.duckdb.comparison_library as cl
from duckdb import DuckDBPyRelation
from splink.duckdb.blocking_rule_library import block_on
from splink.duckdb.linker import DuckDBLinker

from .arr_comparisons import array_reduce_by_freq


def train_splink_model(
    df_1,
    df_2,
    additional_columns_to_retain=[],
    label_colname=None,
    max_pairs=1e6,
):
    num_1_comparison = {
        "output_column_name": "numeric_token_1",
        "comparison_levels": [
            {
                "sql_condition": '"numeric_token_1_l" IS NULL AND "numeric_token_1_r" IS NULL',
                "label_for_charts": "Null",
                "is_null_level": True,
            },
            {
                "sql_condition": '"numeric_token_1_l" = "numeric_token_1_r"',
                "label_for_charts": "Exact match",
                "tf_adjustment_column": "numeric_token_1",
                "tf_adjustment_weight": 1.0,
            },
            {
                "sql_condition": '"numeric_token_2_l" = "numeric_token_1_r"',
                "label_for_charts": "Exact match inverted numbers",
            },
            {
                "sql_condition": '"numeric_token_1_l" IS NULL OR "numeric_token_1_r" IS NULL',
                "label_for_charts": "One null",
            },
            cll.else_level(),
        ],
        "comparison_description": "numeric_token_1",
    }

    num_2_comparison = {
        "output_column_name": "numeric_token_2",
        "comparison_levels": [
            {
                "sql_condition": '"numeric_token_2_l" IS NULL AND "numeric_token_2_r" IS NULL',
                "label_for_charts": "Null",
                "is_null_level": True,
            },
            {
                "sql_condition": '"numeric_token_2_l" = "numeric_token_2_r"',
                "label_for_charts": "Exact match",
                "tf_adjustment_column": "numeric_token_2",
                "tf_adjustment_weight": 1.0,
            },
            {
                "sql_condition": '"numeric_token_1_l" = "numeric_token_2_r"',
                "label_for_charts": "Exact match inverted numbers",
            },
            {
                "sql_condition": '"numeric_token_2_l" IS NULL OR "numeric_token_2_r" IS NULL',
                "label_for_charts": "One null",
            },
            cll.else_level(),
        ],
        "comparison_description": "numeric_token_2",
    }

    num_3_comparison = {
        "output_column_name": "numeric_token_3",
        "comparison_levels": [
            {
                "sql_condition": '"numeric_token_3_l" IS NULL AND "numeric_token_3_r" IS NULL',
                "label_for_charts": "Null",
                "is_null_level": True,
            },
            {
                "sql_condition": '"numeric_token_3_l" = "numeric_token_3_r"',
                "label_for_charts": "Exact match",
                "tf_adjustment_column": "numeric_token_3",
                "tf_adjustment_weight": 1.0,
            },
            {
                "sql_condition": '"numeric_token_2_l" = "numeric_token_3_r"',
                "label_for_charts": "Exact match inverted",
                "tf_adjustment_column": "numeric_token_3",
                "tf_adjustment_weight": 1.0,
            },
            {
                "sql_condition": '"numeric_token_3_l" IS NULL OR "numeric_token_3_r" IS NULL',
                "label_for_charts": "One null",
            },
            cll.else_level(),
        ],
        "comparison_description": "numeric_token_3",
    }

    arr_red_sql = array_reduce_by_freq("token_rel_freq_arr", 0.33)

    token_rel_freq_arr_comparison = {
        "output_column_name": "token_rel_freq_arr",
        "comparison_levels": [
            {
                "sql_condition": '"token_rel_freq_arr_l" IS NULL OR "token_rel_freq_arr_r" IS NULL or length("token_rel_freq_arr_l") = 0 or length("token_rel_freq_arr_r") = 0',
                "label_for_charts": "Null",
                "is_null_level": True,
            },
            {
                "sql_condition": f"{arr_red_sql} < 1e-20",
                "label_for_charts": "<1e-20",
            },
            {
                "sql_condition": f"{arr_red_sql} < 1e-18",
                "label_for_charts": "<1e-18",
            },
            {
                "sql_condition": f"{arr_red_sql} < 1e-16",
                "label_for_charts": "<1e-16",
            },
            {
                "sql_condition": f"{arr_red_sql} < 1e-14",
                "label_for_charts": "<1e-14",
            },
            {
                "sql_condition": f"{arr_red_sql} < 1e-12",
                "label_for_charts": "<1e-12",
            },
            {
                "sql_condition": f"{arr_red_sql} < 1e-10",
                "label_for_charts": "<1e-10",
            },
            {
                "sql_condition": f"{arr_red_sql} < 1e-8",
                "label_for_charts": "<1e-8",
            },
            {
                "sql_condition": f"{arr_red_sql} < 1e-6",
                "label_for_charts": "<1e-6",
            },
            {
                "sql_condition": f"{arr_red_sql} < 1e-4",
                "label_for_charts": "<1e-4",
            },
            {
                "sql_condition": f"{arr_red_sql} < 1e-2",
                "label_for_charts": "<1e-2",
            },
            {
                "sql_condition": f"{arr_red_sql} < 1",
                "label_for_charts": "<1e-2",
            },
            {"sql_condition": "ELSE", "label_for_charts": "All other comparisons"},
        ],
        "comparison_description": "Token relative frequency array",
    }

    tie_breaker = {
        "output_column_name": "original_address_concat",
        "comparison_levels": [
            {
                "sql_condition": '"original_address_concat_l" IS NULL OR "original_address_concat_r" IS NULL',
                "label_for_charts": "Null",
                "is_null_level": True,
            },
            {
                "sql_condition": "original_address_concat_l = original_address_concat_r",
                "label_for_charts": "Exact match",
            },
            {
                "sql_condition": "levenshtein(original_address_concat_l, original_address_concat_r) < 3",
                "label_for_charts": "Lev < 3",
            },
            {
                "sql_condition": "levenshtein(original_address_concat_l, original_address_concat_r) < 10",
                "label_for_charts": "Lev < 10",
            },
            cll.else_level(),
        ],
        "comparison_description": "numeric_token_3",
    }

    arr_red_sql = array_reduce_by_freq("common_end_tokens", 0.0)

    common_end_tokens_comparison = {
        "output_column_name": "common_end_tokens",
        "comparison_levels": [
            {
                "sql_condition": '"common_end_tokens_l" IS NULL OR "common_end_tokens_r" IS NULL or length("common_end_tokens_l") = 0 or length("common_end_tokens_r") = 0',
                "label_for_charts": "Null",
                "is_null_level": True,
            },
            {
                "sql_condition": f"{arr_red_sql} < 1e-2",
                "label_for_charts": "<1e-2",
            },
            {
                "sql_condition": f"{arr_red_sql} < 0.5",
                "label_for_charts": "<1e-2",
            },
            {"sql_condition": "ELSE", "label_for_charts": "All other comparisons"},
        ],
        "comparison_description": "Array intersection",
    }

    comparisons = [
        num_1_comparison,
        num_2_comparison,
        num_3_comparison,
        token_rel_freq_arr_comparison,
        common_end_tokens_comparison,
        tie_breaker,
    ]

    settings = {
        "probability_two_random_records_match": 0.01,
        "link_type": "link_only",
        "blocking_rules_to_generate_predictions": [
            block_on(["postcode"], salting_partitions=10),
        ],
        "comparisons": comparisons,
        "retain_intermediate_calculation_columns": True,
        "source_dataset_column_name": "source_dataset",
        "additional_columns_to_retain": additional_columns_to_retain,
    }

    linker = DuckDBLinker([df_1, df_2], settings)

    linker.estimate_u_using_random_sampling(max_pairs=max_pairs)

    if label_colname is not None:
        linker.estimate_m_from_label_column(label_colname)

    comparisons = linker._settings_obj.comparisons

    # # Increase punishment for non-matching 'numeric' token
    if label_colname is None:
        c = [c for c in comparisons if c._output_column_name == "common_end_tokens"][0]
        c.comparison_levels[1].m_probability = 1.0
        c.comparison_levels[1].u_probability = 0.2

        c.comparison_levels[2].m_probability = 1.0
        c.comparison_levels[2].u_probability = 0.5

        c.comparison_levels[3].m_probability = 0.5
        c.comparison_levels[3].u_probability = 1.0

        # Reduce weights for common end tokens
        c = [c for c in comparisons if c._output_column_name == "numeric_token_1"][0]
        c.comparison_levels[3].m_probability = 0.001
        c.comparison_levels[3].u_probability = 1.0

    # Tie breaker - small adjustments to match weights if there's nothing to distinguish
    # except for leve distance in full adddress
    c = [c for c in comparisons if c._output_column_name == "original_address_concat"][
        0
    ]
    c.comparison_levels[1].m_probability = 0.9
    c.comparison_levels[1].u_probability = 0.45

    c.comparison_levels[2].m_probability = 0.9
    c.comparison_levels[2].u_probability = 0.6

    c.comparison_levels[3].m_probability = 0.9
    c.comparison_levels[3].u_probability = 0.8

    c.comparison_levels[3].m_probability = 0.9
    c.comparison_levels[3].u_probability = 0.9

    c.comparison_levels[4].m_probability = 0.45
    c.comparison_levels[4].u_probability = 0.9

    return linker


def get_pretrained_linker(
    dfs: List[DuckDBPyRelation], precomputed_numeric_tf_table: DuckDBPyRelation = None
):

    with pkg_resources.path(
        "address_matching.data", "splink_model.json"
    ) as settings_path:

        settings_as_dict = json.load(open(settings_path))

        dfs_pd = [d.df() for d in dfs]

        linker = DuckDBLinker(dfs_pd, settings_dict=settings_as_dict)

    if precomputed_numeric_tf_table is not None:
        duckdb.register("numeric_token_freq", precomputed_numeric_tf_table)
        for i in range(1, 4):

            df_sql = f"""
                select
                    numeric_token as numeric_token_{i},
                    tf_numeric_token as tf_numeric_token_{i}
                from numeric_token_freq"""

            df = duckdb.sql(df_sql).df()
            linker.register_term_frequency_lookup(df, f"numeric_token_{i}")

    return linker
