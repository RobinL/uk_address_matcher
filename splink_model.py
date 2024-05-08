# Going to largely follow
# https://github.com/moj-analytical-services/splink/discussions/2022
import random

import duckdb
import pandas as pd
import splink.duckdb.comparison_level_library as cll
import splink.duckdb.comparison_library as cl
from IPython.display import display
from splink.duckdb.blocking_rule_library import block_on
from splink.duckdb.linker import DuckDBLinker

pd.options.display.max_columns = None
pd.options.display.max_colwidth = None
df_pp = pd.read_parquet("splink_in/price_paid.parquet")
df_epc = pd.read_parquet("splink_in/epc.parquet")


num_1_comparison = {
    "output_column_name": "numeric_token_1",
    "comparison_levels": [
        cll.null_level("numeric_token_1"),
        {
            "sql_condition": '"numeric_token_1_l" = "numeric_token_1_r"',
            "label_for_charts": "Exact match",
            "tf_adjustment_column": "numeric_token_1",
            "tf_adjustment_weight": 1.0,
        },
        {
            "sql_condition": '"numeric_token_2_l" = "numeric_token_1_r"',
            "label_for_charts": "Exact match",
            "tf_adjustment_column": "numeric_token_1",
            "tf_adjustment_weight": 1.0,
        },
        cll.else_level(),
    ],
    "comparison_description": "numeric_token_1",
}


num_2_comparison = {
    "output_column_name": "numeric_token_2",
    "comparison_levels": [
        cll.null_level("numeric_token_2"),
        {
            "sql_condition": '"numeric_token_2_l" = "numeric_token_2_r"',
            "label_for_charts": "Exact match",
            "tf_adjustment_column": "numeric_token_2",
            "tf_adjustment_weight": 1.0,
        },
        {
            "sql_condition": '"numeric_token_1_l" = "numeric_token_2_r"',
            "label_for_charts": "Exact match",
            "tf_adjustment_column": "numeric_token_2",
            "tf_adjustment_weight": 1.0,
        },
        cll.else_level(),
    ],
    "comparison_description": "numeric_token_2",
}

num_3_comparison = {
    "output_column_name": "numeric_token_3",
    "comparison_levels": [
        cll.null_level("numeric_token_3"),
        {
            "sql_condition": '"numeric_token_3_l" = "numeric_token_3_r"',
            "label_for_charts": "Exact match",
            "tf_adjustment_column": "numeric_token_3",
            "tf_adjustment_weight": 1.0,
        },
        {
            "sql_condition": '"numeric_token_2_l" = "numeric_token_3_r"',
            "label_for_charts": "Exact match 2",
            "tf_adjustment_column": "numeric_token_3",
            "tf_adjustment_weight": 1.0,
        },
        cll.else_level(),
    ],
    "comparison_description": "numeric_token_3",
}


calc_tf_sql = """
list_reduce(
    list_prepend(
        1.0,
        list_transform(
            array_filter(
                token_relative_frequency_arr_l,
                y -> array_contains(
                    array_intersect(
                        list_transform(token_relative_frequency_arr_l, x -> x.token),
                        list_transform(token_relative_frequency_arr_r, x -> x.token)
                    ),
                    y.token
                )
            ),
            x -> x.relative_frequency
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
                    token_relative_frequency_arr_l,
                        y -> not array_contains(
                                list_transform(token_relative_frequency_arr_r, x -> x.token),
                                y.token
                            )
                ),
                array_filter(
                    token_relative_frequency_arr_r,
                        y -> not array_contains(
                                list_transform(token_relative_frequency_arr_l, x -> x.token),
                                y.token
                            )
                )
            ),

            x -> x.relative_frequency
        )
    ),
    (p, q) -> p / q^0.25
)
"""

token_relative_frequency_arr = {
    "output_column_name": "token_relative_frequency_arr",
    "comparison_levels": [
        cll.null_level("token_relative_frequency_arr"),
        {
            "sql_condition": f"{calc_tf_sql} < 1e-10",
            "label_for_charts": "<1e-10",
        },
        {
            "sql_condition": f"{calc_tf_sql} < 1e-8",
            "label_for_charts": "<1e-8",
        },
        {
            "sql_condition": f"{calc_tf_sql} < 1e-6",
            "label_for_charts": "<1e-6",
        },
        {
            "sql_condition": f"{calc_tf_sql} < 1e-4",
            "label_for_charts": "<1e-4",
        },
        {
            "sql_condition": f"{calc_tf_sql} < 1e-2",
            "label_for_charts": "<1e-2",
        },
        {
            "sql_condition": f"{calc_tf_sql} < 1",
            "label_for_charts": "<1e-2",
        },
        {"sql_condition": "ELSE", "label_for_charts": "All other comparisons"},
    ],
    "comparison_description": "Exact match vs. Col within levenshtein thresholds 1, 2 vs. anything else",
}

other_tokens_comparison = {
    "output_column_name": "common_tokens",
    "comparison_levels": [
        {
            "sql_condition": '"common_tokens_l" IS NULL OR "common_tokens_r" IS NULL or length("common_tokens_l") = 0 or length("common_tokens_r") = 0',
            "label_for_charts": "Null",
            "is_null_level": True,
        },
        {
            "sql_condition": '2*len(list_intersect("common_tokens_l", "common_tokens_r")) - len(list_distinct(list_concat("common_tokens_l", "common_tokens_r"))) > 0',
            "label_for_charts": "More matching tokens than non matching",
        },
        {"sql_condition": "ELSE", "label_for_charts": "All other comparisons"},
    ],
    "comparison_description": "Array intersection at minimum sizes 3, 1 vs. anything else",
}

settings = {
    "probability_two_random_records_match": 0.01,
    "link_type": "link_only",
    "blocking_rules_to_generate_predictions": [
        block_on(["postcode"], salting_partitions=10),
    ],
    "comparisons": [
        num_1_comparison,
        num_2_comparison,
        num_3_comparison,
        token_relative_frequency_arr,
        other_tokens_comparison,
    ],
    "retain_intermediate_calculation_columns": True,
    "source_dataset_column_name": "source_dataset",
}


linker = DuckDBLinker([df_pp, df_epc], settings)


linker.estimate_u_using_random_sampling(max_pairs=1e6)
# linker.estimate_parameters_using_expectation_maximisation(block_on("postcode"))
display(linker.match_weights_chart())

comparisons = linker._settings_obj.comparisons


cl.array_intersect_at_sizes("common_tokens", [3, 1]).as_dict()


df_predict = linker.predict()

sql = """
select unique_id from df_epc
where numeric_token_2 is not null
"""
unique_ids = list(duckdb.sql(sql).df()["unique_id"])

# counter = 0
# uid = "1096552219302014022515255225042148"
# counter += 1
# uid = unique_ids[counter]

uid = random.choice(unique_ids)

sql = f"""
select *
from {df_predict.physical_name}
where
unique_id_l = '{uid}'
order by match_weight desc

"""

record_pairs = linker.query_sql(sql)
epc_uid = record_pairs.head(1)["unique_id_l"].values[0]
pp_uid = record_pairs.head(1)["unique_id_r"].values[0]


sql_get_epc = f"""
select
'epc' as source_dataset, LMK_KEY, address1, address2, address3, POSTTOWN,  postcode
from read_parquet('./raw_address_data/adur_epc.parquet')
where LMK_KEY = '{epc_uid}'

"""
display(duckdb.sql(sql_get_epc).df())


sql_get_pp = f"""
select 'price_paid' as source_dataset, transaction_unique_identifier, saon, paon,
    street, locality, town_city,  postcode
from read_parquet('./raw_address_data/price_paid_addresses.parquet')
where transaction_unique_identifier = '{pp_uid}'
"""
display(duckdb.sql(sql_get_pp).df())


sql_get_epc_2 = f"""
select
numeric_token_1, numeric_token_2,	numeric_token_3, address_concat,
from df_epc
where unique_id = '{epc_uid}'
"""
display(duckdb.sql(sql_get_epc_2).df())


sql_get_pp_2 = f"""
select
numeric_token_1,	numeric_token_2,	numeric_token_3, address_concat,
from df_pp
where unique_id = '{pp_uid}'
"""
display(duckdb.sql(sql_get_pp_2).df())


linker.waterfall_chart(record_pairs.to_dict(orient="records"))
