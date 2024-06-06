# Going to largely follow
# https://github.com/moj-analytical-services/splink/discussions/2022
import splink.duckdb.comparison_level_library as cll
import splink.duckdb.comparison_library as cl
from IPython.display import display
from splink.duckdb.blocking_rule_library import block_on
from splink.duckdb.linker import DuckDBLinker

from .arr_comparisons import array_reduce_by_freq


def train_splink_model(df_1, df_2):
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
                "label_for_charts": "Exact match inverted numbers",
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
                "label_for_charts": "Exact match inverted numbers",
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

    arr_red_sql = array_reduce_by_freq("token_rel_freq_arr", 0.0)

    common_end_tokens_comparison = {
        "output_column_name": "common_end_tokens",
        "comparison_levels": [
            {
                "sql_condition": '"common_end_tokens_l" IS NULL OR "common_end_tokens_r" IS NULL or length("common_end_tokens_l") = 0 or length("common_end_tokens_r") = 0',
                "label_for_charts": "Null",
                "is_null_level": True,
            },
            {
                "sql_condition": f"{arr_red_sql} < 1e-5",
                "label_for_charts": "<1e-10",
            },
            {
                "sql_condition": f"{arr_red_sql} < 1e-2",
                "label_for_charts": "<1e-8",
            },
            {"sql_condition": "ELSE", "label_for_charts": "All other comparisons"},
        ],
        "comparison_description": "Array intersection",
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
            token_rel_freq_arr_comparison,
            common_end_tokens_comparison,
            # This is not needed but makes a human readable form of the address appear
            # in the comparison viewer dashboard
            cl.exact_match("original_address_concat"),
        ],
        "retain_intermediate_calculation_columns": True,
        "source_dataset_column_name": "source_dataset",
        # "additional_columns_to_retain": ["original_address_concat"],
    }

    linker = DuckDBLinker([df_1, df_2], settings)
    # cl.exact_match("original_address_concat").as_dict()

    # Increase max_pairs to 1e7 or above for higher accuracy
    linker.estimate_u_using_random_sampling(max_pairs=1e6)
    # linker.estimate_parameters_using_expectation_maximisation(block_on("postcode"))

    comparisons = linker._settings_obj.comparisons

    # # Increase punishment for non-matching 'numeric' token
    comparisons[0].comparison_levels[3].m_probability = 0.001
    comparisons[0].comparison_levels[3].u_probability = 1.0

    # # Override the parameter estiamtes to null
    # #  to make sure the 'address concat' field has no effect on the model
    comparisons[5].comparison_levels[1].m_probability = 0.5
    comparisons[5].comparison_levels[1].u_probability = 0.5

    comparisons[5].comparison_levels[2].m_probability = 0.5
    comparisons[5].comparison_levels[2].u_probability = 0.5
    display(linker.match_weights_chart())

    return linker
