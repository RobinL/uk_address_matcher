# This turns out to not be really a training script
# since we hard code all the values!

import splink.comparison_level_library as cll
import splink.comparison_library as cl
from .blocking import old_blocking_rules
from splink.internals.misc import match_weight_to_bayes_factor

from splink import block_on, SettingsCreator

toggle_u_probability_fix = True
toggle_m_probability_fix = True

original_address_concat_comparison = cl.ExactMatch(
    "original_address_concat",
).configure(u_probabilities=[1, 2], m_probabilities=[15, 1])


def get_num_1_comparison(
    WEIGHT_1=6.57,
    WEIGHT_2=6.57,
    WEIGHT_3=2,
    WEIGHT_4=-4,
    WEIGHT_5=-8,
):
    num_1_comparison = {
        "output_column_name": "numeric_token_1",
        "comparison_levels": [
            cll.NullLevel("numeric_token_1"),
            {
                "sql_condition": '"numeric_token_1_l" = "numeric_token_1_r"',
                "label_for_charts": "Exact match",
                "m_probability": match_weight_to_bayes_factor(WEIGHT_1),
                "u_probability": 1,
                "tf_adjustment_column": "numeric_token_1",
                "tf_adjustment_weight": 0.1,
                "fix_m_probability": toggle_m_probability_fix,
                "fix_u_probability": toggle_u_probability_fix,
            },
            {
                "sql_condition": """
                            nullif(regexp_extract(numeric_token_1_l, '\\d+', 0), '')
                            = nullif(regexp_extract(numeric_token_1_r, '\\d+', 0), '')
                            """,
                "label_for_charts": "Exact match",
                "m_probability": match_weight_to_bayes_factor(WEIGHT_2),
                "u_probability": 1,
                "tf_adjustment_column": "numeric_token_1",
                "tf_adjustment_weight": 0.1,
                "fix_m_probability": toggle_m_probability_fix,
                "fix_u_probability": toggle_u_probability_fix,
            },
            {
                "sql_condition": "numeric_token_2_l = numeric_token_1_r or numeric_token_1_l = numeric_token_2_r",
                "label_for_charts": "Exact match inverted numbers",
                "m_probability": match_weight_to_bayes_factor(WEIGHT_3),
                "u_probability": 1,
                "fix_m_probability": toggle_m_probability_fix,
                "fix_u_probability": toggle_u_probability_fix,
            },
            {
                "sql_condition": '"numeric_token_1_l" IS NULL OR "numeric_token_1_r" IS NULL',
                "label_for_charts": "Null",
                "m_probability": match_weight_to_bayes_factor(WEIGHT_4),
                "u_probability": 1,
                "fix_m_probability": toggle_m_probability_fix,
                "fix_u_probability": toggle_u_probability_fix,
            },
            cll.ElseLevel().configure(
                m_probability=match_weight_to_bayes_factor(WEIGHT_5),
                u_probability=1,
                fix_m_probability=True,
                fix_u_probability=True,
            ),
        ],
        "comparison_description": "numeric_token_1",
    }
    return num_1_comparison


def get_num_2_comparison(
    WEIGHT_1=6.57,
    WEIGHT_2=0,
    WEIGHT_3=-2,
    WEIGHT_4=-4,
):
    num_2_comparison = {
        "output_column_name": "numeric_token_2",
        "comparison_levels": [
            # Note and
            {
                "sql_condition": '"numeric_token_2_l" IS NULL AND "numeric_token_2_r" IS NULL',
                "label_for_charts": "Null",
                "is_null_level": True,
            },
            {
                "sql_condition": '"numeric_token_2_l" = "numeric_token_2_r"',
                "label_for_charts": "Exact match",
                "m_probability": match_weight_to_bayes_factor(WEIGHT_1),
                "u_probability": 1,
                "tf_adjustment_column": "numeric_token_2",
                "tf_adjustment_weight": 0.1,
                "fix_m_probability": toggle_m_probability_fix,
                "fix_u_probability": toggle_u_probability_fix,
            },
            {
                "sql_condition": "numeric_token_1_l = numeric_token_2_r OR numeric_token_1_r = numeric_token_2_l",
                "label_for_charts": "Exact match inverted numbers",
                "m_probability": match_weight_to_bayes_factor(WEIGHT_2),
                "u_probability": 1,
                "fix_m_probability": toggle_m_probability_fix,
                "fix_u_probability": toggle_u_probability_fix,
            },
            # One has a num 2 and the other does not
            {
                "sql_condition": '"numeric_token_2_l" IS NULL OR "numeric_token_2_r" IS NULL',
                "label_for_charts": "Null",
                "m_probability": match_weight_to_bayes_factor(WEIGHT_3),
                "u_probability": 1,
                "fix_m_probability": toggle_m_probability_fix,
                "fix_u_probability": toggle_u_probability_fix,
            },
            cll.ElseLevel().configure(
                m_probability=match_weight_to_bayes_factor(WEIGHT_4),
                u_probability=1,
                fix_m_probability=True,
                fix_u_probability=True,
            ),
        ],
        "comparison_description": "numeric_token_2",
    }
    return num_2_comparison


original_address_concat_comparison = cl.ExactMatch(
    "original_address_concat",
).configure(u_probabilities=[1, 2], m_probabilities=[15, 1])


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
            "m_probability": 0.6,
            "u_probability": 0.0001,
            "tf_adjustment_column": "numeric_token_3",
            "tf_adjustment_weight": 0.5,
        },
        {
            "sql_condition": '"numeric_token_2_l" = "numeric_token_3_r"',
            "label_for_charts": "Exact match inverted",
            "m_probability": 0.3,
            "u_probability": 0.0025,
            "tf_adjustment_column": "numeric_token_3",
            "tf_adjustment_weight": 0.5,
        },
        # One has a num 3 and the other does not
        {
            "sql_condition": '"numeric_token_3_l" IS NULL OR "numeric_token_3_r" IS NULL',
            "label_for_charts": "Null",
            "m_probability": 1,
            "u_probability": 16,
        },
        cll.ElseLevel().configure(
            m_probability=1,
            u_probability=256,
            fix_m_probability=True,
            fix_u_probability=True,
        ),
    ],
    "comparison_description": "numeric_token_3",
}


def array_reduce_by_freq(column_name: str, power: float) -> str:
    """Generate SQL for reducing arrays by frequency.

    Args:
        column_name: Name of the column containing arrays to compare
        power: Power to raise the denominator to in the second reduction

    Returns:
        SQL string for comparing arrays by frequency
    """
    # First part - multiply frequencies of matching tokens
    matching_tokens = f"""
    list_reduce(
        list_prepend(
            1.0,
            list_transform(
                {column_name}_l,
                x -> CASE
                        WHEN array_contains(
                            list_transform({column_name}_r, y -> y.tok),
                            x.tok
                        )
                        THEN x.rel_freq
                        ELSE 1.0
                    END
            )
        ),
        (p, q) -> p * q
    )"""

    # Second part - divide by frequencies of non-matching tokens
    non_matching_tokens = f"""
    list_reduce(
        list_prepend(
            1.0,
            list_transform(
                list_concat(
                    array_filter(
                        {column_name}_l,
                        y -> NOT array_contains(
                                list_transform({column_name}_r, x -> x.tok),
                                y.tok
                            )
                    ),
                    array_filter(
                        {column_name}_r,
                        y -> NOT array_contains(
                                list_transform({column_name}_l, x -> x.tok),
                                y.tok
                            )
                    )
                ),
                x -> x.rel_freq
            )
        ),
        (p, q) -> p / q^{power}
    )"""

    return f"{matching_tokens} * {non_matching_tokens}"


def generate_arr_reduce_data(
    start_exp=4,
    start_weight=-4,
    segments=[8, 8, 8, 10],
    delta_weights_within_segments=[1, 1, 0.25, 0.25],
    punishment_multiplier=0.33,
):
    data = []
    current_exp = start_exp
    current_weight = start_weight

    for segment, delta_weight in zip(segments, delta_weights_within_segments):
        arr_red_sql = array_reduce_by_freq("token_rel_freq_arr", punishment_multiplier)
        for _ in range(segment):
            if current_exp > 0:
                sql_cond = f"{arr_red_sql} < 1e{current_exp}"
                label = f" < 1e{current_exp}"
            else:
                sql_cond = f"{arr_red_sql} < 1e{current_exp}"
                label = f" < 1e{current_exp}"

            level = {
                "sql_condition": sql_cond,
                "label_for_charts": label,
                "m_probability": match_weight_to_bayes_factor(current_weight),
                "u_probability": 1,
                "fix_m_probability": toggle_m_probability_fix,
                "fix_u_probability": toggle_u_probability_fix,
            }
            data.append(level)
            current_weight += delta_weight
            current_exp -= 1

    return data[::-1]


def get_token_rel_freq_arr_comparison(
    START_EXP=4,
    START_WEIGHT=-4,
    SEGMENTS=[8, 8, 8, 10],
    DELTA_WEIGHTS_WITHIN_SEGMENTS=[1, 1, 0.25, 0.25],
    PUNISHMENT_MULTIPLIER=0.33,
):
    middle_conditions = generate_arr_reduce_data(
        START_EXP,
        START_WEIGHT,
        SEGMENTS,
        DELTA_WEIGHTS_WITHIN_SEGMENTS,
        PUNISHMENT_MULTIPLIER,
    )

    token_rel_freq_arr_comparison = {
        "output_column_name": "token_rel_freq_arr",
        "comparison_levels": [
            {
                "sql_condition": '"token_rel_freq_arr_l" IS NULL OR "token_rel_freq_arr_r" IS NULL or length("token_rel_freq_arr_l") = 0 or length("token_rel_freq_arr_r") = 0',
                "label_for_charts": "Null",
                "is_null_level": True,
            },
            *middle_conditions,
            {
                "sql_condition": "ELSE",
                "label_for_charts": "All other comparisons",
                "m_probability": 1,
                "u_probability": 256,
                "fix_m_probability": toggle_m_probability_fix,
                "fix_u_probability": toggle_u_probability_fix,
            },
        ],
        "comparison_description": "Token relative frequency array",
    }

    return token_rel_freq_arr_comparison


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
            "m_probability": 4,
            "u_probability": 1,
            "fix_m_probability": toggle_m_probability_fix,
            "fix_u_probability": toggle_u_probability_fix,
        },
        {
            "sql_condition": "ELSE",
            "label_for_charts": "All other comparisons",
            "m_probability": 1,
            "u_probability": 1.5,
            "fix_m_probability": toggle_m_probability_fix,
            "fix_u_probability": toggle_u_probability_fix,
        },
    ],
    "comparison_description": "Array intersection",
}


postcode_comparison = {
    "output_column_name": "postcode",
    "comparison_levels": [
        {
            "sql_condition": '"postcode_l" IS NULL AND "postcode_r" IS NULL',
            "label_for_charts": "Null",
            "is_null_level": True,
        },
        {
            "sql_condition": "postcode_l = postcode_r",
            "label_for_charts": "Exact",
            "m_probability": 3e6,
            "u_probability": 1,
            "fix_m_probability": toggle_m_probability_fix,
            "fix_u_probability": toggle_u_probability_fix,
        },
        {
            "sql_condition": "levenshtein(postcode_l, postcode_r) <= 1",
            "label_for_charts": "Lev <= 1",
            "m_probability": 10000,
            "u_probability": 1,
            "fix_m_probability": toggle_m_probability_fix,
            "fix_u_probability": toggle_u_probability_fix,
        },
        {
            "sql_condition": "levenshtein(postcode_l, postcode_r) <= 2",
            "label_for_charts": "Lev <=2",
            "m_probability": 5000,
            "u_probability": 1,
            "fix_m_probability": toggle_m_probability_fix,
            "fix_u_probability": toggle_u_probability_fix,
        },
        {
            "sql_condition": "split_part(postcode_l, ' ', 1) = split_part(postcode_r, ' ', 1)",
            "label_for_charts": "District",
            "m_probability": 3000,
            "u_probability": 1,
            "fix_m_probability": toggle_m_probability_fix,
            "fix_u_probability": toggle_u_probability_fix,
        },
        {
            "sql_condition": "split_part(postcode_l, ' ', 2) = split_part(postcode_r, ' ', 2)",
            "label_for_charts": "Unit not District",
            "m_probability": 2000,
            "u_probability": 1,
            "fix_m_probability": toggle_m_probability_fix,
            "fix_u_probability": toggle_u_probability_fix,
        },
        {
            "sql_condition": "ELSE",
            "label_for_charts": "All other comparisons",
            "m_probability": 1,
            "u_probability": 64,
            "fix_m_probability": toggle_m_probability_fix,
            "fix_u_probability": toggle_u_probability_fix,
        },
    ],
}

flat_positional_comparison = {
    "output_column_name": "flat_positional",
    "comparison_levels": [
        # Note AND not OR
        {
            "sql_condition": '"flat_positional_l" IS NULL AND "flat_positional_r" IS NULL AND "flat_letter_l" IS NULL AND "flat_letter_r" IS NULL',
            "label_for_charts": "Null",
            "is_null_level": True,
        },
        {
            "sql_condition": "flat_positional_l = flat_positional_r",
            "label_for_charts": "Exact match",
            "m_probability": 0.95,
            "u_probability": 0.01,
            "fix_m_probability": toggle_m_probability_fix,
            "fix_u_probability": toggle_u_probability_fix,
        },
        {
            "sql_condition": "flat_letter_l = flat_letter_r",
            "label_for_charts": "Exact match",
            "m_probability": 0.95,
            "u_probability": 0.01,
            "fix_m_probability": toggle_m_probability_fix,
            "fix_u_probability": toggle_u_probability_fix,
        },
        {
            "sql_condition": "flat_letter_l = numeric_token_1_r OR flat_letter_r = numeric_token_1_l",
            "label_for_charts": "Exact match inverted numbers",
            "m_probability": 1,
            "u_probability": 1,
            "fix_m_probability": toggle_m_probability_fix,
            "fix_u_probability": toggle_u_probability_fix,
        },
        {
            "sql_condition": """
            (flat_positional_l IS NOT NULL and flat_positional_r IS NULL and flat_letter_r IS NOT NULL)
            or
            (flat_positional_r IS NOT NULL and flat_positional_l IS NULL and flat_letter_l IS NOT NULL)
            """,
            "label_for_charts": "Exact match",
            "m_probability": 1,
            "u_probability": 1,
            "fix_m_probability": toggle_m_probability_fix,
            "fix_u_probability": toggle_u_probability_fix,
        },
        {
            "sql_condition": "ELSE",
            "label_for_charts": "All other comparisons",
            "m_probability": 1,
            "u_probability": 32,
            "fix_m_probability": toggle_m_probability_fix,
            "fix_u_probability": toggle_u_probability_fix,
        },
    ],
    "comparison_description": "Flat position comparison",
}

blocking_rules = old_blocking_rules + [block_on("postcode")]


def get_settings_for_training(
    num_1_weights=None,
    num_2_weights=None,
    token_rel_freq_arr_comparison=None,
):
    num_1_weights = num_1_weights or {}
    num_2_weights = num_2_weights or {}
    token_rel_freq_arr_comparison = token_rel_freq_arr_comparison or {}
    settings_for_training = SettingsCreator(
        probability_two_random_records_match=3e-8,
        link_type="link_only",
        blocking_rules_to_generate_predictions=blocking_rules,
        comparisons=[
            original_address_concat_comparison,
            flat_positional_comparison,
            get_num_1_comparison(**num_1_weights),
            get_num_2_comparison(**num_2_weights),
            num_3_comparison,
            get_token_rel_freq_arr_comparison(**token_rel_freq_arr_comparison),
            common_end_tokens_comparison,
            postcode_comparison,
        ],
        retain_intermediate_calculation_columns=True,
    )
    return settings_for_training
