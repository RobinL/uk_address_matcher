# This turns out to not be really a training script
# since we hard code all the values!

import splink.comparison_level_library as cll
import splink.comparison_library as cl
from .blocking import old_blocking_rules

from splink import block_on, SettingsCreator

toggle_u_probability_fix = True
toggle_m_probability_fix = True

original_address_concat_comparison = cl.ExactMatch(
    "original_address_concat",
).configure(u_probabilities=[1, 2], m_probabilities=[15, 1])


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

    # # Second part - divide by frequencies of non-matching tokens
    # non_matching_tokens = f"""
    # list_reduce(
    #     list_prepend(
    #         1.0,
    #         list_transform(
    #             list_concat(
    #                 array_filter(
    #                     {column_name}_l,
    #                     y -> NOT array_contains(
    #                             list_transform({column_name}_r, x -> x.tok),
    #                             y.tok
    #                         )
    #                 ),
    #                 array_filter(
    #                     {column_name}_r,
    #                     y -> NOT array_contains(
    #                             list_transform({column_name}_l, x -> x.tok),
    #                             y.tok
    #                         )
    #                 )
    #             ),
    #             x -> x.rel_freq
    #         )
    #     ),
    #     (p, q) -> p / q^{power}
    # )"""

    return f"{matching_tokens}"


num_1_comparison = {
    "output_column_name": "numeric_token_1",
    "comparison_levels": [
        cll.NullLevel("numeric_token_1"),
        {
            "sql_condition": '"numeric_token_1_l" = "numeric_token_1_r"',
            "label_for_charts": "Exact match",
            "m_probability": 0.95,
            "u_probability": 0.01,
            "tf_adjustment_column": "numeric_token_1",
            "tf_adjustment_weight": 0.5,
            "fix_m_probability": toggle_m_probability_fix,
            "fix_u_probability": toggle_u_probability_fix,
        },
        {
            "sql_condition": """
                        nullif(regexp_extract(numeric_token_1_l, '\\d+', 0), '')
                        = nullif(regexp_extract(numeric_token_1_r, '\\d+', 0), '')
                        """,
            "label_for_charts": "Exact match",
            "m_probability": 0.95,
            "u_probability": 0.01,
            "tf_adjustment_column": "numeric_token_1",
            "tf_adjustment_weight": 0.5,
            "fix_m_probability": toggle_m_probability_fix,
            "fix_u_probability": toggle_u_probability_fix,
        },
        {
            "sql_condition": "numeric_token_2_l = numeric_token_1_r or numeric_token_1_l = numeric_token_2_r",
            "label_for_charts": "Exact match inverted numbers",
            "m_probability": 4,
            "u_probability": 1,
            "fix_m_probability": toggle_m_probability_fix,
            "fix_u_probability": toggle_u_probability_fix,
        },
        {
            "sql_condition": '"numeric_token_1_l" IS NULL OR "numeric_token_1_r" IS NULL',
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
    "comparison_description": "numeric_token_1",
}

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
            "m_probability": 0.8,
            "u_probability": 0.001,
            "tf_adjustment_column": "numeric_token_2",
            "tf_adjustment_weight": 0.5,
            "fix_m_probability": toggle_m_probability_fix,
            "fix_u_probability": toggle_u_probability_fix,
        },
        {
            "sql_condition": "numeric_token_1_l = numeric_token_2_r OR numeric_token_1_r = numeric_token_2_l",
            "label_for_charts": "Exact match inverted numbers",
            "m_probability": 1,
            "u_probability": 1,
            "fix_m_probability": toggle_m_probability_fix,
            "fix_u_probability": toggle_u_probability_fix,
        },
        # One has a num 2 and the other does not
        {
            "sql_condition": '"numeric_token_2_l" IS NULL OR "numeric_token_2_r" IS NULL',
            "label_for_charts": "Null",
            "m_probability": 1,
            "u_probability": 16,
            "fix_m_probability": toggle_m_probability_fix,
            "fix_u_probability": toggle_u_probability_fix,
        },
        cll.ElseLevel().configure(
            m_probability=1,
            u_probability=256,
            fix_m_probability=True,
            fix_u_probability=True,
        ),
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

arr_red_sql = array_reduce_by_freq("token_rel_freq_arr", 0.33)


def generate_arr_reduce_data(start_exp, end_exp=2, step=-1):
    data = []
    anchor_exp = -12
    anchor_m_prob = 2048.0

    current_exp = start_exp
    while current_exp <= end_exp:
        if current_exp > 0:
            sql_cond = f"{arr_red_sql} < 1e{current_exp}"
            label = f" < 1e{current_exp}"
        else:
            sql_cond = f"{arr_red_sql} < 1e{current_exp}"
            label = f" < 1e{current_exp}"

        if current_exp > anchor_exp:
            # Above <1e-12: doubles every 4 steps (increases by 2^(1/4) per step)
            m_prob = anchor_m_prob * (2 ** ((anchor_exp - current_exp) / 1))
        else:
            # Below <1e-12: halves every 2 steps (decreases by 2^(-1/2) per step)
            m_prob = anchor_m_prob * (2 ** (-(current_exp - anchor_exp) / 4))

        data.append(
            {
                "sql_condition": sql_cond,
                "label_for_charts": label,
                "m_probability": m_prob,
                "u_probability": 1,
                "fix_m_probability": toggle_m_probability_fix,
                "fix_u_probability": toggle_u_probability_fix,
            }
        )

        current_exp += step

    return data


middle_conditions = generate_arr_reduce_data(-30, 4, 1)


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

settings_for_training = SettingsCreator(
    probability_two_random_records_match=3e-8,
    link_type="link_only",
    blocking_rules_to_generate_predictions=blocking_rules,
    comparisons=[
        original_address_concat_comparison,
        flat_positional_comparison,
        num_1_comparison,
        num_2_comparison,
        num_3_comparison,
        token_rel_freq_arr_comparison,
        common_end_tokens_comparison,
        postcode_comparison,
    ],
    retain_intermediate_calculation_columns=True,
)
