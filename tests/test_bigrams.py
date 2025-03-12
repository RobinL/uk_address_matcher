import pytest
import duckdb
import pandas as pd
from uk_address_matcher.post_linkage.identify_distinguishing_tokens import (
    improve_predictions_using_distinguishing_tokens,
)


def duckdb_map_to_dict(map_object):
    # Handle case where input might be a tuple containing a dictionary
    if isinstance(map_object, tuple) and len(map_object) == 1:
        map_object = map_object[0]

    # Convert each list in 'key' to a tuple so it can be used as a dictionary key
    return {tuple(k): v for k, v in zip(map_object["key"], map_object["value"])}


def test_improve_predictions_using_distinguishing_tokens():
    # Create a connection
    con = duckdb.connect()

    # Messy: 9 A B C
    # Canonical:
    # 9 A B D <- true match
    # 9 B C D
    # 8 A B C D

    # Analysis:
    # The token A is present in messy and two candidates
    # The token 9 is present in messy and two candidiates
    # The bigram 9 A is present in messy and one candidate
    # The bigram 9 A is missing from the two other candidates
    # C is not present in the true match, but is present in two other candidates
    # D is a common end token
    original_address_concat_r = "9 A B C D"
    data = [
        {
            "match_weight": 18.76543219876543,
            "match_probability": 0.9999978654321098,
            "source_dataset_l": "c_",
            "source_dataset_r": "m_",
            "unique_id_l": "l1",
            "unique_id_r": "r1",
            "original_address_concat_l": "9 A C D",
            "original_address_concat_r": original_address_concat_r,
            "common_end_tokens_r": [{"tok": "D", "rel_freq": 0.0004}],
            "postcode_l": "W1A",
            "postcode_r": "W1A",
        },
        {
            "match_weight": 19.12345678901234,
            "match_probability": 0.9999987654321098,
            "source_dataset_l": "c_",
            "source_dataset_r": "m_",
            "unique_id_l": "l2",
            "unique_id_r": "r1",
            "original_address_concat_l": "9 B C D",
            "original_address_concat_r": original_address_concat_r,
            "common_end_tokens_r": [{"tok": "D", "rel_freq": 0.0004}],
            "postcode_l": "W1A",
            "postcode_r": "W1A",
        },
        {
            "match_weight": 17.98765432109876,
            "match_probability": 0.9999965432109876,
            "source_dataset_l": "c_",
            "source_dataset_r": "m_",
            "unique_id_l": "l3",
            "unique_id_r": "r1",
            "original_address_concat_l": "8 B A C D Z",
            "original_address_concat_r": original_address_concat_r,
            "common_end_tokens_r": [{"tok": "D", "rel_freq": 0.0004}],
            "postcode_l": "W1A",
            "postcode_r": "W1A",
        },
    ]

    df = pd.DataFrame(data)
    df_ddb = con.sql("select * from df")

    # Apply the function to improve predictions
    df_improved = improve_predictions_using_distinguishing_tokens(
        df_predict=df_ddb,
        con=con,
        match_weight_threshold=-100,
        top_n_matches=5,
        use_bigrams=True,
    )

    # Display results for debugging
    sql = """
    select
    unique_id_r,
    unique_id_l,
    concat_ws(' ', original_address_concat_l, postcode_l) as address_concat_l,
    concat_ws(' ', original_address_concat_r, postcode_r) as address_concat_r,
    overlapping_tokens_this_l_and_r,
    tokens_elsewhere_in_block_but_not_this,
    overlapping_bigrams_this_l_and_r,
    bigrams_elsewhere_in_block_but_not_this,
    overlapping_bigrams_this_l_and_r_filtered,
    bigrams_elsewhere_in_block_but_not_this_filtered,
    missing_tokens
    from df_improved
    order by unique_id_r, unique_id_l
    """
    # Uncomment to show results during test run
    # ┌─────────────┬─────────────┬──────────────────┬──────────────────┬─────────────────────────────────┬────────────────────────────────────────┬──────────────────────────────────┬─────────────────────────────────────────┬───────────────────────────────────────────┬──────────────────────────────────────────────────┬────────────────┐
    # │ unique_id_r │ unique_id_l │ address_concat_l │ address_concat_r │ overlapping_tokens_this_l_and_r │ tokens_elsewhere_in_block_but_not_this │ overlapping_bigrams_this_l_and_r │ bigrams_elsewhere_in_block_but_not_this │ overlapping_bigrams_this_l_and_r_filtered │ bigrams_elsewhere_in_block_but_not_this_filtered │ missing_tokens │
    # │   varchar   │   varchar   │     varchar      │     varchar      │      map(varchar, ubigint)      │         map(varchar, ubigint)          │     map(varchar[], ubigint)      │         map(varchar[], ubigint)         │          map(varchar[], ubigint)          │             map(varchar[], ubigint)              │   varchar[]    │
    # ├─────────────┼─────────────┼──────────────────┼──────────────────┼─────────────────────────────────┼────────────────────────────────────────┼──────────────────────────────────┼─────────────────────────────────────────┼───────────────────────────────────────────┼──────────────────────────────────────────────────┼────────────────┤
    # │ r1          │ l1          │ 9 A C D W1A      │ 9 A B C D W1A    │ {9=2, A=2, C=3, W1A=3}          │ {B=2}                                  │ {[9, A]=1}                       │ {[B, C]=1}                              │ {[9, A]=1}                                │ {[B, C]=1}                                       │ [D]            │
    # │ r1          │ l2          │ 9 B C D W1A      │ 9 A B C D W1A    │ {9=2, B=2, C=3, W1A=3}          │ {A=2}                                  │ {[B, C]=1}                       │ {[9, A]=1}                              │ {[B, C]=1}                                │ {[9, A]=1}                                       │ [D]            │
    # │ r1          │ l3          │ 8 B A C D Z W1A  │ 9 A B C D W1A    │ {A=2, B=2, C=3, W1A=3}          │ {9=2}                                  │ {}                               │ {[9, A]=1, [B, C]=1}                    │ {}                                        │ {[9, A]=1, [B, C]=1}                             │ [8, D, Z]      │
    # └─────────────┴─────────────┴──────────────────┴──────────────────┴─────────────────────────────────┴────────────────────────────────────────┴──────────────────────────────────┴─────────────────────────────────────────┴───────────────────────────────────────────┴──────────────────────────────────────────────────┴────────────────┘

    # Tests

    # missing tokens are those observed in the messy address but never in the canonical
    sql = """
    select overlapping_tokens_this_l_and_r, overlapping_bigrams_this_l_and_r_filtered
    from df_improved
    where unique_id_r = 'r1'
    and unique_id_l = 'l1'
    """

    # Analysis:
    # The token A is present in messy and two candidates
    # The token 9 is present in messy and two candidiates
    # The bigram 9 A is present in messy and one candidate
    # The bigram 9 A is missing from the two other candidates
    # C is not present in the true match, but is present in two other candidates
    # D is a common end token

    overlapping_tokens_this_l_and_r, overlapping_bigrams_this_l_and_r_filtered = (
        con.sql(sql).fetchone()
    )

    # The token A is present in messy and two candidates
    assert overlapping_tokens_this_l_and_r["A"] == 2

    # The token 9 is not uniquely distinguishing
    assert overlapping_tokens_this_l_and_r["9"] == 2

    assert "D" not in overlapping_tokens_this_l_and_r

    overlapping_bigrams_as_dict = duckdb_map_to_dict(
        overlapping_bigrams_this_l_and_r_filtered
    )

    assert overlapping_bigrams_as_dict[("9", "A")] == 1

    sql = """
    select bigrams_elsewhere_in_block_but_not_this_filtered
    from df_improved
    where unique_id_r = 'r1'
    and unique_id_l = 'l3'
    """

    bigrams_elsewhere_in_block_but_not_this_filtered = con.sql(sql).fetchone()

    bi_as_dict = duckdb_map_to_dict(bigrams_elsewhere_in_block_but_not_this_filtered)

    assert bi_as_dict[("9", "A")] == 1
