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


def generate_test_data(
    messy_address: str, canonical_addresses: list[str]
) -> list[dict]:
    last_token = messy_address.split()[-1]
    common_end_tokens_r = [{"tok": last_token, "rel_freq": 0.0004}]
    data = []
    for i, canonical_address in enumerate(canonical_addresses, start=1):
        row = {
            "match_weight": 0.0,
            "match_probability": 0.5,
            "source_dataset_l": "c_",
            "source_dataset_r": "m_",
            "unique_id_l": f"l{i}",
            "unique_id_r": "r1",
            "original_address_concat_l": canonical_address,
            "original_address_concat_r": messy_address,
            "common_end_tokens_r": common_end_tokens_r,
            "postcode_l": "W1A",
            "postcode_r": "W1A",
        }
        data.append(row)
    return data


def test_improve_predictions_using_distinguishing_tokens():
    # Create a connection
    con = duckdb.connect()

    messy_address = "9 A B C D"
    canonical_addresses = ["9 A C D", "9 B C D", "8 B A C D Z"]
    # Analysis:
    # The token A is present in messy and two candidates
    # The token 9 is present in messy and two candidiates
    # The bigram 9 A is present in messy and one candidate
    # The bigram 9 A is missing from the two other candidates
    # C is not present in the true match, but is present in two other candidates
    # D is a common end token

    # Generate test data programmatically
    data = generate_test_data(messy_address, canonical_addresses)
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

    # Tests remain unchanged
    sql = """
    select overlapping_tokens_this_l_and_r, overlapping_bigrams_this_l_and_r_filtered
    from df_improved
    where unique_id_r = 'r1'
    and unique_id_l = 'l1'
    """
    overlapping_tokens_this_l_and_r, overlapping_bigrams_this_l_and_r_filtered = (
        con.sql(sql).fetchone()
    )

    assert (
        overlapping_tokens_this_l_and_r["A"] == 2
    )  # Token A in messy and two candidates
    assert (
        overlapping_tokens_this_l_and_r["9"] == 2
    )  # Token 9 not uniquely distinguishing
    assert "D" not in overlapping_tokens_this_l_and_r

    overlapping_bigrams_as_dict = duckdb_map_to_dict(
        overlapping_bigrams_this_l_and_r_filtered
    )
    assert overlapping_bigrams_as_dict[("9", "A")] == 1  # Bigram "9 A" in one candidate

    sql = """
    select bigrams_elsewhere_in_block_but_not_this_filtered
    from df_improved
    where unique_id_r = 'r1'
    and unique_id_l = 'l3'
    """
    bigrams_elsewhere_in_block_but_not_this_filtered = con.sql(sql).fetchone()
    bi_as_dict = duckdb_map_to_dict(bigrams_elsewhere_in_block_but_not_this_filtered)
    assert bi_as_dict[("9", "A")] == 1  # Bigram "9 A" missing from this candidate
