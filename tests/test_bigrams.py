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
    con = duckdb.connect()

    messy_address = "9 A B C D"

    canonical_data = [
        {
            "address": "9 A C D",
            "assertions": {
                "overlapping_tokens_this_l_and_r": [
                    ("A", 2),  # Token A appears in messy and two candidates
                    ("9", 2),  # Token 9 appears in messy and two candidates
                    (
                        "D",
                        None,
                    ),  # D should not be in overlapping tokens (it's common end token)
                ],
                "overlapping_bigrams_this_l_and_r_filtered": [
                    (("9", "A"), 1),  # Bigram "9 A" is ONLY in this candidate
                ],
            },
        },
        {
            "address": "9 B C D",
            "assertions": {
                "overlapping_bigrams_this_l_and_r_filtered": [
                    (("B", "C"), 1),  # Bigram "B C" is ONLY in this candidate
                ],
            },
        },
        {
            "address": "8 B A C D Z",
            "assertions": {
                "bigrams_elsewhere_in_block_but_not_this_filtered": [
                    (("9", "A"), 1),  # Bigram "9 A" is missing from this candidate
                ],
            },
        },
    ]

    # Extract canonical addresses for test data generation
    canonical_addresses = [item["address"] for item in canonical_data]

    # Generate test data
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

    # Perform assertions for each canonical address with expected outcomes
    for i, item in enumerate(canonical_data, start=1):
        unique_id_l = f"l{i}"
        assertions = item["assertions"]
        if not assertions:
            continue  # Skip if no assertions are defined

        # Fetch relevant columns for this unique_id_l using full column names
        sql = f"""
        select overlapping_tokens_this_l_and_r,
               overlapping_bigrams_this_l_and_r_filtered,
               bigrams_elsewhere_in_block_but_not_this_filtered
        from df_improved
        where unique_id_l = '{unique_id_l}'
        """
        row = con.sql(sql).fetchone()
        overlap_tokens, overlap_bigrams_filtered, bigrams_elsewhere_filtered = row

        # Convert the fetched data into dictionaries
        overlap_tokens_dict = dict(overlap_tokens)
        overlap_bigrams_f_dict = duckdb_map_to_dict(overlap_bigrams_filtered)
        bigrams_elsewhere_f_dict = duckdb_map_to_dict(bigrams_elsewhere_filtered)

        # Check each assertion
        for column, checks in assertions.items():
            if column == "overlapping_tokens_this_l_and_r":
                map_dict = overlap_tokens_dict
            elif column == "overlapping_bigrams_this_l_and_r_filtered":
                map_dict = overlap_bigrams_f_dict
            elif column == "bigrams_elsewhere_in_block_but_not_this_filtered":
                map_dict = bigrams_elsewhere_f_dict
            else:
                continue  # Skip unrecognized columns

            for key, expected in checks:
                if expected is None:
                    assert key not in map_dict, (
                        f"{key} should not be in {column} for {unique_id_l}"
                    )
                else:
                    actual = map_dict.get(key)
                    assert actual == expected, (
                        f"{key} in {column} for {unique_id_l} should be {expected}, got {actual}"
                    )

    # Close the connection
    con.close()
