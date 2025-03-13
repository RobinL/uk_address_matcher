import pytest
import duckdb
import pandas as pd
from uk_address_matcher.post_linkage.identify_distinguishing_tokens import (
    improve_predictions_using_distinguishing_tokens,
)

### Helper Functions


def duckdb_map_to_dict(map_object):
    """Convert DuckDB map object to a Python dictionary."""
    if isinstance(map_object, tuple) and len(map_object) == 1:
        map_object = map_object[0]
    return {tuple(k): v for k, v in zip(map_object["key"], map_object["value"])}


def generate_test_data(
    messy_address: str,
    canonical_addresses: list[str],
    common_end_token: str | None = None,
) -> list[dict]:
    """Generate test data for a messy address and its canonical counterparts."""
    # Set common_end_tokens_r based on whether a common_end_token is provided
    common_end_tokens_r = (
        [{"tok": common_end_token, "rel_freq": 0.0004}]
        if common_end_token is not None
        else []
    )
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


def run_assertions(
    messy_address: str,
    canonical_data: list[dict],
    common_end_token: str | None = None,
    show: bool = False,
) -> None:
    """Run assertions for a messy address against its canonical data."""
    con = duckdb.connect()
    try:
        # Extract canonical addresses from the input data
        canonical_addresses = [item["address"] for item in canonical_data]
        # Generate test data and create a DuckDB table
        data = generate_test_data(messy_address, canonical_addresses, common_end_token)
        df = pd.DataFrame(data)
        df_ddb = con.sql("select * from df")
        if show:
            df_ddb.show(max_width=10000)
        # Improve predictions using the distinguishing tokens function
        df_improved = improve_predictions_using_distinguishing_tokens(
            df_predict=df_ddb,
            con=con,
            match_weight_threshold=-100,
            top_n_matches=5,
            use_bigrams=True,
        )
        if show:
            df_improved.show(max_width=10000)

        # Iterate over each canonical entry
        for i, item in enumerate(canonical_data, start=1):
            unique_id_l = f"l{i}"
            assertions = item["assertions"]
            if not assertions:
                continue

            # SQL query including all relevant columns
            sql = f"""
            select overlapping_tokens_this_l_and_r,
                   tokens_elsewhere_in_block_but_not_this,
                   overlapping_bigrams_this_l_and_r_filtered,
                   bigrams_elsewhere_in_block_but_not_this_filtered,
                   missing_tokens
            from df_improved
            where unique_id_l = '{unique_id_l}'
            """
            row = con.sql(sql).fetchone()

            # Unpack the row into five variables
            (
                overlap_tokens,
                tokens_elsewhere,
                overlap_bigrams_filtered,
                bigrams_elsewhere_filtered,
                missing_tokens_list,
            ) = row

            # Convert map-type columns to dictionaries
            overlap_tokens_dict = dict(overlap_tokens)
            tokens_elsewhere_dict = dict(tokens_elsewhere)
            overlap_bigrams_f_dict = duckdb_map_to_dict(overlap_bigrams_filtered)
            bigrams_elsewhere_f_dict = duckdb_map_to_dict(bigrams_elsewhere_filtered)
            # missing_tokens_list is already a list, no conversion needed

            # Assertion loop with conditions for all testable columns
            for column, checks in assertions.items():
                if column == "overlapping_tokens_this_l_and_r":
                    map_dict = overlap_tokens_dict
                elif column == "tokens_elsewhere_in_block_but_not_this":
                    map_dict = tokens_elsewhere_dict
                elif column == "overlapping_bigrams_this_l_and_r_filtered":
                    map_dict = overlap_bigrams_f_dict
                elif column == "bigrams_elsewhere_in_block_but_not_this_filtered":
                    map_dict = bigrams_elsewhere_f_dict
                elif column == "missing_tokens":
                    # For list-type column, compare the actual list with the expected list
                    actual_list = missing_tokens_list
                    expected_list = checks
                    assert actual_list == expected_list, (
                        f"missing_tokens for {unique_id_l} should be {expected_list}, "
                        f"got {actual_list}"
                    )
                    continue  # Skip the map-specific loop below
                else:
                    continue

                # For map-type columns, iterate over key-value pairs
                for key, expected in checks:
                    if expected is None:
                        assert key not in map_dict, (
                            f"{key} should not be in {column} for {unique_id_l}"
                        )
                    else:
                        actual = map_dict.get(key)
                        assert actual == expected, (
                            f"{key} in {column} for {unique_id_l} should be {expected}, "
                            f"got {actual}"
                        )
    finally:
        con.close()


### Test Functions


def test_scenario_one():
    messy_address = "10 X Y Z"
    canonical_data = [
        {
            "address": "10 X Y Z",
            "assertions": {
                "overlapping_tokens_this_l_and_r": [
                    ("10", 1),  # Token 10 appears in messy and this candidate
                ],
                "overlapping_bigrams_this_l_and_r_filtered": [
                    (("10", "X"), 1),  # Bigram "10 X" is unique to this candidate
                ],
            },
        },
        {
            "address": "9 X Y Z",
            "assertions": {
                "tokens_elsewhere_in_block_but_not_this": [
                    ("10", 1),
                ],
            },
        },
    ]
    run_assertions(messy_address, canonical_data, common_end_token="D")


def test_scenario_two():
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
                    ),  # D should not be in overlapping tokens (common end token)
                ],
                "overlapping_bigrams_this_l_and_r_filtered": [
                    (("9", "A"), 1),  # Bigram "9 A" is unique to this candidate
                ],
            },
        },
        {
            "address": "9 B C D",
            "assertions": {
                "overlapping_bigrams_this_l_and_r_filtered": [
                    (("B", "C"), 1),  # Bigram "B C" is unique to this candidate
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
    run_assertions(messy_address, canonical_data, common_end_token="D")


def test_scenario_three():
    """
    missing tokens are tokens that we see in the candidate address
    but are not in the messy address and hence should be punished
    """
    messy_address = "1 HIGH STREET BOVINGDON"
    canonical_data = [
        {
            "address": "1 HIGH STREET BOVINGDON",
            "assertions": {
                "overlapping_tokens_this_l_and_r": [
                    ("HIGH", 2),
                    ("STREET", 2),
                ]
            },
        },
        {
            "address": "THE ANNEXE 1 HIGH STREET BOVINGDON",
            "assertions": {
                "missing_tokens": [
                    "THE",
                    "ANNEXE",
                ],
            },
        },
    ]
    run_assertions(messy_address, canonical_data)
