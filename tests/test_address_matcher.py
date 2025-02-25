import duckdb
from pathlib import Path

from uk_address_matcher import clean_data_using_precomputed_rel_tok_freq, get_linker
from uk_address_matcher.post_linkage.identify_distinguishing_tokens import (
    improve_predictions_using_distinguishing_tokens,
)

from tests.utils import prepare_combined_test_data
from splink import block_on


def run_matcher_workflow(df_messy, df_canonical, con=None):
    """
    Run the complete address matching workflow.

    Args:
        df_messy: DuckDB relation with messy addresses
        df_canonical: DuckDB relation with canonical addresses
        con: Optional DuckDB connection


    Returns:
        DuckDB relation: Result of the matching process
    """
    if con is None:
        con = duckdb.connect(database=":memory:")

    # Step 1: Clean the data
    df_messy_clean = clean_data_using_precomputed_rel_tok_freq(df_messy, con=con)
    df_canonical_clean = clean_data_using_precomputed_rel_tok_freq(
        df_canonical, con=con
    )

    # Step 2: Set up and run the linker (first pass)

    # Determine which additional columns to retain
    additional_columns = ["original_address_concat", "true_match_id"]

    linker = get_linker(
        df_addresses_to_match=df_messy_clean,
        df_addresses_to_search_within=df_canonical_clean,
        con=con,
        include_full_postcode_block=True,
        additional_columns_to_retain=additional_columns,
    )

    linker._settings_obj._blocking_rules_to_generate_predictions = [
        block_on("test_block").get_blocking_rule("duckdb")
    ]

    df_predict = linker.inference.predict(
        threshold_match_weight=-50, experimental_optimisation=True
    )
    df_predict_ddb = df_predict.as_duckdbpyrelation()

    # Step 3: Improve predictions using distinguishing tokens (second pass)
    df_predict_improved = improve_predictions_using_distinguishing_tokens(  # noqa: F841
        df_predict=df_predict_ddb,
        con=con,
        match_weight_threshold=-20,
    )

    sql = """
    select p.*, c.true_match_id
    from df_predict_improved p
    left join df_messy_clean c
    on p.unique_id_r = c.unique_id
    """
    df_predict_improved_with_true_match_id = con.sql(sql)

    return df_predict_improved_with_true_match_id


def test_address_matching_combined():
    """Test that address matching correctly identifies the expected matches (combined approach)"""
    con = duckdb.connect(database=":memory:")

    # Get combined test data from YAML file
    yaml_path = Path(__file__).parent / "test_addresses.yaml"
    df_messy_combined, df_canonical_combined = prepare_combined_test_data(
        yaml_path, con
    )

    # Run the matching workflow on the combined data
    # Use test_block for blocking to ensure we only compare within test blocks
    results = run_matcher_workflow(df_messy_combined, df_canonical_combined, con)

    # Register the results for querying
    con.register("results", results)

    # Get all test blocks with their expected and actual matches
    matches_query = """
    WITH top_matches AS (
        SELECT
            unique_id_r AS test_block_id,
            true_match_id AS expected_match_id,
            unique_id_l AS actual_match_id,
            match_weight
        FROM results
        QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id_r ORDER BY match_weight DESC) = 1
    )
    SELECT * FROM top_matches
    """
    matches = con.execute(matches_query).fetchall()

    # Calculate match rate
    total_cases = len(matches)
    correct_matches = sum(1 for m in matches if m[1] == m[2])
    match_rate = (correct_matches / total_cases) * 100 if total_cases > 0 else 0

    # Create a results dictionary
    test_results = {
        "total_cases": total_cases,
        "correct_matches": correct_matches,
        "match_rate": match_rate,
        "mismatches": [],
    }

    # Collect details of mismatches
    if correct_matches < total_cases:
        for match in matches:
            test_block_id, expected_match_id, actual_match_id, match_weight = match

            if expected_match_id == actual_match_id:
                continue

            mismatch_query = """
            WITH
            messy_record AS (
                SELECT
                    'Messy Record' as record_type,
                    m.address_concat as address,
                    m.postcode as postcode,
                    NULL as match_weight
                FROM df_messy_combined m
                WHERE m.unique_id = ?
            ),
            true_match AS (
                SELECT
                    'True Match' as record_type,
                    c.address_concat as address,
                    c.postcode as postcode,
                    (SELECT match_weight FROM results WHERE unique_id_r = ? AND unique_id_l = ?) as match_weight
                FROM df_canonical_combined c
                WHERE c.unique_id = ?
            ),
            false_match AS (
                SELECT
                    'False Match' as record_type,
                    c.address_concat as address,
                    c.postcode as postcode,
                    ? as match_weight
                FROM df_canonical_combined c
                WHERE c.unique_id = ?
            )

            SELECT * FROM messy_record
            UNION ALL
            SELECT * FROM true_match
            UNION ALL
            SELECT * FROM false_match
            ORDER BY record_type
            """

            mismatch_details = con.execute(
                mismatch_query,
                [
                    test_block_id,
                    test_block_id,
                    expected_match_id,
                    expected_match_id,
                    match_weight,
                    actual_match_id,
                ],
            ).fetchall()

            # Structure the mismatch details
            mismatch_data = {"test_block_id": test_block_id, "records": []}

            for detail in mismatch_details:
                record_type, address, postcode, weight = detail
                mismatch_data["records"].append(
                    {
                        "record_type": record_type,
                        "address": address,
                        "postcode": postcode,
                        "match_weight": weight,
                    }
                )

            test_results["mismatches"].append(mismatch_data)

    # Print results for local testing purposes
    print("\nAddress Matching Results:")
    print(f"Total test cases: {total_cases}")
    print(f"Correct matches: {correct_matches}")
    print(f"Match rate: {match_rate:.2f}%\n")

    if test_results["mismatches"]:
        print("Details of mismatches:")
        print("-" * 80)

        for mismatch in test_results["mismatches"]:
            print(f"Test Block ID: {mismatch['test_block_id']}")
            print(
                f"{'Record Type':<15} {'Address':<60} {'Postcode':<10} {'Match Weight'}"
            )
            print("-" * 100)

            for record in mismatch["records"]:
                weight_str = (
                    f"{record['match_weight']:.2f}"
                    if record["match_weight"] is not None
                    else "N/A"
                )
                print(
                    f"{record['record_type']:<15} {record['address']:<60} {record['postcode']:<10} {weight_str}"
                )

            print("-" * 100)
            print()

    # Store results in pytest module for the runner to access
    import pytest

    pytest._test_results = test_results
