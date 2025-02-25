import duckdb
from pathlib import Path
import pytest
from uk_address_matcher import clean_data_using_precomputed_rel_tok_freq, get_linker
from uk_address_matcher.post_linkage.identify_distinguishing_tokens import (
    improve_predictions_using_distinguishing_tokens,
)
from tests.utils import prepare_combined_test_data
from splink import block_on

# Constants
MATCH_WEIGHT_THRESHOLD_PREDICT = -50
MATCH_WEIGHT_THRESHOLD_IMPROVE = -20


def run_matcher_workflow(messy_addresses, canonical_addresses, duckdb_con=None):
    """
    Run the complete address matching workflow.

    Args:
        messy_addresses: DuckDB relation with messy addresses
        canonical_addresses: DuckDB relation with canonical addresses
        duckdb_con: Optional DuckDB connection (defaults to in-memory database)

    Returns:
        DuckDB relation: Matching results with predicted and true match IDs
    """
    if duckdb_con is None:
        duckdb_con = duckdb.connect(database=":memory:")

    # Clean the input data
    messy_clean = clean_data_using_precomputed_rel_tok_freq(
        messy_addresses, con=duckdb_con
    )
    canonical_clean = clean_data_using_precomputed_rel_tok_freq(
        canonical_addresses, con=duckdb_con
    )

    # Configure the linker
    columns_to_retain = ["original_address_concat", "true_match_id"]
    linker = get_linker(
        df_addresses_to_match=messy_clean,
        df_addresses_to_search_within=canonical_clean,
        con=duckdb_con,
        include_full_postcode_block=True,
        additional_columns_to_retain=columns_to_retain,
    )
    linker._settings_obj._blocking_rules_to_generate_predictions = [
        block_on("test_block").get_blocking_rule("duckdb")
    ]

    # Predict matches (first pass)
    predicted_matches = linker.inference.predict(
        threshold_match_weight=MATCH_WEIGHT_THRESHOLD_PREDICT,
        experimental_optimisation=True,
    ).as_duckdbpyrelation()

    # Improve predictions (second pass)
    improved_matches = improve_predictions_using_distinguishing_tokens(
        df_predict=predicted_matches,
        con=duckdb_con,
        match_weight_threshold=MATCH_WEIGHT_THRESHOLD_IMPROVE,
    )

    # Join true match IDs from the messy data
    sql = """
    SELECT p.*, c.true_match_id
    FROM improved_matches p
    LEFT JOIN messy_clean c
    ON p.unique_id_r = c.unique_id
    """
    return duckdb_con.sql(sql)


def evaluate_matching_results(matching_results, duckdb_con):
    """
    Evaluate address matching results and collect statistics.

    Args:
        matching_results: DuckDB relation with matching results
        duckdb_con: DuckDB connection

    Returns:
        dict: Results including total cases, correct matches, match rate, and mismatch details
    """
    duckdb_con.register("results", matching_results)

    # Query to get top matches per test block
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
    matches = duckdb_con.execute(matches_query).fetchall()

    # Calculate statistics
    total_cases = len(matches)
    correct_matches = sum(1 for m in matches if m[1] == m[2])
    match_rate = (correct_matches / total_cases * 100) if total_cases > 0 else 0

    results = {
        "total_cases": total_cases,
        "correct_matches": correct_matches,
        "match_rate": match_rate,
        "mismatches": [],
    }

    # Collect mismatch details if any
    if correct_matches < total_cases:
        for test_block_id, expected_id, actual_id, match_weight in matches:
            if expected_id == actual_id:
                continue

            mismatch_query = """
            WITH
            messy_record AS (
                SELECT 'Messy Record' AS record_type, address_concat AS address,
                    postcode, NULL AS match_weight
                FROM messy_table_combined
                WHERE unique_id = ?
            ),
            true_match AS (
                SELECT 'True Match' AS record_type, address_concat AS address,
                    postcode,
                    (SELECT match_weight FROM results WHERE unique_id_r = ? AND unique_id_l = ?)
                    AS match_weight
                FROM canonical_table_combined
                WHERE unique_id = ?
            ),
            false_match AS (
                SELECT 'False Match' AS record_type, address_concat AS address,
                    postcode, ? AS match_weight
                FROM canonical_table_combined
                WHERE unique_id = ?
            )
            SELECT * FROM messy_record
            UNION ALL SELECT * FROM true_match
            UNION ALL SELECT * FROM false_match
            ORDER BY record_type
            """
            details = duckdb_con.execute(
                mismatch_query,
                [
                    test_block_id,
                    test_block_id,
                    expected_id,
                    expected_id,
                    match_weight,
                    actual_id,
                ],
            ).fetchall()

            mismatch = {
                "test_block_id": test_block_id,
                "records": [
                    {
                        "record_type": r[0],
                        "address": r[1],
                        "postcode": r[2],
                        "match_weight": r[3],
                    }
                    for r in details
                ],
            }
            results["mismatches"].append(mismatch)

    return results


def print_matching_results(test_results):
    """
    Print the address matching results and mismatch details.

    Args:
        test_results: Dictionary with evaluation results
    """
    print("\nAddress Matching Results:")
    print(f"Total test cases: {test_results['total_cases']}")
    print(f"Correct matches: {test_results['correct_matches']}")
    print(f"Match rate: {test_results['match_rate']:.2f}%\n")

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
                weight = (
                    f"{record['match_weight']:.2f}"
                    if record["match_weight"] is not None
                    else "N/A"
                )
                print(
                    f"{record['record_type']:<15} {record['address']:<60} {record['postcode']:<10} {weight}"
                )
            print("-" * 100)
            print()


def test_address_matching_combined():
    """
    Test that address matching correctly identifies expected matches using a combined dataset.

    Stores results in pytest._test_results for test runner access.
    """
    duckdb_con = duckdb.connect(database=":memory:")
    yaml_path = Path(__file__).parent / "test_addresses.yaml"

    # Prepare data
    messy_addresses, canonical_addresses = prepare_combined_test_data(
        yaml_path, duckdb_con
    )

    # Run matching workflow
    matching_results = run_matcher_workflow(
        messy_addresses, canonical_addresses, duckdb_con
    )

    # Evaluate results
    test_results = evaluate_matching_results(matching_results, duckdb_con)

    # Print results (for local testing)
    print_matching_results(test_results)

    # Store for pytest
    pytest._test_results = test_results
