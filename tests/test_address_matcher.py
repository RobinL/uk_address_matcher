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
    Evaluate address matching results, including the distinguishability metric and mismatch details.

    Args:
        matching_results: DuckDB relation with matching results
        duckdb_con: DuckDB connection

    Returns:
        dict: Results including total cases, correct matches, match rate, distinguishability, and mismatches
    """
    # Register the results table in DuckDB
    duckdb_con.register("results", matching_results)

    # Query all matches, ordered by test_block_id and match_weight descending
    all_matches_query = """
    SELECT
        unique_id_r AS test_block_id,
        unique_id_l AS match_id,
        match_weight,
        true_match_id
    FROM results
    ORDER BY test_block_id, match_weight DESC
    """
    all_matches = duckdb_con.execute(all_matches_query).fetchall()

    # Group matches by test_block_id
    from collections import defaultdict

    matches_by_block = defaultdict(list)
    for row in all_matches:
        test_block_id, match_id, match_weight, true_match_id = row
        matches_by_block[test_block_id].append((match_id, match_weight, true_match_id))

    # Initialize counters and results
    total_cases = 0
    correct_matches = 0
    total_distinguishability = 0.0
    mismatches = []

    # Process each test block
    for test_block_id, matches in matches_by_block.items():
        total_cases += 1
        if not matches:
            continue

        # Sort matches by match_weight in descending order
        matches.sort(key=lambda x: x[1], reverse=True)

        # Extract top match details
        top_match_id, top_match_weight, true_match_id = matches[0]

        if top_match_id == true_match_id:
            # True match is the top match
            correct_matches += 1
            if len(matches) > 1:
                second_match_weight = matches[1][1]
                distinguishability = top_match_weight - second_match_weight
            else:
                distinguishability = float("inf")
            total_distinguishability += distinguishability
        else:
            # Top match is incorrect, calculate distinguishability penalty
            true_match_weight = next(
                (mw for mid, mw, _ in matches if mid == true_match_id), None
            )
            if true_match_weight is not None:
                distinguishability_penalty = top_match_weight - true_match_weight
                total_distinguishability -= distinguishability_penalty
            else:
                distinguishability_penalty = float("inf")
                total_distinguishability -= float("inf")

            # Collect mismatch details
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
                    true_match_id,
                    true_match_id,
                    top_match_weight,
                    top_match_id,
                ],
            ).fetchall()

            mismatch = {
                "test_block_id": test_block_id,
                "distinguishability_penalty": distinguishability_penalty,
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
            mismatches.append(mismatch)

    # Calculate match rate
    match_rate = (correct_matches / total_cases * 100) if total_cases > 0 else 0

    # Compile results
    results = {
        "total_cases": total_cases,
        "correct_matches": correct_matches,
        "match_rate": match_rate,
        "total_distinguishability": total_distinguishability,
        "mismatches": mismatches,
    }

    return results


def print_matching_results(test_results):
    """
    Print the address matching results, including distinguishability and mismatch details with penalties.

    Args:
        test_results: Dictionary with evaluation results
    """
    print("\nAddress Matching Results:")
    print(f"Total test cases: {test_results['total_cases']}")
    print(f"Correct matches: {test_results['correct_matches']}")
    print(f"Match rate: {test_results['match_rate']:.2f}%")
    print(f"Total distinguishability: {test_results['total_distinguishability']:.2f}\n")

    if test_results["mismatches"]:
        print("Details of mismatches:")
        print("-" * 80)
        for mismatch in test_results["mismatches"]:
            print(f"Test Block ID: {mismatch['test_block_id']}")
            penalty = (
                f"{mismatch['distinguishability_penalty']:.2f}"
                if mismatch["distinguishability_penalty"] != float("inf")
                else "inf"
            )
            print(f"Distinguishability Penalty: {penalty}")
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
