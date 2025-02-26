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

    # Calculate distinguishability in SQL using window functions
    # This calculates the difference between each match's weight and the top match weight in its group
    matches_with_distinguishability_query = """
    SELECT
        unique_id_r AS test_block_id,
        unique_id_l AS match_id,
        match_weight,
        true_match_id,
        FIRST_VALUE(match_weight) OVER (
            PARTITION BY unique_id_r
            ORDER BY match_weight DESC
        ) - match_weight AS distinguishability_from_top_match,
        CASE WHEN unique_id_l = true_match_id THEN 1 ELSE 0 END AS is_correct_match,
        CASE WHEN unique_id_l = true_match_id THEN
            FIRST_VALUE(unique_id_l) OVER (
                PARTITION BY unique_id_r
                ORDER BY match_weight DESC
            ) = true_match_id
        ELSE 0 END AS is_true_match_at_top
    FROM results
    """
    duckdb_con.execute(
        f"CREATE OR REPLACE VIEW matches_with_metrics AS {matches_with_distinguishability_query}"
    )

    duckdb_con.table("matches_with_metrics").show(max_width=50000)

    # Get top matches for each test block with separate reward and penalty calculations
    top_matches_query = """
    SELECT
        test_block_id,
        FIRST_VALUE(match_id) OVER (
            PARTITION BY test_block_id
            ORDER BY match_weight DESC
        ) AS top_match_id,
        true_match_id,
        FIRST_VALUE(is_correct_match) OVER (
            PARTITION BY test_block_id
            ORDER BY match_weight DESC
        ) AS is_top_match_correct,
        -- Reward: difference between top and second best (when top is correct)
        MIN(distinguishability_from_top_match) OVER (
            PARTITION BY test_block_id
            ORDER BY match_weight DESC
            ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING
        ) AS reward,
        -- Penalty: difference between top and true match (when top is incorrect)
        (SELECT distinguishability_from_top_match
         FROM matches_with_metrics m2
         WHERE m2.test_block_id = matches_with_metrics.test_block_id
           AND m2.match_id = matches_with_metrics.true_match_id) AS penalty
    FROM matches_with_metrics
    QUALIFY ROW_NUMBER() OVER (PARTITION BY test_block_id ORDER BY match_weight DESC) = 1
    """
    duckdb_con.sql(top_matches_query).show(max_width=50000)
    top_matches = duckdb_con.execute(top_matches_query).fetchall()

    # Get match details for reporting
    all_matches = duckdb_con.execute("""
        SELECT
            test_block_id,
            match_id,
            match_weight,
            true_match_id,
            distinguishability_from_top_match,
            is_correct_match,
            is_true_match_at_top
        FROM matches_with_metrics
        ORDER BY test_block_id, match_weight DESC
    """).fetchall()

    # Initialize counters and results
    total_cases = len(top_matches)
    correct_matches = sum(match[3] for match in top_matches)
    total_reward = 0.0
    mismatches = []

    # Process each test block to collect mismatch details
    for row in top_matches:
        (
            test_block_id,
            top_match_id,
            true_match_id,
            is_top_match_correct,
            reward,
            penalty,
        ) = row

        # Get all matches for this test block
        block_matches = [m for m in all_matches if m[0] == test_block_id]

        # Add to total reward based on whether top match is correct
        if is_top_match_correct:
            # Add reward (may be None if there's only one match)
            total_reward += reward if reward is not None else float("inf")
        else:
            # Subtract penalty (may be None if true match isn't in results)
            total_reward -= penalty if penalty is not None else float("inf")

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
                    block_matches[0][2],  # top match weight
                    top_match_id,
                ],
            ).fetchall()

            mismatch = {
                "test_block_id": test_block_id,
                "distinguishability_penalty": penalty
                if penalty is not None
                else float("inf"),
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
        "total_reward": total_reward,
        "mismatches": mismatches,
    }

    return results


def print_matching_results(test_results):
    """
    Print the address matching results, including reward metrics and mismatch details.

    Args:
        test_results: Dictionary with evaluation results
    """
    print("\nAddress Matching Results:")
    print(f"Total test cases: {test_results['total_cases']}")
    print(f"Correct matches: {test_results['correct_matches']}")
    print(f"Match rate: {test_results['match_rate']:.2f}%")
    print(
        f"Total reward: {test_results['total_reward']:.2f}\n"
    )  # Renamed from total_distinguishability

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
