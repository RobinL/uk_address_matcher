import duckdb
from uk_address_matcher.cleaning.cleaning_steps import (
    parse_out_flat_position_and_letter,
    remove_duplicate_end_tokens,
)


def run_tests(test_cases):
    duckdb_connection = duckdb.connect()

    # Prepare the test data as a DuckDB table
    test_data = "SELECT * FROM (VALUES\n"
    test_data += ",\n".join(f"('{case['input']}')" for case in test_cases)
    test_data += ") AS t(address_concat)"

    input_relation = duckdb_connection.sql(test_data)

    # Run the actual function
    result = parse_out_flat_position_and_letter(input_relation, duckdb_connection)
    results = result.fetchall()

    # Assert that the results match the expected output
    for actual_row, case in zip(results, test_cases):
        expected_flat_pos = case["expected_flat_positional"]
        expected_flat_letter = case["expected_flat_letter"]
        assert actual_row[-2] == expected_flat_pos, (
            f"For address '{case['input']}', expected positional {expected_flat_pos} but got {actual_row[-2]}"
        )
        assert actual_row[-1] == expected_flat_letter, (
            f"For address '{case['input']}', expected letter {expected_flat_letter} but got {actual_row[-1]}"
        )


def test_parse_out_flat_positional():
    test_cases = [
        {
            "input": "11A SPITFIRE COURT 243 BIRMINGHAM",
            "expected_flat_positional": None,
            "expected_flat_letter": "A",
        },
        {
            "input": "FLAT A 11 SPITFIRE COURT 243 BIRMINGHAM",
            "expected_flat_positional": None,
            "expected_flat_letter": "A",
        },
        {
            "input": "BASEMENT FLAT A 11 SPITFIRE COURT 243 BIRMINGHAM",
            "expected_flat_positional": "BASEMENT",
            "expected_flat_letter": "A",
        },
        {
            "input": "BASEMENT FLAT 11 SPITFIRE COURT 243 BIRMINGHAM",
            "expected_flat_positional": "BASEMENT",
            "expected_flat_letter": None,
        },
        {
            "input": "GARDEN FLAT 11 SPITFIRE COURT 243 BIRMINGHAM",
            "expected_flat_positional": "GARDEN",
            "expected_flat_letter": None,
        },
        {
            "input": "TOP FLOOR FLAT 12A HIGH STREET",
            "expected_flat_positional": "TOP FLOOR",
            "expected_flat_letter": "A",
        },
        {
            "input": "GROUND FLOOR FLAT B 25 MAIN ROAD",
            "expected_flat_positional": "GROUND FLOOR",
            "expected_flat_letter": "B",
        },
        {
            "input": "FIRST FLOOR 15B LONDON ROAD",
            "expected_flat_positional": "FIRST FLOOR",
            "expected_flat_letter": "B",
        },
        {
            "input": "UNIT C MY HOUSE 120 MY ROAD",
            "expected_flat_positional": None,
            "expected_flat_letter": "C",
        },
    ]
    run_tests(test_cases)


def test_remove_duplicate_end_tokens():
    test_cases = [
        {
            "input": "9A SOUTHVIEW ROAD SOUTHWICK LONDON LONDON",
            "expected": "9A SOUTHVIEW ROAD SOUTHWICK LONDON",
        },
        {
            "input": "1 HIGH STREET ST ALBANS ST ALBANS",
            "expected": "1 HIGH STREET ST ALBANS",
        },
        {
            "input": "2 CORINATION ROAD KINGS LANGLEY HERTFORDSHIRE HERTFORDSHIRE",
            "expected": "2 CORINATION ROAD KINGS LANGLEY HERTFORDSHIRE",
        },
        {
            "input": "FLAT 2 8 ORCHARD WAY MILTON KEYNES MILTON KEYNES",
            "expected": "FLAT 2 8 ORCHARD WAY MILTON KEYNES",
        },
        {
            "input": "9 SOUTHVIEW ROAD SOUTHWICK LONDON",
            "expected": "9 SOUTHVIEW ROAD SOUTHWICK LONDON",
        },
        {
            "input": "1 LONDON ROAD LONDON",
            "expected": "1 LONDON ROAD LONDON",
        },
    ]

    duckdb_connection = duckdb.connect()

    # Prepare test data
    test_data = "SELECT * FROM (VALUES\n"
    test_data += ",\n".join(f"('{case['input']}')" for case in test_cases)
    test_data += ") AS t(address_concat)"

    input_relation = duckdb_connection.sql(test_data)

    # Run the function
    result = remove_duplicate_end_tokens(input_relation, duckdb_connection)
    results = result.fetchall()

    # Assert results
    for actual_row, case in zip(results, test_cases):
        assert actual_row[0] == case["expected"], (
            f"For address '{case['input']}', "
            f"expected '{case['expected']}' but got '{actual_row[0]}'"
        )
