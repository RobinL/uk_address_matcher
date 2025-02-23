import duckdb

from uk_address_matcher.cleaning.cleaning_steps import parse_out_flat_positional


def test_parse_out_flat_positional():
    duckdb_connection = duckdb.connect()

    test_data = """
        SELECT * FROM (VALUES
            ('11A SPITFIRE COURT 243 BIRMINGHAM'),
            ('FLAT A 11 SPITFIRE COURT 243 BIRMINGHAM'),
            ('BASEMENT FLAT A 11 SPITFIRE COURT 243 BIRMINGHAM'),
            ('BASEMENT FLAT 11 SPITFIRE COURT 243 BIRMINGHAM'),
            ('GARDEN FLAT 11 SPITFIRE COURT 243 BIRMINGHAM'),
            ('TOP FLOOR FLAT 12A HIGH STREET'),
            ('GROUND FLOOR FLAT B 25 MAIN ROAD'),
            ('FIRST FLOOR 15B LONDON ROAD')
        ) AS t(address_concat)
    """
    input_relation = duckdb_connection.sql(test_data)

    result = parse_out_flat_positional(input_relation, duckdb_connection)

    results = result.fetchall()

    expected = [
        ["A"],  # 11A SPITFIRE COURT
        ["A"],  # FLAT A
        ["BASEMENT", "A"],  # BASEMENT FLAT A
        ["BASEMENT"],  # BASEMENT FLAT 11
        ["GARDEN"],  # GARDEN FLAT 11
        ["TOP FLOOR"],  # TOP FLOOR FLAT 12A
        ["GROUND FLOOR", "B"],  # GROUND FLOOR FLAT B
        ["FIRST FLOOR"],  # FIRST FLOOR 15B
    ]

    for actual_row, expected_flat_pos in zip(results, expected):
        assert actual_row[-1] == expected_flat_pos, (
            f"For address '{actual_row[0]}', expected {expected_flat_pos} but got {actual_row[-1]}"
        )


def test_edge_cases():
    duckdb_connection = duckdb.connect()
    test_data = """
        SELECT * FROM (VALUES
            (''),
            ('NO FLAT INFO HERE'),
            ('FLAT'),
            ('BASEMENT'),
            ('123 BASEMENT ROAD'),
            ('FLAT 123B STREET')
        ) AS t(address_concat)
    """
    input_relation = duckdb_connection.sql(test_data)
    result = parse_out_flat_positional(input_relation, duckdb_connection)
    results = result.fetchall()

    expected = [
        [],  # empty string
        [],  # no flat info
        [],  # just FLAT
        ["BASEMENT"],  # just BASEMENT
        [],  # BASEMENT as part of street name
        ["B"],  # FLAT 123B
    ]

    for actual_row, expected_flat_pos in zip(results, expected):
        assert actual_row[-1] == expected_flat_pos, (
            f"For address '{actual_row[0]}', expected {expected_flat_pos} but got {actual_row[-1]}"
        )
