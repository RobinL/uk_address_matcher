from duckdb import DuckDBPyRelation, DuckDBPyConnection


def improve_predictions_using_distinguishing_tokens(
    *,
    df_predict: DuckDBPyRelation,
    con: DuckDBPyConnection,
    match_weight_threshold: float = -20,
    top_n_matches: int = 5,
):
    """
    Improve match predictions by identifying distinguishing tokens between addresses.

    Args:
        df_predict: DuckDB relation containing the prediction data
        con: DuckDB connection
        match_weight_threshold: Minimum match weight to consider
        top_n_matches: Number of top matches to consider for each unique_id_r

    Returns:
        DuckDBPyRelation: Table with improved match predictions
    """
    cols = """
        match_weight,
        match_probability,
        source_dataset_l,
        unique_id_l,
        source_dataset_r,
        unique_id_r,
        original_address_concat_l,
        original_address_concat_r,
        postcode_l,
        postcode_r,
    """

    # Create a table with tokenized addresses
    sql = f"""

    WITH good_matches AS (
        SELECT *,
            list_distinct(regexp_split_to_array(upper(trim(original_address_concat_l)), '\\s+')) AS tokens_l,
            list_distinct(regexp_split_to_array(upper(trim(original_address_concat_r)), '\\s+')) AS tokens_r
        FROM df_predict
        WHERE match_weight > {match_weight_threshold}
    ),
    ranked_matches AS (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY unique_id_r
                ORDER BY match_weight DESC
            ) AS rn
        FROM good_matches
    ),
    top_n_matches AS (
        SELECT * EXCLUDE (rn)
        FROM ranked_matches
        WHERE rn <= {top_n_matches}
    )
    SELECT
        match_weight,
        match_probability,
        unique_id_l,
        unique_id_r,

        tokens_l,
        tokens_r,

        -- Get all tokens from the window of top matches
        flatten(
            array_agg(tokens_l) OVER (
                PARTITION BY unique_id_r
                ORDER BY match_weight DESC
                ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING
            )
        ) AS all_tokens_in_group_l,

        -- Canonical distinguishing tokens are tokens in the row
        list_intersect(
            list_filter(tokens_l,
                t -> len(list_filter(all_tokens_in_group_l, x -> x = t)) = 1
            ),        -- canonical distinguishing tokens
            tokens_r  -- messy tokens
        ) AS distinguishing_tokens_1_count_1,

        list_intersect(
            list_filter(tokens_l,
                t -> len(list_filter(all_tokens_in_group_l, x -> x = t)) = 2
            ),        -- canonical distinguishing tokens
            tokens_r  -- messy tokens
        ) AS distinguishing_tokens_1_count_2,

        -- 'punishing' tokens are ones which are NOT in this address but ARE in the other
        -- addresses in the window
        list_filter(
            tokens_r,
            t -> (t IN array_distinct(all_tokens_in_group_l)) AND (t NOT IN tokens_l)
        ) AS punishment_tokens,

        flatten(
            array_agg(tokens_l) OVER (
                PARTITION BY unique_id_r
                ORDER BY reverse(original_address_concat_l) DESC
                ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
            )
        ) AS all_tokens_in_group_window_2_l,

        -- Canonical distinguishing tokens are tokens in the row
        list_intersect(
            list_filter(tokens_l,
                t -> len(list_filter(all_tokens_in_group_window_2_l, x -> x = t)) = 1
            ),        -- canonical distinguishing tokens
            tokens_r  -- messy tokens
        ) AS distinguishing_tokens_2_count_1,

        -- missing tokens are tokens in the canonical address but not in the messy address
        list_filter(tokens_l, t -> t NOT IN tokens_r) AS missing_tokens,

        original_address_concat_l,
        original_address_concat_r,
        postcode_l,
        postcode_r,
        source_dataset_l,
        source_dataset_r

    FROM top_n_matches
    ORDER BY unique_id_r
    """

    windowed_tokens = con.sql(sql)

    # Calculate new match weights based on distinguishing tokens
    # TODO null handling in unique_distinguishing_match?
    sql = """
    CREATE OR REPLACE TABLE matches AS
    WITH compute_new_mw AS (
        SELECT
            unique_id_r,
            unique_id_l,

            CASE
                WHEN len(distinguishing_tokens_1_count_1) > 0 THEN match_weight + 2
                ELSE match_weight
            END AS match_weight_1,

            CASE
                WHEN len(distinguishing_tokens_1_count_2) > 0 THEN match_weight_1 + 1
                ELSE match_weight_1
            END AS match_weight_2,

            CASE
                WHEN len(distinguishing_tokens_2_count_1) > 0 THEN match_weight_2 + 1
                ELSE match_weight_2
            END AS match_weight_3,

            CASE
                WHEN len(punishment_tokens) = 1 THEN match_weight_3 - 2
                WHEN len(punishment_tokens) = 2 THEN match_weight_3 - 3
                WHEN len(punishment_tokens) = 3 THEN match_weight_3 - 4
                ELSE match_weight_3
            END AS match_weight_4,

            CASE
                WHEN len(missing_tokens) > 0 THEN match_weight_4 - (0.1 * len(missing_tokens))
                ELSE match_weight_4
            END AS match_weight_5,

            match_weight AS match_weight_original,
            match_probability AS match_probability_original,

            distinguishing_tokens_1_count_1,
            distinguishing_tokens_1_count_2,
            distinguishing_tokens_2_count_1,
            punishment_tokens,
            missing_tokens,
            original_address_concat_l,
            postcode_l,
            original_address_concat_r,
            postcode_r,
            source_dataset_l,
            source_dataset_r

        FROM windowed_tokens
    )
    SELECT
        match_weight_5 AS match_weight,
        pow(2, match_weight_5)/(1+pow(2, match_weight_5)) AS match_probability,
        * EXCLUDE (match_weight_1, match_weight_2, match_weight_3, match_weight_4, match_weight_5)
    FROM compute_new_mw
    """

    con.execute(sql)
    matches = con.table("matches")
    return matches
