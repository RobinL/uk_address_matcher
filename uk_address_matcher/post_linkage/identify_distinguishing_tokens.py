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
        SELECT *
        FROM df_predict
        WHERE match_weight > {match_weight_threshold}
    ),
    top_n_matches AS (
        SELECT *
        FROM good_matches
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY unique_id_r
            ORDER BY match_weight DESC
        ) <= {top_n_matches}  -- e.g., 5 for top 5 matches
    ),
    tokenise_r AS (
        SELECT DISTINCT
            unique_id_r,
            regexp_split_to_array(upper(trim(original_address_concat_r)), '\\s+') AS tokens_r
        FROM top_n_matches
    ),
    tokens as (
        select
        t.unique_id_r,
        t.tokens_r,
        flatten(array_agg((regexp_split_to_array(upper(trim(original_address_concat_l)), '\\s+')))) as tokens_in_block_l,

        -- Counts of tokens in canonical addresses within block
        list_aggregate(tokens_in_block_l, 'histogram') AS hist_all_tokens_in_block_l,

        --
        map_from_entries(
                list_filter(
                    map_entries(hist_all_tokens_in_block_l),
                    x -> list_contains(tokens_r, x.key)
                )
            ) AS hist_overlapping_tokens_r_block_l
        from top_n_matches m
        join tokenise_r t using (unique_id_r)
        group by t.unique_id_r, t.tokens_r
    ),
    intermediate AS (
        SELECT
            match_weight,
            match_probability,
            unique_id_l,
            m.unique_id_r,
            original_address_concat_l,
            original_address_concat_r,
            (regexp_split_to_array(upper(trim(original_address_concat_l)), '\\s+')) AS tokens_l,
            t.tokens_r,

            -- Filter out any tokens not in l block!
            map_from_entries(
                list_filter(
                    map_entries(hist_overlapping_tokens_r_block_l),
                    x -> list_contains(tokens_l, x.key)
                )
            ) AS overlapping_tokens_this_l_and_r,

            t.hist_all_tokens_in_block_l,
            t.hist_overlapping_tokens_r_block_l,

            list_filter(t.tokens_r, tok -> tok NOT IN tokens_l) as tokens_r_not_in_l,

            map_from_entries(
                list_filter(
                    map_entries(hist_all_tokens_in_block_l),
                    x -> list_contains(tokens_r_not_in_l, x.key)
                )
            ) AS tokens_elsewhere_in_block_but_not_this,

        -- missing tokens are tokens in the canonical address but not in the messy address
        list_filter(tokens_l, t -> t NOT IN tokens_r) AS missing_tokens,

            postcode_l,
            postcode_r
        FROM top_n_matches m
        left join tokens t using (unique_id_r)
    )
    SELECT
        unique_id_l,
        unique_id_r,
        match_weight,
        match_probability,
        original_address_concat_l,
        postcode_l,
        original_address_concat_r,
        postcode_r,

        overlapping_tokens_this_l_and_r,
        tokens_elsewhere_in_block_but_not_this,
        hist_overlapping_tokens_r_block_l,
        hist_all_tokens_in_block_l,
        missing_tokens,

    FROM intermediate
    ORDER BY unique_id_r;
    """

    windowed_tokens = con.sql(sql)

    # Calculate new match weights based on distinguishing tokens

    REWARD_MULTIPLIER = 2
    PUNISHMENT_MULTIPLIER = 2
    sql = f"""
    CREATE OR REPLACE TABLE matches AS

    SELECT
        unique_id_r,
        unique_id_l,

        map_values(overlapping_tokens_this_l_and_r)
            .list_transform(x -> 1/(x^2))
            .list_sum() *  {REWARD_MULTIPLIER}
        - map_values(tokens_elsewhere_in_block_but_not_this)
            .length() * {PUNISHMENT_MULTIPLIER}
        - (0.1 * len(missing_tokens))  as mw_adjustment,

        match_weight AS match_weight_original,
        match_probability AS match_probability_original,
        (match_weight_original + mw_adjustment) AS match_weight,
        pow(2, match_weight)/(1+pow(2, match_weight)) AS match_probability,
        overlapping_tokens_this_l_and_r,
        tokens_elsewhere_in_block_but_not_this,
        missing_tokens,
        original_address_concat_l,
        postcode_l,
        original_address_concat_r,
        postcode_r,


    FROM windowed_tokens
    """

    con.execute(sql)
    matches = con.table("matches")
    return matches
