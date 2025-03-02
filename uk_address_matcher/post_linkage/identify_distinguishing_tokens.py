from duckdb import DuckDBPyRelation, DuckDBPyConnection


def improve_predictions_using_distinguishing_tokens(
    *,
    df_predict: DuckDBPyRelation,
    con: DuckDBPyConnection,
    match_weight_threshold: float = -20,
    top_n_matches: int = 5,
    use_bigrams: bool = True,
    additional_columns_to_retain: list[str] | None = None,
):
    """
    Improve match predictions by identifying distinguishing tokens between addresses.

    Args:
        df_predict: DuckDB relation containing the prediction data
        con: DuckDB connection
        match_weight_threshold: Minimum match weight to consider
        top_n_matches: Number of top matches to consider for each unique_id_r
        use_bigrams: Whether to use bigram-based matching (default: True)

    Returns:
        DuckDBPyRelation: Table with improved match predictions
    """

    add_cols_select = ""
    if additional_columns_to_retain:
        for col in additional_columns_to_retain:
            add_cols_select += f"{col}_l, {col}_r, "

    # Create a table with tokenized addresses
    sql_token_and_bigrams = f"""

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
    remove_common_end_tokens AS (
        SELECT
            * EXCLUDE (original_address_concat_r, original_address_concat_l),

            original_address_concat_l
                .trim()
                .upper()
                .regexp_split_to_array('\\s+')
                .list_reverse()
                .list_filter((tok, i) -> not (i = 1 and list_contains(common_end_tokens_r.list_transform(x -> x.tok), tok)))
                .list_filter((tok, i) -> not (i = 1 and list_contains(common_end_tokens_r.list_transform(x -> x.tok), tok)))
                .list_reverse()
                .array_to_string(' ')
                as original_address_concat_l,

            original_address_concat_r
                .trim()
                .upper()
                .regexp_split_to_array('\\s+')
                .list_reverse()
                .list_filter((tok, i) -> not (i = 1 and list_contains(common_end_tokens_r.list_transform(x -> x.tok), tok)))
                .list_filter((tok, i) -> not (i = 1 and list_contains(common_end_tokens_r.list_transform(x -> x.tok), tok)))
                .list_reverse()
                .array_to_string(' ')
                as original_address_concat_r,

        FROM top_n_matches
    ),
    tokenise_r AS (
        SELECT DISTINCT
            unique_id_r,

            concat_ws(' ', original_address_concat_r, postcode_r)
                .trim()
                .upper()
                .regexp_split_to_array('\\s+')
                as tokens_r
        FROM remove_common_end_tokens
    ),
    tokens as (
        select
        t.unique_id_r,
        t.tokens_r,

        -----------------
        -- TOKENS SECTION
        -----------------



        concat_ws(' ', original_address_concat_l, postcode_l)
                .trim()
                .upper()
                .regexp_split_to_array('\\s+')
                .array_agg()
                .flatten()
                as tokens_in_block_l,



        -- Counts of tokens in canonical addresses within block
        list_aggregate(tokens_in_block_l, 'histogram') AS hist_all_tokens_in_block_l,

        -- Filter to only include tokens that appear in both r and the block
        map_from_entries(
                list_filter(
                    map_entries(hist_all_tokens_in_block_l),
                    x -> list_contains(tokens_r, x.key)
                )
            ) AS hist_overlapping_tokens_r_block_l,



        {
        '''
        -----------------
        -- BIGRAMS SECTION
        -----------------

        -- Create bigrams from all tokens in block
        list_transform(
            list_zip(
                list_slice(tokens_in_block_l, 1, length(tokens_in_block_l) - 1),
                list_slice(tokens_in_block_l, 2, length(tokens_in_block_l))
            ),
            tup -> ARRAY[tup[1], tup[2]]
        ) AS bigrams_in_block_l,

        -- Counts of bigrams in canonical addresses within block
        list_aggregate(bigrams_in_block_l, 'histogram') AS hist_all_bigrams_in_block_l,

        -- Create bigrams from tokens_r
        list_transform(
            list_zip(
                list_slice(tokens_r, 1, length(tokens_r) - 1),
                list_slice(tokens_r, 2, length(tokens_r))
            ),
            tup -> ARRAY[tup[1], tup[2]]
        ) AS bigrams_r,

        -- Filter to only include bigrams that appear in both r and the block
        map_from_entries(
            list_filter(
                map_entries(hist_all_bigrams_in_block_l),
                x -> list_contains(bigrams_r, x.key)
            )
        ) AS hist_overlapping_bigrams_r_block_l
        '''
        if use_bigrams
        else ""
    }

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

            -----------------
            -- TOKENS SECTION
            -----------------

            concat_ws(' ', original_address_concat_l, postcode_l)
                .trim()
                .upper()
                .regexp_split_to_array('\\s+')
                -- .list_filter(tok -> tok NOT IN ('FLAT'))
                AS tokens_l,
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



            {
        '''
            -----------------
            -- BIGRAMS SECTION
            -----------------

            -- Create bigrams from tokens_l
            list_transform(
                list_zip(
                    list_slice((tokens_l), 1,
                              length((tokens_l)) - 1),
                    list_slice((tokens_l), 2,
                              length((tokens_l)))
                ),
                tup -> ARRAY[tup[1], tup[2]]
            ) AS bigrams_l,

            t.bigrams_r,

            -- Filter to only include bigrams that appear in both this l and r
            map_from_entries(
                list_filter(
                    map_entries(hist_overlapping_bigrams_r_block_l),
                    x -> list_contains(bigrams_l, x.key)
                )
            ) AS overlapping_bigrams_this_l_and_r,

            t.hist_all_bigrams_in_block_l,
            t.hist_overlapping_bigrams_r_block_l,

            -- Bigrams in r but not in this l
            list_filter(t.bigrams_r, bg -> bg NOT IN bigrams_l) as bigrams_r_not_in_l,

            -- Bigrams that appear elsewhere in the block but not in this l
            map_from_entries(
                list_filter(
                    map_entries(hist_all_bigrams_in_block_l),
                    x -> list_contains(bigrams_r_not_in_l, x.key)
                )
            ) AS bigrams_elsewhere_in_block_but_not_this,
            '''
        if use_bigrams
        else ""
    }

            postcode_l,
            postcode_r,
            {add_cols_select}
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

        -----------------
        -- TOKENS SECTION
        -----------------

        overlapping_tokens_this_l_and_r,
        tokens_elsewhere_in_block_but_not_this,
        hist_overlapping_tokens_r_block_l,
        hist_all_tokens_in_block_l,
        missing_tokens,



        {
        '''
        -----------------
        -- BIGRAMS SECTION
        -----------------
        -- Filter out from bigrams tokens already covered in tokens (unigrams) part

        overlapping_bigrams_this_l_and_r,
        bigrams_elsewhere_in_block_but_not_this,
        hist_overlapping_bigrams_r_block_l,
        hist_all_bigrams_in_block_l,

        overlapping_bigrams_this_l_and_r
        .map_entries()
        .list_filter(x ->
            NOT (
                (
                    map_contains(overlapping_tokens_this_l_and_r, x.key[1])
                    AND overlapping_tokens_this_l_and_r[x.key[1]] <= x.value
                )
                AND
                (
                    map_contains(overlapping_tokens_this_l_and_r, x.key[2])
                    AND overlapping_tokens_this_l_and_r[x.key[2]] <= x.value
                )
            )
        )
        .map_from_entries() AS overlapping_bigrams_this_l_and_r_filtered,


        bigrams_elsewhere_in_block_but_not_this
        .map_entries()
         .list_filter(x ->
            NOT (
                (
                    map_contains(tokens_elsewhere_in_block_but_not_this, x.key[1])
                    AND tokens_elsewhere_in_block_but_not_this[x.key[1]] <= x.value
                )
                AND
                (
                    map_contains(tokens_elsewhere_in_block_but_not_this, x.key[2])
                    AND tokens_elsewhere_in_block_but_not_this[x.key[2]] <= x.value
                )
            )
        )
        .map_from_entries() AS bigrams_elsewhere_in_block_but_not_this_filtered,


        '''
        if use_bigrams
        else ""
    }
    {add_cols_select}


    FROM intermediate
    ORDER BY unique_id_r;
    """

    windowed_tokens = con.sql(sql_token_and_bigrams)

    # Calculate new match weights based on distinguishing tokens and bigrams

    overall_reward_multiplier = 1.5
    REWARD_MULTIPLIER = 2 * overall_reward_multiplier
    PUNISHMENT_MULTIPLIER = 1 * overall_reward_multiplier
    BIGRAM_REWARD_MULTIPLIER = 2 * overall_reward_multiplier
    BIGRAM_PUNISHMENT_MULTIPLIER = 1 * overall_reward_multiplier

    sql = f"""
    CREATE OR REPLACE TABLE matches AS

    SELECT
        unique_id_r,
        unique_id_l,

        -- Token-based adjustments
        ifnull(map_values(overlapping_tokens_this_l_and_r)
            .list_transform(x -> 1/(x^2))
            .list_sum() *  {REWARD_MULTIPLIER}, 0)

        -  ifnull(map_values(tokens_elsewhere_in_block_but_not_this)
            .list_transform(x -> 1)
            .list_sum() *  {PUNISHMENT_MULTIPLIER}, 0)

        - (0.1 * len(missing_tokens))

        -- Bigram-based adjustments
        {
        f'''
        + ifnull(map_values(overlapping_bigrams_this_l_and_r_filtered)
            .list_transform(x -> 1/(x^2))
            .list_sum() * {BIGRAM_REWARD_MULTIPLIER}, 0)
        -  ifnull(map_values(bigrams_elsewhere_in_block_but_not_this_filtered)
            .list_transform(x -> 1)
            .list_sum() *  {BIGRAM_PUNISHMENT_MULTIPLIER}, 0)


        '''
        if use_bigrams
        else ""
    }
        as mw_adjustment,

        match_weight AS match_weight_original,
        match_probability AS match_probability_original,
        (match_weight_original + mw_adjustment) AS match_weight,
        pow(2, match_weight)/(1+pow(2, match_weight)) AS match_probability,

        -- Token-related fields
        overlapping_tokens_this_l_and_r,
        tokens_elsewhere_in_block_but_not_this,
        missing_tokens,

        {
        '''
        -- Bigram-related fields
        overlapping_bigrams_this_l_and_r,
        bigrams_elsewhere_in_block_but_not_this,
        overlapping_bigrams_this_l_and_r_filtered,
        bigrams_elsewhere_in_block_but_not_this_filtered,
        '''
        if use_bigrams
        else ""
    }

        original_address_concat_l,
        postcode_l,
        original_address_concat_r,
        postcode_r,
        {add_cols_select}



    FROM windowed_tokens
    """

    con.execute(sql)
    matches = con.table("matches")
    return matches
