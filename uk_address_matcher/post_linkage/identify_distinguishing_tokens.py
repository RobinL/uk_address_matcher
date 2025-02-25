from duckdb import DuckDBPyRelation, DuckDBPyConnection


def improve_predictions_using_distinguishing_tokens(
    *,
    df_predict: DuckDBPyRelation,
    con: DuckDBPyConnection,
    match_weight_threshold: float = -20,
):
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

    # TODO: SHould these have list distinct? How else to deal with two 5s in same address?
    sql = f"""
    create or replace table with_tokens as
    SELECT
        list_distinct(regexp_split_to_array(upper(trim(original_address_concat_l)), '\\s+')) as tokens_l,
        list_distinct(regexp_split_to_array(upper(trim(original_address_concat_r)), '\\s+')) as tokens_r,
        {cols}
        FROM df_predict
        where match_weight > {match_weight_threshold}
    """

    with_tokens = con.sql(sql)  # noqa: F841

    # l is canonical
    # r is messy

    # We want:
    # (1) Within group, all canonical tokens, so we know which canonical token ones appear only once in the group
    # (2) Within group,all canonical tokens

    sql = f"""

    SELECT
        match_weight,
        match_probability,
        unique_id_l,
        unique_id_r,

        tokens_l,
        tokens_r,

        -- Alternatively could  RANGE instead of ROWS to create a window based on
        -- match_weight values
        -- This includes all addresses with match_weight within 3 of the current row
        flatten(
            array_agg(tokens_l) FILTER (WHERE match_weight > -20) OVER (
                PARTITION BY unique_id_r
                ORDER BY match_weight DESC
                ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING
            )
        ) as all_tokens_in_group_l,

        -- Canonical distinguishing tokens are tokens in the row
        list_intersect(
            list_filter(tokens_l,
                t -> len(list_filter(all_tokens_in_group_l, x -> x = t)) = 1
            ),        -- canonical distinguishing tokens
            tokens_r  -- messy tokens
        )  as distinguishing_tokens_1_count_1,


        list_intersect(
            list_filter(tokens_l,
                t -> len(list_filter(all_tokens_in_group_l, x -> x = t)) = 2
            ),        -- canonical distinguishing tokens
            tokens_r  -- messy tokens
        )  as distinguishing_tokens_1_count_2,

        -- 'punishing' tokens are ones which are NOT in this address but ARE in the other
        -- addresses in the window

        list_filter(
            tokens_r,
            t -> (t IN array_distinct(all_tokens_in_group_l)) AND (t NOT IN tokens_l)
        ) AS punishment_tokens,


        flatten(
            array_agg(tokens_l) FILTER (WHERE match_weight > -5) OVER (
                PARTITION BY unique_id_r
                ORDER BY reverse(original_address_concat_l) DESC
                ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
            )
        ) as all_tokens_in_group_window_2_l,

        -- Canonical distinguishing tokens are tokens in the row
        list_intersect(
            list_filter(tokens_l,
                t -> len(list_filter(all_tokens_in_group_window_2_l, x -> x = t)) = 1
            ),        -- canonical distinguishing tokens
            tokens_r  -- messy tokens
        )  as distinguishing_tokens_2_count_1,

        -- missing tokens are tokens in the canonical address but not in the messy address
        list_filter(tokens_l, t -> t NOT IN tokens_r) as missing_tokens,


        original_address_concat_l,
        original_address_concat_r,
        postcode_l,
        postcode_r,


    FROM with_tokens
    where match_weight > {match_weight_threshold}

    order by unique_id_r

    """

    windowed_tokens = con.sql(sql)  # noqa: F841

    # TODO null handling in unique_distinguishing_match?
    sql = """
    CREATE OR REPLACE TABLE matches as
    with compute_new_mw as (
    select
        unique_id_r,
        unique_id_l,



        case
            when len(distinguishing_tokens_1_count_1) > 0 then match_weight + 2
            else match_weight
        end as match_weight_1,

        case
            when len(distinguishing_tokens_1_count_2) > 0 then match_weight_1 + 1
            else match_weight_1
        end as match_weight_2,

        case
            when len(distinguishing_tokens_2_count_1) > 0 then match_weight_2 + 1
            else match_weight_2
        end as match_weight_3,

        case
            when len(punishment_tokens) = 1 then match_weight_3 - 2
            when len(punishment_tokens) = 2 then match_weight_3 - 3
            when len(punishment_tokens) = 3 then match_weight_3 - 4
            else match_weight_3
        end as match_weight_4,

        case
            when len(missing_tokens) > 0 then match_weight_4 - 0.1
            else match_weight_4
        end as match_weight_5,

    match_weight as match_weight_original,

        match_probability as match_probability_original,


        distinguishing_tokens_1_count_1,
        distinguishing_tokens_1_count_2,
        distinguishing_tokens_2_count_1,
        punishment_tokens,
        missing_tokens,
        original_address_concat_l,
        postcode_l,
        original_address_concat_r,
        postcode_r,

    from windowed_tokens
    )
    select
    match_weight_5 as match_weight,
    pow(2, match_weight_5)/(1+pow(2, match_weight_5)) as match_probability,
    * EXCLUDE (match_weight_1, match_weight_2, match_weight_3, match_weight_4, match_weight_5),

    from compute_new_mw
    """

    con.execute(sql)
    matches = con.table("matches")
    return matches
