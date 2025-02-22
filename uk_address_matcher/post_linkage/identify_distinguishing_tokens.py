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

    sql = f"""
    create or replace table with_tokens as
    SELECT
        regexp_split_to_array(upper(trim(original_address_concat_l)), '\\s+') as tokens_l,
        regexp_split_to_array(upper(trim(original_address_concat_r)), '\\s+') as tokens_r,
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
        flatten(
            array_agg(tokens_l)  FILTER (WHERE match_weight > -20) OVER (
                PARTITION BY unique_id_r
                ORDER BY reverse(original_address_concat_l) DESC
                ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
            )
        ) as all_tokens_in_group_l,


        list_filter(tokens_l, t -> len(list_filter(all_tokens_in_group_l, x -> x = t)) = 1
            AND t NOT IN ('FLAT', 'FLOOR'))
            AS canonical_distinguishing_tokens_1,

        list_filter(
            list_intersect(tokens_r, canonical_distinguishing_tokens_1),
            t -> t NOT IN ('FLAT', 'FLOOR')
        ) AS messy_distinguishing_tokens_1,

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
    select
        unique_id_r,
        unique_id_l,
        len(list_intersect(canonical_distinguishing_tokens_1, messy_distinguishing_tokens_1)) > 0 as dist_tok_match,
        match_weight as match_weight_original,

        case
            when dist_tok_match then match_weight + 5
            else match_weight - 5
        end as match_weight,

        match_probability as match_probability_original,
        pow(2, match_weight)/(1+pow(2, match_weight)) as match_probability,


        canonical_distinguishing_tokens_1,
        messy_distinguishing_tokens_1,
        original_address_concat_l,
        postcode_l,
        original_address_concat_r,
        postcode_r,

    from windowed_tokens
    """

    con.execute(sql)
    matches = con.table("matches")
    return matches
