from duckdb import DuckDBPyConnection, DuckDBPyRelation


def best_matches_with_distinguishability(
    df_predict: DuckDBPyRelation,
    df_addresses_to_match: DuckDBPyRelation,
    con: DuckDBPyConnection,
    distinguishability_thresholds=[1, 5, 10],
    best_match_only: bool = True,
    additional_columns_to_retain=None,
):
    """
    Finds the best match for each messy address and computes the
    distinguishability of the match, defined as the difference in match weight
    between the top and next best match

    Args:
        df_predict: table containing pairwise predictions from either
            `linker.inference.predict` or
            `improve_predictions_using_distinguishing_tokens`
        df_addresses_to_match: raw table containing addresses to be matched
            in pre-cleaned form cols = (unique_id, address_concat, postcode)
        con: DuckDB connection for executing SQL queries
        distinguishability_thresholds: List of thresholds for categorizing match
            distinguishability. Default is [1, 5, 10].
        best_match_only: If True, only return the best match for each address.
            If False, return all matches. Default is True.

    Returns:
        DuckDBPyRelation: A table containing matched addresses with
        distinguishability metrics.
    """

    con.register("predict_for_distinguishability", df_predict)
    con.register("addresses_to_match", df_addresses_to_match)

    add_cols_select = ""
    if additional_columns_to_retain:
        for col in additional_columns_to_retain:
            add_cols_select += f"{col}_l, {col}_r, "

    if 0 not in distinguishability_thresholds:
        distinguishability_thresholds.append(0)
    thres_sorted = sorted(distinguishability_thresholds, reverse=True)

    d_case_whens = "\n".join(
        [
            f"WHEN distinguishability > {d} THEN '{str(index).zfill(2)}: Distinguishability > {d}'"
            for index, d in enumerate(thres_sorted, start=2)
        ]
    )
    next_label_index = len(thres_sorted) + 2
    next_label_value = f"{str(next_label_index).zfill(2)}."

    rn_filter = (
        "QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id_r ORDER BY match_weight DESC) = 1"
        if best_match_only
        else ""
    )

    if best_match_only:
        sort_str = "ORDER BY distinguishability_category ASC, match_weight DESC"
    else:
        sort_str = "ORDER BY unique_id_r,  match_weight DESC"

    sql = f"""
    WITH
        distinguishability_calc AS (
            SELECT
                *,
                match_weight - LEAD(match_weight) OVER (
                    PARTITION BY unique_id_r ORDER BY match_weight DESC
                ) AS distinguishability,
                COUNT(*) OVER (PARTITION BY unique_id_r) AS match_count
            FROM predict_for_distinguishability
            {rn_filter}
        ),
        categorized_matches AS (
            SELECT
                *,
                CASE
                    WHEN match_count = 1 THEN '01: One match only'
                    WHEN distinguishability IS NULL THEN '{next_label_value}: NaN (last match in group)'
                    {d_case_whens}
                    WHEN distinguishability = 0 THEN '{next_label_value}: Distinguishability = 0'
                    ELSE '99: error, uncategorized'
                END AS distinguishability_category
            FROM distinguishability_calc

        )
    SELECT
        a.unique_id AS unique_id_r,
        t.unique_id_l,
        a.address_concat AS address_concat_r,
        a.postcode AS postcode_r,
        t.original_address_concat_l,
        t.postcode_l,
        t.match_probability,
        t.match_weight,
        t.distinguishability,
        COALESCE(t.distinguishability_category, '99: No match') AS distinguishability_category,
        {add_cols_select}
    FROM addresses_to_match AS a
    LEFT JOIN categorized_matches AS t
    ON a.unique_id = t.unique_id_r
    {sort_str}
    """

    return con.sql(sql)


def best_matches_summary(
    *,
    df_predict: DuckDBPyRelation,
    df_addresses_to_match: DuckDBPyRelation,
    con: DuckDBPyConnection,
    disinguishability_thresholds=[1, 5, 10],
    group_by_match_weight_bins=False,
):
    """
    Generates a summary of match distinguishability categories with counts and percentages.


    Args:
        df_predict: Table containing pairwise predictions from either
            `linker.inference.predict` or
            `improve_predictions_using_distinguishing_tokens`
        df_addresses_to_match: Raw table containing addresses to be matched
            in pre-cleaned form cols = (unique_id, address_concat, postcode)
        con: DuckDB connection for executing SQL queries
        disinguishability_thresholds: List of thresholds for categorizing match
            distinguishability. Default is [1, 5, 10].
        group_by_match_weight_bins: If True, further groups results by match weight
            bins. Default is False.

    Returns:
        DuckDBPyRelation: A summary table w

    """
    d_list_cat = best_matches_with_distinguishability(
        df_predict=df_predict,
        df_addresses_to_match=df_addresses_to_match,
        con=con,
        distinguishability_thresholds=disinguishability_thresholds,
    )
    con.register("d_list_cat", d_list_cat)

    sql = """
    select
        distinguishability_category,
        count(*) as count,
        printf('%.2f%%', 100*count(*)/sum(count(*)) over()) as percentage
    from d_list_cat
    group by distinguishability_category
    order by distinguishability_category asc
    """

    if group_by_match_weight_bins:
        sql = """
        WITH a AS (
            SELECT
                *,
                CASE
                    WHEN match_weight < -20 tHEN '00. mw < -20'
                    WHEN match_weight >= -20 AND match_weight < -10 THEN '01. -20 to -10'
                    WHEN match_weight >= -10 AND match_weight < 0 THEN '02. -10 to 0'
                    WHEN match_weight >= 0 AND match_weight < 10 THEN '03. 0 to 10'
                    WHEN match_weight >= 10 AND match_weight < 20 THEN '04. 10 to 20'
                    WHEN match_weight >= 20 THEN '05. mw > 20'
                    ELSE 'Unknown'
                END AS match_weight_bin_label
            FROM d_list_cat
        )
        SELECT
            distinguishability_category,
            match_weight_bin_label,
            COUNT(*) AS count,
            printf('%.2f%%', 100.0 * COUNT(*) / (SELECT COUNT(*) FROM a)) AS percentage
        FROM a
        GROUP BY distinguishability_category, match_weight_bin_label
        ORDER BY distinguishability_category ASC, match_weight_bin_label DESC
        """

    return con.sql(sql)
