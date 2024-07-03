from duckdb import DuckDBPyConnection, DuckDBPyRelation


def distinguishability_table(
    df_predict: DuckDBPyRelation,
    unique_id_l: str = None,
    human_readable=False,
    best_match_only=True,
    distinguishability_thresholds=[1, 5, 10],
):

    if human_readable:
        select_cols = """
            unique_id_l,
            unique_id_r,
            source_dataset_l,
            source_dataset_r,
            original_address_concat_l,
            original_address_concat_r,
            postcode_l,
            postcode_r,
            match_probability,
            match_weight,
            distinguishability,
            distinguishability_category
            """
    else:
        select_cols = "*"

    if unique_id_l is not None:
        uid_where_condition = f"where unique_id_l = '{unique_id_l}'"
    else:
        uid_where_condition = "where 1=1"

    best_match_only_condition = "and rn = 1" if best_match_only else ""
    if 0 not in distinguishability_thresholds:
        distinguishability_thresholds.append(0)

    thres_sorted = sorted(distinguishability_thresholds, reverse=True)
    d_case_whens = "\n".join(
        [
            f"when distinguishability > {d} then '{str(index).zfill(2)}: Distinguishability > {d}'"
            for index, d in enumerate(thres_sorted, start=2)
        ]
    )

    next_label_index = len(thres_sorted) + 2
    next_label_value = f"{str(next_label_index).zfill(2)}."

    sql = f"""
    WITH results_with_rn AS (
            SELECT
                unique_id_l,
                *,
                ROW_NUMBER() OVER (PARTITION BY unique_id_l ORDER BY match_weight DESC) AS rn
            FROM predict_for_distinguishability
        ),
        second_place_match_weight AS (
            SELECT
                *,
                LEAD(match_weight) OVER (PARTITION BY unique_id_l ORDER BY match_weight DESC) AS previous_match_weight,
            FROM results_with_rn

        ),
        distinguishability as (
            SELECT
                *,
                match_weight - coalesce(previous_match_weight, null) as distinguishability
                from second_place_match_weight
        ),
        disting_2 as (
        select *,
        case
        when distinguishability is null then '01: One match only'
        {d_case_whens}
        when distinguishability = 0 then '{next_label_value}: Distinguishability = 0'
        else '99: error, uncategorised'
        end as distinguishability_category
        from distinguishability

        )
        select
            {select_cols}

        from disting_2
        {uid_where_condition}
        {best_match_only_condition}
        order by unique_id_l

    """

    return df_predict.query("predict_for_distinguishability", sql)


def distinguishability_by_id(
    df_predict: DuckDBPyRelation,
    df_addresses_to_match: DuckDBPyRelation,
    con: DuckDBPyConnection,
    distinguishability_thresholds=[1, 5, 10],
):

    if 0 not in distinguishability_thresholds:
        distinguishability_thresholds.append(0)

    thres_sorted = sorted(distinguishability_thresholds, reverse=True)
    d_case_whens = "\n".join(
        [
            f"when distinguishability > {d} then '{str(index).zfill(2)}: Distinguishability > {d}'"
            for index, d in enumerate(thres_sorted, start=2)
        ]
    )

    next_label_index = len(thres_sorted) + 2
    next_label_value = f"{str(next_label_index).zfill(2)}."

    con.register("df_predict_xyz", df_predict)
    con.register("df_addresses_to_match_xyz", df_addresses_to_match)

    sql = f"""
    WITH results_with_rn AS (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY unique_id_l ORDER BY match_weight DESC) AS rn
            FROM df_predict_xyz
        ),
        second_place_match_weight AS (
            SELECT
                *,
                LEAD(match_weight) OVER (PARTITION BY unique_id_l ORDER BY match_weight DESC) AS previous_match_weight,
            FROM results_with_rn

        ),
        distinguishability as (
            SELECT
                *,
                match_weight - coalesce(previous_match_weight, null) as distinguishability
                from second_place_match_weight
        ),
        disting_2 as (
        select *,
        case
        when distinguishability is null then '01: One match only'
        {d_case_whens}
        when distinguishability = 0 then '{next_label_value}: Distinguishability = 0'
        else '99: error, uncategorised'
        end as distinguishability_category

        from distinguishability

        ),
        dist_by_id as (
        select
            *


        from disting_2
        where rn = 1
        order by unique_id_l
        ),
        dist_by_id_with_nulls as (
            select d.*,
                a.original_address_concat as original_address_concat,
                a.postcode as postcode

            from df_addresses_to_match_xyz as a
            left join dist_by_id as d
            on a.unique_id = d.unique_id_l
        )

        select
        unique_id_l,
        distinguishability,
        match_probability,
        match_weight,
        case
            when distinguishability_category is null then '99: No match'
            else distinguishability_category end as distinguishability_category,
        coalesce(original_address_concat_l,original_address_concat) as original_address_concat_l,
        coalesce(postcode_l,postcode) as postcode_l,
        unique_id_r,
        original_address_concat_r,
        postcode_r,



        from dist_by_id_with_nulls
        order by distinguishability_category asc, match_weight desc

    """

    return df_predict.query("predict_for_distinguishability", sql)


def distinguishability_summary(
    *,
    df_predict: DuckDBPyRelation,
    df_addresses_to_match: DuckDBPyRelation,
    con: DuckDBPyConnection,
    disinguishability_thresholds=[1, 5, 10],
    group_by_match_weight_bins=False,
):
    d_list_cat = distinguishability_by_id(
        df_predict, df_addresses_to_match, con, disinguishability_thresholds
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
