import time
import duckdb
from uk_address_matcher import (
    clean_data_using_precomputed_rel_tok_freq,
    get_linker,
    best_matches_with_distinguishability,
)
from uk_address_matcher.post_linkage.identify_distinguishing_tokens import (
    improve_predictions_using_distinguishing_tokens,
)


def black_box():
    con = duckdb.connect(":memory:")

    epc_path = (
        "read_csv('secret_data/epc/raw/domestic-*/certificates.csv', filename=true)"
    )

    full_os_path = "read_parquet('secret_data/ord_surv/raw/add_gb_builtaddress_sorted_zstd.parquet')"

    labels_path = "read_csv('secret_data/epc/labels_2000.csv', all_varchar=true)"

    sql = f"""
    select * from {labels_path}
    where confidence = 'epc_splink_agree'
    and hash(messy_id) % 10 = 0
    UNION ALL
    select * from {labels_path}
    where confidence in ('likely', 'certain')

    """
    labels_filtered = con.sql(sql)
    labels_filtered.count("*").fetchall()[0][0]

    sql = f"""
    create or replace table epc_data_raw as
    select
    LMK_KEY as unique_id,
    concat_ws(' ', ADDRESS1, ADDRESS2, ADDRESS3) as address_concat,
    POSTCODE as postcode,
    UPRN as uprn,
    UPRN_SOURCE as uprn_source
    from {epc_path}
    where unique_id in (select messy_id from labels_filtered)

    """
    con.execute(sql)

    sql = f"""
    create or replace table os as
    select
    uprn as unique_id,
    regexp_replace(fulladdress, ',[^,]*$', '') AS address_concat,
    postcode
    from {full_os_path}
    where postcode in
    (select distinct postcode from epc_data_raw)
    and
    description != 'Non Addressable Object'

    """
    con.execute(sql)
    df_os = con.table("os")

    df_epc_data = con.sql("select * exclude (uprn,uprn_source) from epc_data_raw")

    messy_count = df_epc_data.count("*").fetchall()[0][0]
    canonical_count = df_os.count("*").fetchall()[0][0]
    print(f"messy_count: {messy_count:,}, canonical_count: {canonical_count:,}")

    # -----------------------------------------------------------------------------
    # Step 2: Clean data
    # -----------------------------------------------------------------------------

    df_epc_data_clean = clean_data_using_precomputed_rel_tok_freq(df_epc_data, con=con)

    df_os_clean = clean_data_using_precomputed_rel_tok_freq(df_os, con=con)

    # -----------------------------------------------------------------------------
    # Step 3: Link data - pass 1
    # -----------------------------------------------------------------------------

    linker = get_linker(
        df_addresses_to_match=df_epc_data_clean,
        df_addresses_to_search_within=df_os_clean,
        con=con,
        include_full_postcode_block=False,
        include_outside_postcode_block=True,
        retain_intermediate_calculation_columns=True,
    )

    df_predict = linker.inference.predict(
        threshold_match_weight=-50, experimental_optimisation=True
    )
    df_predict_ddb = df_predict.as_duckdbpyrelation()

    # display(linker.visualisations.match_weights_chart())

    USE_BIGRAMS = True

    df_predict_improved = improve_predictions_using_distinguishing_tokens(
        df_predict=df_predict_ddb,
        con=con,
        match_weight_threshold=-10,
        top_n_matches=5,
        use_bigrams=USE_BIGRAMS,
    )

    df_with_distinguishability = best_matches_with_distinguishability(
        df_predict=df_predict_improved,
        df_addresses_to_match=df_epc_data,
        con=con,
        distinguishability_thresholds=[1, 5, 10],
        best_match_only=True,
    )

    print(df_with_distinguishability.count("*").fetchall()[0][0])
    print(labels_filtered.count("*").fetchall()[0][0])

    squish = 1.5
    sql = f"""
    CREATE OR REPLACE TABLE truth_status as

    with joined_labels as (
    select
        m.*,
        e.uprn as epc_uprn,
        e.uprn_source as epc_source,
        l.correct_uprn as correct_uprn,
        l.confidence as label_confidence,
        case
            when m.unique_id_l = l.correct_uprn then 'true positive'
            else 'false positive'
        end as truth_status
    from df_with_distinguishability m
    left join epc_data_raw e on m.unique_id_r = e.unique_id
    left join labels_filtered l on m.unique_id_r = l.messy_id
    )
    select *,
    case
    when distinguishability_category = '01: One match only' then 1.0
    when distinguishability_category = '99: No match' then 0.0
    else (pow({squish}, distinguishability))/(1+pow({squish}, distinguishability))
    end as truth_status_numeric,
    case
    when truth_status = 'true positive'
        then  truth_status_numeric  + 1.0
    when truth_status = 'false positive'
        then -1* truth_status_numeric - 1.0
    else truth_status_numeric
    end as score
    from joined_labels


    """
    con.execute(sql)

    print(con.table("truth_status").count("*").fetchall()[0][0])

    sql = """
    select * from truth_status
    where 1=1
    -- unique_id_r = '53242442132009020316233502068709' and unique_id_l = '34094897'

    order by random()
    limit 10
    """
    con.sql(sql).show(max_width=100000)

    con.table("truth_status").filter(
        "lower(distinguishability_category) like '%no match%'"
    ).show(max_width=100000)

    score = con.sql("select sum(score) from truth_status").fetchall()[0][0]
    print(f"Score: {score:,.2f}")
    return score


black_box()
