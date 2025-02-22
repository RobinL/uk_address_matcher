import importlib.resources as pkg_resources
import json

from duckdb import DuckDBPyConnection, DuckDBPyRelation

from splink import DuckDBAPI, Linker, SettingsCreator


def _get_model_settings_dict():
    with (
        pkg_resources.files("uk_address_matcher.data")
        .joinpath("splink_model.json")
        .open("r") as f
    ):
        return json.load(f)


def _get_precomputed_numeric_tf_table(con: DuckDBPyConnection):
    tf_path = pkg_resources.files("uk_address_matcher.data").joinpath(
        "numeric_token_frequencies.parquet"
    )
    return con.read_parquet(str(tf_path))


def get_linker(
    df_addresses_to_match: DuckDBPyRelation,
    df_addresses_to_search_within: DuckDBPyRelation,
    *,
    con: DuckDBPyConnection,
    additional_columns_to_retain: list[str] | None = None,
    include_full_postcode_block=False,
    precomputed_numeric_tf_table: DuckDBPyRelation | None = None,
) -> Linker:
    settings_as_dict = _get_model_settings_dict()

    if additional_columns_to_retain:
        settings_as_dict.setdefault("additional_columns_to_retain", [])
        settings_as_dict["additional_columns_to_retain"] += additional_columns_to_retain

    brs = settings_as_dict["blocking_rules_to_generate_predictions"]
    if "l.postcode = r.postcode" not in brs and include_full_postcode_block:
        brs.append({"blocking_rule": "l.postcode = r.postcode"})

    settings = SettingsCreator.from_path_or_dict(settings_as_dict)

    db_api = DuckDBAPI(connection=con)

    # Need to guarantee that the canonical dataset is on the left
    sql = """
    select * exclude (source_dataset),
    '0_'  as source_dataset
    from df_addresses_to_search_within
    """
    df_addresses_to_match_fix = con.sql(sql)
    con.register("df_addresses_to_match_fix", df_addresses_to_match_fix)

    sql = """
    select * exclude (source_dataset),
    '1_' as source_dataset
    from df_addresses_to_match
    """
    df_addresses_to_search_within_fix = con.sql(sql)
    con.register("df_addresses_to_search_within_fix", df_addresses_to_search_within_fix)

    linker = Linker(
        [df_addresses_to_match, df_addresses_to_search_within],
        settings=settings,
        db_api=db_api,
    )

    if precomputed_numeric_tf_table is None:
        precomputed_numeric_tf_table = _get_precomputed_numeric_tf_table(con)

    for i in range(1, 4):
        df_sql = f"""
            select
                numeric_token as numeric_token_{i},
                tf_numeric_token as tf_numeric_token_{i}
            from precomputed_numeric_tf_table"""

        df = con.sql(df_sql)
        linker.table_management.register_term_frequency_lookup(
            df, f"numeric_token_{i}", overwrite=True
        )

    return linker
