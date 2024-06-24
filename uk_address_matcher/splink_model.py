import importlib.resources as pkg_resources
import json
from typing import List

from duckdb import DuckDBPyConnection, DuckDBPyRelation
from splink.duckdb.linker import DuckDBLinker


def get_pretrained_linker(
    dfs: List[DuckDBPyRelation],
    *,
    con: DuckDBPyConnection,
    precomputed_numeric_tf_table: DuckDBPyRelation = None,
):
    # Load the settings file
    with pkg_resources.path(
        "uk_address_matcher.data", "splink_model.json"
    ) as settings_path:
        settings_as_dict = json.load(open(settings_path))

    # Convert DuckDBPyRelations to pandas DataFrames
    dfs_pd = [d.df() for d in dfs]

    # Initialize the linker
    linker = DuckDBLinker(dfs_pd, settings_dict=settings_as_dict, connection=con)

    # Load the default term frequency table if none is provided
    if precomputed_numeric_tf_table is None:
        with pkg_resources.path(
            "uk_address_matcher.data", "numeric_token_frequencies.parquet"
        ) as default_tf_path:
            precomputed_numeric_tf_table = con.read_parquet(str(default_tf_path))

    if precomputed_numeric_tf_table is not None:
        for i in range(1, 4):
            df_sql = f"""
                select
                    numeric_token as numeric_token_{i},
                    tf_numeric_token as tf_numeric_token_{i}
                from precomputed_numeric_tf_table"""

            df = con.sql(df_sql).df()
            linker.register_term_frequency_lookup(
                df, f"numeric_token_{i}", overwrite=True
            )

    return linker
