import duckdb

sql = """
SELECT
[
{'token': 'A', 'relative_frequency': 0.5},
{'token': 'B', 'relative_frequency': 0.01},
]
AS token_relative_frequency_arr_l,
[
{'token': 'B', 'relative_frequency': 0.01},
{'token': 'C', 'relative_frequency': 0.25},
]
AS token_relative_frequency_arr_r




"""
token_df = duckdb.sql(sql)


sql = """
select
list_reduce(
    list_prepend(
        1.0,
        list_transform(
            list_concat(
                array_filter(
                    token_relative_frequency_arr_l,
                        y -> not array_contains(
                                list_transform(token_relative_frequency_arr_r, x -> x.token),
                                y.token
                            )
                ),
                array_filter(
                    token_relative_frequency_arr_r,
                        y -> not array_contains(
                                list_transform(token_relative_frequency_arr_l, x -> x.token),
                                y.token
                            )
                )
            ),

            x -> x.relative_frequency
        )
    ),
    (p, q) -> p / q
) as score
from token_df
"""
duckdb.sql(sql).df()
