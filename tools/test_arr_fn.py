import duckdb

sql = """
SELECT
[
{'token': 'A', 'relative_frequency': 0.5},
{'token': 'B', 'relative_frequency': 0.01},
{'token': 'C', 'relative_frequency': 0.25},
]
AS token_rel_freq_arr_l,
[
{'token': 'B', 'relative_frequency': 0.01},
{'token': 'C', 'relative_frequency': 0.25},
{'token': 'D', 'relative_frequency': 0.1},
]
AS token_rel_freq_arr_r




"""
token_df = duckdb.sql(sql)


tok_rel_freq_l_as_json = """
array_transform(token_rel_freq_arr_l, x -> to_json(x))
"""

tok_rel_freq_r_as_json = """
array_transform(token_rel_freq_arr_r, x -> to_json(x))
"""

tok_intersection_as_json = f"""
    array_intersect(
        {tok_rel_freq_l_as_json},
        {tok_rel_freq_r_as_json}
    )
"""

frequencies_of_token_intersection_as_array = f"""
array_transform(
    {tok_intersection_as_json},
    x-> cast(json_extract(x, '$.relative_frequency') as float)
)
"""

tok_difference_as_json = f"""
list_concat(
    list_filter(
        {tok_rel_freq_l_as_json},
        x -> not array_contains({tok_intersection_as_json},x)
    ),
    list_filter(
        {tok_rel_freq_r_as_json},
        x -> not array_contains({tok_intersection_as_json},x)
    )
)
"""

frequencies_of_token_difference_as_array = f"""
array_transform(
    {tok_difference_as_json},
    x-> cast(json_extract(x, '$.relative_frequency') as float)
)
"""

calc_tf_sql = f"""
list_reduce(
    list_prepend(1.0, {frequencies_of_token_intersection_as_array}),
    (x,y) -> x * y
)
*
list_reduce(
    list_prepend(1.0, {frequencies_of_token_difference_as_array}),
    (x,y) -> x / y^0.33
)
"""
print(calc_tf_sql)

sql = f"""
select {calc_tf_sql} as tf
from token_df
"""
duckdb.sql(sql)
