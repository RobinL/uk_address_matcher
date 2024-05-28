from typing import Callable, List, Optional

import duckdb
from duckdb import DuckDBPyRelation


def run_pipeline(
    cleaning_queue: List[Callable],
    print_intermediate: bool = False,
    filter_sql: Optional[str] = None,
) -> DuckDBPyRelation:
    """
    This function applies a series of SQL transforms to a data frame. Each transform is
    implemented as a function and is passed in the cleaning_queue list.

    If print_intermediate is set to True, the function will print the data frame after
    each transform. The filter_sql parameter can be used to filter the data frame
    before printing, but it doesn't affect the actual data processing.

    If filter_sql is not provided, the entire data frame will be printed.

    Args:
        cleaning_queue (List[Callable]): A list of functions that implement SQL
            transforms.
        print_intermediate (bool, optional): Whether to print the data frame after each
            transform. Defaults to False.
        filter_sql (Optional[str], optional): A SQL condition to filter the data frame
            before printing. Doesn't affect the data processing. Defaults to None.

    Returns:
        DuckDBPyRelation: The data frame after all transforms have been applied.
    """

    table_name = "initial_table_name"

    # Have to register name to avoid recursion error
    # of repeatedly assigning the result of a query to a df
    # of the same name
    for i, cleaning_function in enumerate(cleaning_queue):
        df = cleaning_function(table_name)
        table_name = f"df_{i}"
        duckdb.register(table_name, df)
        if print_intermediate:
            df_filtered = df.filter(filter_sql) if filter_sql else df
            df_filtered.show(max_rows=10, max_width=10000, max_col_width=10000)

    return df
