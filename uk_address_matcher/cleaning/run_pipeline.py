from typing import Callable, List, Optional

from duckdb import DuckDBPyConnection, DuckDBPyRelation


def run_pipeline(
    ddb_pyrel: DuckDBPyRelation,
    *,
    con: DuckDBPyConnection,
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
        df (DuckDBPyRelation): The input data frame to which the SQL transforms are
            applied.
        cleaning_queue (List[Callable]): A list of functions that implement SQL
            transforms.
        print_intermediate (bool, optional): Whether to print the data frame after each
            transform. Defaults to False.
        filter_sql (Optional[str], optional): A SQL condition to filter the data frame
            before printing. Doesn't affect the data processing. Defaults to None.

    Returns:
        DuckDBPyRelation: The data frame after all transforms have been applied.
    """

    for i, cleaning_function in enumerate(cleaning_queue):
        ddb_pyrel = cleaning_function(ddb_pyrel, con)

        if print_intermediate:
            print(
                f"{'-' * 20}\nApplying function: {cleaning_function.__name__}, result:"
            )
            df_filtered = ddb_pyrel.filter(filter_sql) if filter_sql else ddb_pyrel
            df_filtered.show(max_rows=10, max_width=10000, max_col_width=10000)

    return ddb_pyrel
