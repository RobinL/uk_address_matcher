import importlib.resources as pkg_resources

from duckdb import DuckDBPyConnection, DuckDBPyRelation

from .regexes import (
    construct_nested_call,
    move_flat_to_front,
    remove_apostrophes,
    remove_commas_periods,
    remove_multiple_spaces,
    remove_repeated_tokens,
    replace_fwd_slash_with_dash,
    separate_letter_num,
    standarise_num_dash_num,
    standarise_num_letter,
    trim,
)


def upper_case_address_and_postcode(
    ddb_pyrel: DuckDBPyRelation, con: DuckDBPyConnection
) -> DuckDBPyRelation:
    sql = """
    select
        * exclude (address_concat, postcode),
        upper(address_concat) as address_concat,
        upper(postcode) as postcode
    from ddb_pyrel
    """

    return con.sql(sql)


def derive_original_address_concat(
    ddb_pyrel: DuckDBPyRelation, con: DuckDBPyConnection
) -> DuckDBPyRelation:
    sql = """
    SELECT
        *,
        address_concat AS original_address_concat
    FROM ddb_pyrel
    """

    return con.sql(sql)


def trim_whitespace_address_and_postcode(
    ddb_pyrel: DuckDBPyRelation, con: DuckDBPyConnection
) -> DuckDBPyRelation:
    sql = """
    select
        * exclude (address_concat, postcode),
        trim(address_concat) as address_concat,
        trim(postcode) as postcode
    from ddb_pyrel
    """

    return con.sql(sql)


def clean_address_string_first_pass(
    ddb_pyrel: DuckDBPyRelation, con: DuckDBPyConnection
) -> DuckDBPyRelation:
    fn_call = construct_nested_call(
        "address_concat",
        [
            remove_commas_periods,
            remove_apostrophes,
            remove_multiple_spaces,
            replace_fwd_slash_with_dash,
            standarise_num_dash_num,
            separate_letter_num,
            standarise_num_letter,
            move_flat_to_front,
            remove_repeated_tokens,
            trim,
        ],
    )
    sql = f"""
    select
        * exclude (address_concat),
        {fn_call} as address_concat,

    from ddb_pyrel
    """

    return con.sql(sql)


def parse_out_flat_positional(
    ddb_pyrel: DuckDBPyRelation, con: DuckDBPyConnection
) -> DuckDBPyRelation:
    """
    Extracts the first flat positional from address strings and creates a new field called flat_positional.

    Examples of flat positionals:
        "BASEMENT", "GROUND FLOOR", "FIRST FLOOR", "SECOND FLOOR", "TOP FLOOR"

    Args:
        table_name (str): The name of the table to process.
        con (DuckDBPyConnection): The DuckDB connection.

    Returns:
        DuckDBPyRelation: The modified table with the new field flat_positional.
    """
    regex_pattern = r"\b(BASEMENT|GROUND FLOOR|FIRST FLOOR|SECOND FLOOR|TOP FLOOR)\b"  # Matches flat positionals

    sql = f"""
    SELECT
        *,
        NULLIF(regexp_extract(address_concat, '{regex_pattern}', 1),'') AS flat_positional
    FROM ddb_pyrel
    """
    return con.sql(sql)


def parse_out_numbers(
    ddb_pyrel: DuckDBPyRelation, con: DuckDBPyConnection
) -> DuckDBPyRelation:
    """
    Extracts and processes numeric tokens from address strings, ensuring the max length
    of the number+letter is 6 with no more than 1 letter which can be at the start or end.
    It also captures ranges like '1-2', '12-17', '98-102' as a single 'number', and
    matches patterns like '20A', 'A20', '20', and '20-21'.

    Args:
        table_name (str): The name of the table to process.
        con (DuckDBPyConnection): The DuckDB connection.

    Returns:
        DuckDBPyRelation: The modified table with processed fields.
    """
    regex_pattern = (
        r"\b"  # Word boundary
        # Prioritize matching number ranges first
        r"(\d{1,5}-\d{1,5}|[A-Za-z]?\d{1,5}[A-Za-z]?)"
        r"\b"  # Word boundary
    )
    sql = f"""
    SELECT
        * EXCLUDE (address_concat),
        regexp_replace(address_concat, '{regex_pattern}', '', 'g') AS address_without_numbers,
        regexp_extract_all(address_concat, '{regex_pattern}') AS numeric_tokens
    FROM ddb_pyrel
    """
    return con.sql(sql)


def clean_address_string_second_pass(
    ddb_pyrel: DuckDBPyRelation, con: DuckDBPyConnection
) -> DuckDBPyRelation:
    fn_call = construct_nested_call(
        "address_without_numbers",
        [remove_multiple_spaces, trim],
    )
    sql = f"""
    select
        * exclude (address_without_numbers),
        {fn_call} as address_without_numbers,

    from ddb_pyrel
    """

    return con.sql(sql)


def split_numeric_tokens_to_cols(
    ddb_pyrel: DuckDBPyRelation, con: DuckDBPyConnection
) -> DuckDBPyRelation:
    sql = """
    select
        * exclude (numeric_tokens),
        numeric_tokens[1] as numeric_token_1,
        numeric_tokens[2] as numeric_token_2,
        numeric_tokens[3] as numeric_token_3
    from ddb_pyrel
    """

    return con.sql(sql)


def extract_numeric_1_alt(
    ddb_pyrel: DuckDBPyRelation, con: DuckDBPyConnection
) -> DuckDBPyRelation:
    """
    Extracts a single letter followed by a numeric token from address strings
    and creates a new field called numeric_1_alt containing this value.

    Examples:
        'FLAT B, 427, FULHAM PALACE ROAD, LONDON' -> '427B'
        'UNIT C my house 120 my road' -> '120C'

    Args:
        table_name (str): The name of the table to process.
        con (DuckDBPyConnection): The DuckDB connection.

    Returns:
        DuckDBPyRelation: The modified table with the new field.
    """
    regex_pattern = (
        r"\b([A-Za-z])\b"  # Matches a single letter (A-Z or a-z) surrounded by word boundaries
        r".*?"  # Non-greedy match for any characters in between
        r"\b(\d+)\b"  # Matches any sequence of digits surrounded by word boundaries
    )

    sql = f"""
    SELECT
        *,
        NULLIF(regexp_extract(address_concat, '{regex_pattern}', 2) ||
        regexp_extract(address_concat, '{regex_pattern}', 1),'') AS numeric_1_alt
    FROM ddb_pyrel
    """
    return con.sql(sql)


def tokenise_address_without_numbers(
    ddb_pyrel: DuckDBPyRelation, con: DuckDBPyConnection
) -> DuckDBPyRelation:
    sql = """
    select
        * exclude (address_without_numbers),
        regexp_split_to_array(trim(address_without_numbers), '\\s+')
            AS address_without_numbers_tokenised
    from ddb_pyrel
    """

    return con.sql(sql)


def get_token_frequeny_table(
    ddb_pyrel: DuckDBPyRelation, con: DuckDBPyConnection
) -> DuckDBPyRelation:
    sql = """
    WITH concatenated_tokens AS (
        SELECT
            unique_id,
            list_concat(
                array_filter(
                    [numeric_token_1, numeric_token_2, numeric_token_3],
                    x -> x IS NOT NULL
                ),
                address_without_numbers_tokenised
            ) AS all_tokens
        FROM ddb_pyrel
    ),
    unnested as (
    SELECT
            unnest(all_tokens) AS token
            FROM concatenated_tokens
    ),
    token_counts AS (
        SELECT
            token,
            count(*) AS count,
             count(*)  / (select count(*) from unnested) rel_freq
        from unnested
        GROUP BY token
    )
    SELECT
        token, rel_freq
    FROM token_counts
    ORDER BY count DESC
    """
    return con.sql(sql)


def _tokens_with_freq_sql(
    ddb_pyrel_name: str, rel_tok_freq_name: str = "rel_tok_freq"
) -> str:
    return f"""
     addresses_exploded AS (
        SELECT
            unique_id,
            unnest(address_without_numbers_tokenised) as token,
            generate_subscripts(address_without_numbers_tokenised, 1) AS token_order
        FROM ddb_pyrel_alias
    ),
    address_groups AS (
        SELECT addresses_exploded.*,
        COALESCE({rel_tok_freq_name}.rel_freq, 5e-5) AS rel_freq
        FROM addresses_exploded
        LEFT JOIN {rel_tok_freq_name}
        ON addresses_exploded.token = {rel_tok_freq_name}.token
    ),
    token_freq_lookup AS (
        SELECT
            unique_id,
            -- This guarantees preservation of token order vs.
            -- list(struct_pack(token := token, rel_freq := rel_freq))
            list_transform(
                list_zip(
                    array_agg(token order by unique_id, token_order asc),
                    array_agg(rel_freq order by unique_id, token_order asc)
                    ),
                x-> struct_pack(tok:= x[1], rel_freq:= x[2])
            )
            as token_rel_freq_arr
        FROM address_groups
        GROUP BY unique_id
    )
    SELECT
        d.* EXCLUDE (address_without_numbers_tokenised),
        r.token_rel_freq_arr
    FROM
        ddb_pyrel_alias as d
    INNER JOIN token_freq_lookup as r
    ON d.unique_id = r.unique_id
    """


def add_term_frequencies_to_address_tokens(
    ddb_pyrel: DuckDBPyRelation, con: DuckDBPyConnection
) -> DuckDBPyRelation:
    # Compute relative term frequencies amongst the tokens
    con.register("ddb_pyrel_alias", ddb_pyrel)
    sql = f"""
    WITH rel_tok_freq_cte AS (
        SELECT
            token,
            count(*)  / sum(count(*)) OVER() as rel_freq
        FROM (
            SELECT
                unnest(address_without_numbers_tokenised) as token
            FROM ddb_pyrel_alias
        )
        GROUP BY token
    ),
    {_tokens_with_freq_sql("ddb_pyrel", rel_tok_freq_name="rel_tok_freq_cte")}
    """

    return con.sql(sql)


def add_term_frequencies_to_address_tokens_using_registered_df(
    ddb_pyrel: DuckDBPyRelation, con: DuckDBPyConnection
) -> DuckDBPyRelation:

    # For some reason this is necessary to avoid a
    # BinderException: Binder Error: Max expression depth limit of 1000 exceeded.

    con.register("ddb_pyrel_alias", ddb_pyrel)
    sql = f"""
    WITH

    {_tokens_with_freq_sql("ddb_pyrel_name")}

    """

    return con.sql(sql)


def first_unusual_token(
    ddb_pyrel: DuckDBPyRelation, con: DuckDBPyConnection
) -> DuckDBPyRelation:
    # Get first below freq
    first_token = (
        "list_any_value(list_filter(token_rel_freq_arr, x -> x.rel_freq < 0.001))"
    )

    sql = f"""
    select
    {first_token} as first_unusual_token,
    *
    from ddb_pyrel
    """
    return con.sql(sql)


def use_first_unusual_token_if_no_numeric_token(
    ddb_pyrel: DuckDBPyRelation, con: DuckDBPyConnection
) -> DuckDBPyRelation:
    sql = """
    select
        * exclude (numeric_token_1, token_rel_freq_arr, first_unusual_token),
        case
            when numeric_token_1 is null then first_unusual_token.tok
            else numeric_token_1
            end as numeric_token_1,

    case
        when numeric_token_1 is null
        then list_filter(token_rel_freq_arr, x -> coalesce(x.tok != first_unusual_token.tok, true))
        else token_rel_freq_arr
    end
    as token_rel_freq_arr
    from ddb_pyrel
    """

    return con.sql(sql)


def final_column_order(
    ddb_pyrel: DuckDBPyRelation, con: DuckDBPyConnection
) -> DuckDBPyRelation:
    sql = """
    select
        unique_id,
        source_dataset,
        numeric_token_1,
        numeric_token_2,
        numeric_token_3,
        list_transform(token_rel_freq_arr, x -> x.tok) as token_rel_freq_arr_readable,
        list_transform(common_end_tokens, x -> x.tok) as common_end_tokens_readable,
        token_rel_freq_arr,
        common_end_tokens,
        postcode,

        * exclude (
            unique_id,
            source_dataset,
            numeric_token_1,
            numeric_token_2,
            numeric_token_3,
            token_rel_freq_arr,
            common_end_tokens,
            postcode
                )

    from ddb_pyrel
    """

    return con.sql(sql)


def move_common_end_tokens_to_field(
    ddb_pyrel: DuckDBPyRelation, con: DuckDBPyConnection
) -> DuckDBPyRelation:
    # Want to put common tokens towards the end of the address
    # into their own field.  These tokens (e.g. SOMERSET or LONDON)
    # are often ommitted from so 'punishing' lack of agreement is probably
    # not necessary

    # For some reason this is necessary to avoid a
    # BinderException: Binder Error: Max expression depth limit of 1000 exceeded.
    con.register("ddb_pyrel_alias2", ddb_pyrel)
    with pkg_resources.path(
        "uk_address_matcher.data", "common_end_tokens.csv"
    ) as csv_path:
        sql = f"""
        select array_agg(token) as end_tokens_to_remove
        from read_csv_auto("{csv_path}")
        where token_count > 3000
        """
    common_end_tokens = con.sql(sql)

    # Not sure why this is needed
    con.register("common_end_tokens", common_end_tokens)

    end_tokens_as_array = """
    list_transform(common_end_tokens, x -> x.tok)
    """

    remove_end_tokens = f"""
    list_filter(token_rel_freq_arr,
        (x,i) ->
            not
            (
            i > len(token_rel_freq_arr) - 2
            and
            list_contains({end_tokens_as_array}, x.tok)
            )
    )
    """

    sql = f"""
    with

    joined as (
        select *
    from ddb_pyrel_alias2
    cross join common_end_tokens
    ),

    end_tokens_included as (
    select
    * exclude (end_tokens_to_remove),
    list_filter(token_rel_freq_arr[-3:],
        x ->  list_contains(end_tokens_to_remove, x.tok)
    )
    as common_end_tokens
    from joined
    )

    select
        * exclude (token_rel_freq_arr),
        {remove_end_tokens} as token_rel_freq_arr
    from end_tokens_included

    """

    return con.sql(sql)


def separate_unusual_tokens(
    ddb_pyrel: DuckDBPyRelation, con: DuckDBPyConnection
) -> DuckDBPyRelation:
    sql = """
    SELECT
        *,
        list_transform(list_filter(
            list_select(
                token_rel_freq_arr,
                list_grade_up(list_transform(token_rel_freq_arr, x -> x.rel_freq))
            ),
            x -> x.rel_freq < 1e-4 and x.rel_freq >= 5e-5
        ), x-> x.tok) AS unusual_tokens_arr,
        list_transform(list_filter(
            list_select(
                token_rel_freq_arr,
                list_grade_up(list_transform(token_rel_freq_arr, x -> x.rel_freq))
            ),
            x -> x.rel_freq < 5e-5 and x.rel_freq >= 1e-7
        ), x-> x.tok) AS very_unusual_tokens_arr,
        list_transform(list_filter(
            list_select(
                token_rel_freq_arr,
                list_grade_up(list_transform(token_rel_freq_arr, x -> x.rel_freq))
            ),
            x -> x.rel_freq < 1e-7
        ), x-> x.tok) AS extremely_unusual_tokens_arr
    FROM ddb_pyrel
    """
    return con.sql(sql)
