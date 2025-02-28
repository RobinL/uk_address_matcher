from duckdb import DuckDBPyRelation, DuckDBPyConnection


def improve_predictions_using_distinguishing_tokens(
    *,
    df_predict: DuckDBPyRelation,
    con: DuckDBPyConnection,
    match_weight_threshold: float = -20,
    top_n_matches: int = 5,
    use_bigrams: bool = True,
    use_trigrams: bool = True,
):
    """
    Improve match predictions by identifying distinguishing tokens between addresses.

    Args:
        df_predict: DuckDB relation containing the prediction data
        con: DuckDB connection
        match_weight_threshold: Minimum match weight to consider
        top_n_matches: Number of top matches to consider for each unique_id_r
        use_bigrams: Whether to use bigram analysis in matching
        use_trigrams: Whether to use trigram analysis in matching

    Returns:
        DuckDBPyRelation: Table with improved match predictions
    """
    # Define template strings for n-gram operations

    # Template for creating n-grams from a token list
    ngram_template = """
    -- Create {gram_type} from {token_list}
    list_transform(
        list_zip(
            {zip_parts}
        ),
        tup -> {concat_parts}
    ) AS {output_name}"""

    # Function to generate n-gram creation SQL
    def generate_ngram_sql(gram_type, token_list, output_name):
        n = 1
        if gram_type == "bigrams":
            n = 2
        elif gram_type == "trigrams":
            n = 3

        zip_parts = []
        concat_parts = []

        for i in range(1, n + 1):
            zip_parts.append(
                f"list_slice({token_list}, {i}, length({token_list}) - {n - i})"
            )

        concat_parts_str = "concat_ws(' '"
        for i in range(1, n + 1):
            concat_parts_str += f", tup[{i}]"
        concat_parts_str += ")"

        return ngram_template.format(
            gram_type=gram_type,
            token_list=token_list,
            zip_parts=",\n                ".join(zip_parts),
            concat_parts=concat_parts_str,
            output_name=output_name,
        )

    # Template for histogram overlapping calculations
    histogram_overlap_template = """
    -- Filter to only include {gram_type} that appear in both r and the block
    map_from_entries(
        list_filter(
            map_entries(hist_all_{gram_type}_in_block_l),
            x -> list_contains({gram_type}_r, x.key)
        )
    ) AS hist_overlapping_{gram_type}_r_block_l"""

    # Template for filtering to items not in another list
    not_in_list_template = """
    -- {gram_type} in r but not in this l
    list_filter({source_list}, item -> item NOT IN {filter_list}) as {output_name}"""

    # Template for creating map from entries that overlap
    overlap_map_template = """
    -- Filter to only include {gram_type} that appear in both this l and r
    map_from_entries(
        list_filter(
            map_entries(hist_overlapping_{gram_type}_r_block_l),
            x -> list_contains({gram_type}_l, x.key)
        )
    ) AS overlapping_{gram_type}_this_l_and_r"""

    # Template for items that appear elsewhere but not in this item
    elsewhere_but_not_here_template = """
    -- {gram_type} that appear elsewhere in the block but not in this l
    map_from_entries(
        list_filter(
            map_entries(hist_all_{gram_type}_in_block_l),
            x -> list_contains({gram_type}_r_not_in_l, x.key)
        )
    ) AS {gram_type}_elsewhere_in_block_but_not_this"""

    # Template for the adjustment formula in the final SQL
    adjustment_template = """ifnull(map_values(overlapping_{gram_type}_this_l_and_r)
        .list_transform(x -> 1/(x^2))
        .list_sum() * {reward_multiplier}, 0)
    - map_values({gram_type}_elsewhere_in_block_but_not_this)
        .length() * {punishment_multiplier}"""

    # Token section is always included
    tokens_section_cte = f"""
        flatten(array_agg((regexp_split_to_array(upper(trim(original_address_concat_l)), '\\s+')))) as tokens_in_block_l,

        -- Counts of tokens in canonical addresses within block
        list_aggregate(tokens_in_block_l, 'histogram') AS hist_all_tokens_in_block_l,

        {histogram_overlap_template.format(gram_type="tokens")}"""

    # Prepare parts of the SQL that depend on use_bigrams and use_trigrams
    tokens_cte_parts = [tokens_section_cte]

    if use_bigrams:
        bigram_section_tokens = f"""
        -- Create bigrams from all tokens in block
        {generate_ngram_sql("bigrams", "tokens_in_block_l", "bigrams_in_block_l")},

        -- Counts of bigrams in canonical addresses within block
        list_aggregate(bigrams_in_block_l, 'histogram') AS hist_all_bigrams_in_block_l,

        -- Create bigrams from tokens_r
        {generate_ngram_sql("bigrams", "tokens_r", "bigrams_r")},

        {histogram_overlap_template.format(gram_type="bigrams")}"""
        tokens_cte_parts.append(bigram_section_tokens)

    if use_trigrams:
        trigram_section_tokens = f"""
        -- Create trigrams from all tokens in block
        {generate_ngram_sql("trigrams", "tokens_in_block_l", "trigrams_in_block_l")},

        -- Counts of trigrams in canonical addresses within block
        list_aggregate(trigrams_in_block_l, 'histogram') AS hist_all_trigrams_in_block_l,

        -- Create trigrams from tokens_r
        {generate_ngram_sql("trigrams", "tokens_r", "trigrams_r")},

        {histogram_overlap_template.format(gram_type="trigrams")}"""
        tokens_cte_parts.append(trigram_section_tokens)

    # Token section for intermediate CTE is always included
    intermediate_cte_parts = [
        f"""
            original_address_concat_l
                .trim()
                .upper()
                .regexp_split_to_array('\\s+')
                AS tokens_l,
            t.tokens_r,

            {overlap_map_template.format(gram_type="tokens")},

            t.hist_all_tokens_in_block_l,
            t.hist_overlapping_tokens_r_block_l,

            {
            not_in_list_template.format(
                gram_type="Tokens",
                source_list="t.tokens_r",
                filter_list="tokens_l",
                output_name="tokens_r_not_in_l",
            )
        },

            {elsewhere_but_not_here_template.format(gram_type="tokens")},

            -- missing tokens are tokens in the canonical address but not in the messy address
            list_filter(tokens_l, t -> t NOT IN tokens_r) AS missing_tokens"""
    ]

    if use_bigrams:
        bigram_section_intermediate = f"""
            -- Create bigrams from tokens_l
            {generate_ngram_sql("bigrams", "(tokens_l)", "bigrams_l")},

            t.bigrams_r,

            {overlap_map_template.format(gram_type="bigrams")},

            t.hist_all_bigrams_in_block_l,
            t.hist_overlapping_bigrams_r_block_l,

            {
            not_in_list_template.format(
                gram_type="Bigrams",
                source_list="t.bigrams_r",
                filter_list="bigrams_l",
                output_name="bigrams_r_not_in_l",
            )
        },

            {elsewhere_but_not_here_template.format(gram_type="bigrams")}"""
        intermediate_cte_parts.append(bigram_section_intermediate)

    if use_trigrams:
        trigram_section_intermediate = f"""
            -- Create trigrams from tokens_l
            {generate_ngram_sql("trigrams", "(tokens_l)", "trigrams_l")},

            t.trigrams_r,

            {overlap_map_template.format(gram_type="trigrams")},

            t.hist_all_trigrams_in_block_l,
            t.hist_overlapping_trigrams_r_block_l,

            {
            not_in_list_template.format(
                gram_type="Trigrams",
                source_list="t.trigrams_r",
                filter_list="trigrams_l",
                output_name="trigrams_r_not_in_l",
            )
        },

            {elsewhere_but_not_here_template.format(gram_type="trigrams")}"""
        intermediate_cte_parts.append(trigram_section_intermediate)

    # Fields for the final SELECT statement
    select_fields_parts = [
        """
        unique_id_l,
        unique_id_r,
        match_weight,
        match_probability,
        original_address_concat_l,
        postcode_l,
        original_address_concat_r,
        postcode_r,

        -----------------
        -- TOKENS SECTION
        -----------------

        overlapping_tokens_this_l_and_r,
        tokens_elsewhere_in_block_but_not_this,
        hist_overlapping_tokens_r_block_l,
        hist_all_tokens_in_block_l,
        missing_tokens"""
    ]

    if use_bigrams:
        bigram_fields = """
        -----------------
        -- BIGRAMS SECTION
        -----------------

        overlapping_bigrams_this_l_and_r,
        bigrams_elsewhere_in_block_but_not_this,
        hist_overlapping_bigrams_r_block_l,
        hist_all_bigrams_in_block_l"""
        select_fields_parts.append(bigram_fields)

    if use_trigrams:
        trigram_fields = """
        -----------------
        -- TRIGRAMS SECTION
        -----------------

        overlapping_trigrams_this_l_and_r,
        trigrams_elsewhere_in_block_but_not_this,
        hist_overlapping_trigrams_r_block_l,
        hist_all_trigrams_in_block_l"""
        select_fields_parts.append(trigram_fields)

    # Join parts with proper separators to build the SQL
    tokens_cte = ",\n".join(tokens_cte_parts)
    intermediate_cte = ",\n".join(intermediate_cte_parts)
    select_fields = ",".join(select_fields_parts)

    # SQL for tokenizing and creating n-grams
    sql_token_and_ngrams = f"""
    WITH good_matches AS (
        SELECT *
        FROM df_predict
        WHERE match_weight > {match_weight_threshold}
    ),
    top_n_matches AS (
        SELECT *
        FROM good_matches
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY unique_id_r
            ORDER BY match_weight DESC
        ) <= {top_n_matches}  -- e.g., 5 for top 5 matches
    ),
    tokenise_r AS (
        SELECT DISTINCT
            unique_id_r,
            original_address_concat_r
                .trim()
                .upper()
                .regexp_split_to_array('\\s+')
                -- .list_filter(tok -> tok NOT IN ('FLAT'))
                as tokens_r
        FROM top_n_matches
    ),
    tokens as (
        select
        t.unique_id_r,
        t.tokens_r,

        -----------------
        -- TOKENS SECTION
        -----------------
{tokens_cte}

        from top_n_matches m
        join tokenise_r t using (unique_id_r)
        group by t.unique_id_r, t.tokens_r
    ),
    intermediate AS (
        SELECT
            match_weight,
            match_probability,
            unique_id_l,
            m.unique_id_r,
            original_address_concat_l,
            original_address_concat_r,

            -----------------
            -- TOKENS SECTION
            -----------------
{intermediate_cte},
            postcode_l,
            postcode_r
        FROM top_n_matches m
        left join tokens t using (unique_id_r)
    )
    SELECT{select_fields}
    FROM intermediate
    ORDER BY unique_id_r;
    """
    # print(sql_token_and_ngrams)

    windowed_tokens = con.sql(sql_token_and_ngrams)

    # Define multipliers for each n-gram type
    overall_reward_multiplier = 1.5
    REWARD_MULTIPLIER = 2 * overall_reward_multiplier
    PUNISHMENT_MULTIPLIER = 3 * overall_reward_multiplier
    BIGRAM_REWARD_MULTIPLIER = 2 * overall_reward_multiplier
    BIGRAM_PUNISHMENT_MULTIPLIER = 3 * overall_reward_multiplier
    TRIGRAM_REWARD_MULTIPLIER = 2 * overall_reward_multiplier
    TRIGRAM_PUNISHMENT_MULTIPLIER = 3 * overall_reward_multiplier

    # Base adjustment for tokens (always included)
    adjustment_parts = [
        f"""
        -- Token-based adjustments
        {
            adjustment_template.format(
                gram_type="tokens",
                reward_multiplier=REWARD_MULTIPLIER,
                punishment_multiplier=PUNISHMENT_MULTIPLIER,
            )
        }
        - (0.1 * len(missing_tokens))"""
    ]

    # Add bigram adjustment if enabled
    if use_bigrams:
        bigram_adjustment = f"""
        -- Bigram-based adjustments
        + {
            adjustment_template.format(
                gram_type="bigrams",
                reward_multiplier=BIGRAM_REWARD_MULTIPLIER,
                punishment_multiplier=BIGRAM_PUNISHMENT_MULTIPLIER,
            )
        }"""
        adjustment_parts.append(bigram_adjustment)

    # Add trigram adjustment if enabled
    if use_trigrams:
        trigram_adjustment = f"""
        -- Trigram-based adjustments
        + {
            adjustment_template.format(
                gram_type="trigrams",
                reward_multiplier=TRIGRAM_REWARD_MULTIPLIER,
                punishment_multiplier=TRIGRAM_PUNISHMENT_MULTIPLIER,
            )
        }"""
        adjustment_parts.append(trigram_adjustment)

    # Join adjustment parts
    adjustment_expr = "\n".join(adjustment_parts)

    # Output fields for final SELECT
    output_fields_parts = [
        """
        -- Token-related fields
        overlapping_tokens_this_l_and_r,
        tokens_elsewhere_in_block_but_not_this,
        missing_tokens"""
    ]

    if use_bigrams:
        output_fields_parts.append("""
        -- Bigram-related fields
        overlapping_bigrams_this_l_and_r,
        bigrams_elsewhere_in_block_but_not_this""")

    if use_trigrams:
        output_fields_parts.append("""
        -- Trigram-related fields
        overlapping_trigrams_this_l_and_r,
        trigrams_elsewhere_in_block_but_not_this""")

    output_fields_parts.append("""
        original_address_concat_l,
        postcode_l,
        original_address_concat_r,
        postcode_r""")

    output_fields = ",".join(output_fields_parts)

    # Calculate new match weights based on distinguishing tokens, bigrams and trigrams
    sql = f"""
    CREATE OR REPLACE TABLE matches AS

    SELECT
        unique_id_r,
        unique_id_l,
        {adjustment_expr}
        as mw_adjustment,

        match_weight AS match_weight_original,
        match_probability AS match_probability_original,
        (match_weight_original + mw_adjustment) AS match_weight,
        pow(2, match_weight)/(1+pow(2, match_weight)) AS match_probability,
        {output_fields}

    FROM windowed_tokens
    """

    con.execute(sql)

    matches = con.table("matches")
    return matches
