def get_product_of_rel_token_freq(arr_col_name):

    overlapping_tokens_arr = f"""
    array_intersect(
                    list_transform({arr_col_name}_l, x->x.token),
                    list_transform({arr_col_name}_r, x->x.token)
            )"""

    # Cant use array intersect on struct, hence this workaround
    filter_tf_for_intersection = f"""
    array_filter({arr_col_name}_l,
    y -> array_contains({overlapping_tokens_arr}, y.token))
    """

    # Need to prepend 1.0 because can't reduce empty list
    product_of_relative_frequencies = f"""
    list_reduce(
        list_prepend(1.0,
            list_transform({filter_tf_for_intersection}, x -> x.relative_frequency)
        ),
    (p,q) -> p*q)
    """
    return product_of_relative_frequencies
