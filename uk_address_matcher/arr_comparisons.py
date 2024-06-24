def array_reduce_by_freq(
    token_rel_freq_array_name, nonagreement_punishment_weight=0.33
):
    # This pretty gnarly SQL calculates:
    # 1. The product of the relative frequencies of matching tokens
    # 2. Then divides by the product of the relative frequencies of non-matching tokens
    # But non-matching items are weighted lower than matching (the 0.33)
    # Note that:
    # - You can't ues array_intersect directly on a struct, so i have to list_transform
    # - There's no array_difference fn, so I use list_filter with list_contains

    calc_tf_sql = f"""
    list_reduce(
        list_prepend(
            1.0,
            list_transform(
                array_filter(
                    {token_rel_freq_array_name}_l,
                    y -> array_contains(
                        array_intersect(
                            list_transform({token_rel_freq_array_name}_l, x -> x.tok),
                            list_transform({token_rel_freq_array_name}_r, x -> x.tok)
                        ),
                        y.tok
                    )
                ),
                x -> x.rel_freq
            )
        ),
        (p, q) -> p * q
    )
    """

    punish = f"""
    list_reduce(
        list_prepend(
            1.0,
            list_transform(
                list_concat(
                    array_filter(
                        {token_rel_freq_array_name}_l,
                            y -> not array_contains(
                                    list_transform({token_rel_freq_array_name}_r, x -> x.tok),
                                    y.tok
                                )
                    ),
                    array_filter(
                        {token_rel_freq_array_name}_r,
                            y -> not array_contains(
                                    list_transform({token_rel_freq_array_name}_l, x -> x.tok),
                                    y.tok
                                )
                    )
                ),

                x -> x.rel_freq
            )
        ),
        (p, q) -> p / q^{nonagreement_punishment_weight}
    )
    """
    if nonagreement_punishment_weight > 0.0:
        return f"{calc_tf_sql} *  {punish}"
    else:
        return calc_tf_sql


# Alt implementation that turns out to be much slower, I'm not sure why
# def array_reduce_by_freq(
#     token_rel_freq_array_name, nonagreement_punishment_weight=0.33
# ):
#     # This pretty gnarly SQL calculates:
#     # 1. The product of the relative frequencies of matching tokens
#     # 2. Then divides by the product of the relative frequencies of non-matching tokens
#     # But non-matching items are weighted lower than matching (the 0.33)
#     # Note that:
#     # - You acn't ues array_intersect directly on a struct, so I convert
#     #       the struct to json first
#     # - There's no array_difference fn, so I use list_filter with list_contains

#     tok_rel_freq_l_as_json = f"""
#     array_transform({token_rel_freq_array_name}_l, x -> to_json(x))
#     """

#     tok_rel_freq_r_as_json = f"""
#     array_transform({token_rel_freq_array_name}_r, x -> to_json(x))
#     """

#     tok_intersection_as_json = f"""
#         array_intersect(
#             {tok_rel_freq_l_as_json},
#             {tok_rel_freq_r_as_json}
#         )
#     """

#     frequencies_of_token_intersection_as_array = f"""
#     array_transform(
#         {tok_intersection_as_json},
#         x-> cast(json_extract(x, '$.rel_freq') as float)
#     )
#     """

#     tok_difference_as_json = f"""
#     list_concat(
#         list_filter(
#             {tok_rel_freq_l_as_json},
#             x -> not array_contains({tok_intersection_as_json},x)
#         ),
#         list_filter(
#             {tok_rel_freq_r_as_json},
#             x -> not array_contains({tok_intersection_as_json},x)
#         )
#     )
#     """

#     frequencies_of_token_difference_as_array = f"""
#     array_transform(
#         {tok_difference_as_json},
#         x-> cast(json_extract(x, '$.rel_freq') as float)
#     )
#     """

#     if nonagreement_punishment_weight > 0.0:
#         punish_difference = f"""
#             * list_reduce(
#                 list_prepend(1.0, {frequencies_of_token_difference_as_array}),
#                 (x,y) -> x / y^{nonagreement_punishment_weight}
#             )
#         """
#     else:
#         punish_difference = ""

#     return f"""
#     list_reduce(
#         list_prepend(1.0, {frequencies_of_token_intersection_as_array}),
#         (x,y) -> x * y
#     )
#     {punish_difference}
#     """
