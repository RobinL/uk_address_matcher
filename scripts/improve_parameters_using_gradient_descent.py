import numpy as np
import os
import pandas as pd
import altair as alt
from IPython.display import display, clear_output
import duckdb
from uk_address_matcher import (
    clean_data_using_precomputed_rel_tok_freq,
    get_linker,
    best_matches_with_distinguishability,
)
from uk_address_matcher.post_linkage.identify_distinguishing_tokens import (
    improve_predictions_using_distinguishing_tokens,
)
from uk_address_matcher.linking_model.training import get_settings_for_training

import logging

if os.path.exists("del.duckdb"):
    os.remove("del.duckdb")
con_disk = duckdb.connect("del.duckdb")

epc_path = "read_csv('secret_data/epc/raw/domestic-*/certificates.csv', filename=true)"

full_os_path = (
    "read_parquet('secret_data/ord_surv/raw/add_gb_builtaddress_sorted_zstd.parquet')"
)

labels_path = "read_csv('secret_data/epc/labels_2000.csv', all_varchar=true)"

sql = f"""
create or replace table labels_filtered as
select * from {labels_path}
where confidence = 'epc_splink_agree'
and hash(messy_id) % 10 = 0
-- and 1=2
UNION ALL
select * from {labels_path}
where confidence in ('likely', 'certain')
-- limit 100

"""
con_disk.execute(sql)
labels_filtered = con_disk.table("labels_filtered")
labels_filtered.count("*").fetchall()[0][0]

sql = f"""
create or replace table epc_data_raw as
select
LMK_KEY as unique_id,
concat_ws(' ', ADDRESS1, ADDRESS2, ADDRESS3) as address_concat,
POSTCODE as postcode,
UPRN as uprn,
UPRN_SOURCE as uprn_source
from {epc_path}
where unique_id in (select messy_id from labels_filtered)

"""
con_disk.execute(sql)

sql = f"""
create or replace table os as
select
uprn as unique_id,
regexp_replace(fulladdress, ',[^,]*$', '') AS address_concat,
postcode
from {full_os_path}
where postcode in
(select distinct postcode from epc_data_raw)
and
description != 'Non Addressable Object'

"""
con_disk.execute(sql)
df_os = con_disk.table("os")

df_epc_data = con_disk.sql("select * exclude (uprn,uprn_source) from epc_data_raw")

# messy_count = df_epc_data.count("*").fetchall()[0][0]
# canonical_count = df_os.count("*").fetchall()[0][0]
# print(f"messy_count: {messy_count:,}, canonical_count: {canonical_count:,}")

# -----------------------------------------------------------------------------
# Step 2: Clean data
# -----------------------------------------------------------------------------

df_epc_data_clean = clean_data_using_precomputed_rel_tok_freq(df_epc_data, con=con_disk)
sql = """
create table epc_data_clean as
select * from df_epc_data_clean
"""
con_disk.execute(sql)


df_os_clean = clean_data_using_precomputed_rel_tok_freq(df_os, con=con_disk)
sql = """
create table os_clean as
select * from df_os_clean
"""
con_disk.execute(sql)


def black_box(
    *,
    IMPROVE_DISTINGUISHING_MWT,
    REWARD_MULTIPLIER=3,
    PUNISHMENT_MULTIPLIER=1.5,
    BIGRAM_REWARD_MULTIPLIER=3,
    BIGRAM_PUNISHMENT_MULTIPLIER=1.5,
    MISSING_TOKEN_PENALTY=0.1,
    NUM_1_WEIGHT_1=95,
    NUM_1_WEIGHT_2=95,
    NUM_1_WEIGHT_3=4,
    NUM_1_WEIGHT_4=1 / 16,
    NUM_1_WEIGHT_5=1 / 256,
    NUM_2_WEIGHT_1=0.8 / 0.001,
    NUM_2_WEIGHT_2=1,
    NUM_2_WEIGHT_3=1 / 16,
    NUM_2_WEIGHT_4=1 / 256,
):
    con = duckdb.connect(":memory:")
    sql = "attach 'del.duckdb' as cleaned;"
    con.execute(sql)

    sql = """
    create table labels_filtered as
    select * from cleaned.labels_filtered
    """
    con.execute(sql)
    labels_filtered = con.table("labels_filtered")

    sql = """
    create table df_epc_data_clean as
    select * from cleaned.epc_data_clean
    """
    con.execute(sql)
    df_epc_data_clean = con.table("df_epc_data_clean")

    sql = """

    create table df_os_clean as
    select * from cleaned.os_clean
    """
    con.execute(sql)
    df_os_clean = con.table("df_os_clean")

    sql = """
    create table epc_data_raw as
    select * from cleaned.epc_data_raw
    """
    con.execute(sql)

    df_epc_data = con.table("epc_data_raw")

    con.execute("detach cleaned")
    df_os_clean = con.table("df_os_clean")

    settings = get_settings_for_training(
        num_1_weights={
            "WEIGHT_1": NUM_1_WEIGHT_1,
            "WEIGHT_2": NUM_1_WEIGHT_2,
            "WEIGHT_3": NUM_1_WEIGHT_3,
            "WEIGHT_4": NUM_1_WEIGHT_4,
            "WEIGHT_5": NUM_1_WEIGHT_5,
        },
        num_2_weights={
            "WEIGHT_1": NUM_2_WEIGHT_1,
            "WEIGHT_2": NUM_2_WEIGHT_2,
            "WEIGHT_3": NUM_2_WEIGHT_3,
            "WEIGHT_4": NUM_2_WEIGHT_4,
        },
    )
    # import json

    # print(json.dumps(settings.get_settings("duckdb").as_dict(), indent=4))
    logging.getLogger("splink").setLevel(logging.WARNING)

    linker = get_linker(
        df_addresses_to_match=df_epc_data_clean,
        df_addresses_to_search_within=df_os_clean,
        con=con,
        include_full_postcode_block=True,
        include_outside_postcode_block=True,
        retain_intermediate_calculation_columns=True,
        settings=settings,
    )
    c = linker.visualisations.match_weights_chart()
    # display(c)
    df_predict = linker.inference.predict(
        threshold_match_weight=-50, experimental_optimisation=True
    )
    df_predict_ddb = df_predict.as_duckdbpyrelation()

    USE_BIGRAMS = True

    df_predict_improved = improve_predictions_using_distinguishing_tokens(
        df_predict=df_predict_ddb,
        con=con,
        match_weight_threshold=IMPROVE_DISTINGUISHING_MWT,
        top_n_matches=5,
        use_bigrams=USE_BIGRAMS,
        REWARD_MULTIPLIER=REWARD_MULTIPLIER,
        PUNISHMENT_MULTIPLIER=PUNISHMENT_MULTIPLIER,
        BIGRAM_REWARD_MULTIPLIER=BIGRAM_REWARD_MULTIPLIER,
        BIGRAM_PUNISHMENT_MULTIPLIER=BIGRAM_PUNISHMENT_MULTIPLIER,
        MISSING_TOKEN_PENALTY=MISSING_TOKEN_PENALTY,
    )

    df_with_distinguishability = best_matches_with_distinguishability(
        df_predict=df_predict_improved,
        df_addresses_to_match=df_epc_data,
        con=con,
        distinguishability_thresholds=[1, 5, 10],
        best_match_only=True,
    )

    # print(df_with_distinguishability.count("*").fetchall()[0][0])
    # print(labels_filtered.count("*").fetchall()[0][0])

    squish = 1.5
    sql = f"""
    CREATE OR REPLACE TABLE truth_status as

    with joined_labels as (
    select
        m.*,
        e.uprn as epc_uprn,
        e.uprn_source as epc_source,
        l.correct_uprn as correct_uprn,
        l.confidence as label_confidence,
        case
            when m.unique_id_l = l.correct_uprn then 'true positive'
            else 'false positive'
        end as truth_status
    from df_with_distinguishability m
    left join epc_data_raw e on m.unique_id_r = e.unique_id
    left join labels_filtered l on m.unique_id_r = l.messy_id
    )
    select *,
    case
    when distinguishability_category = '01: One match only' then 1.0
    when distinguishability_category = '99: No match' then 0.0
    else (pow({squish}, distinguishability))/(1+pow({squish}, distinguishability))
    end as truth_status_numeric,
    case
    when truth_status = 'true positive'
        then  truth_status_numeric  + 1.0
    when truth_status = 'false positive'
        then (-1* truth_status_numeric) - 1.0
    else truth_status_numeric
    end as score,
    case
    when truth_status = 'true positive'
        then 1.0::float
    when truth_status = 'false positive'
        then 0.0::float
    else 0.0::float
    end as truth_status_binary
    from joined_labels


    """
    con.execute(sql)

    # print(con.table("truth_status").count("*").fetchall()[0][0])

    # sql = """
    # select * from truth_status
    # where 1=1 and
    # unique_id_r = '53242442132009020316233502068709' and unique_id_l = '34094897'

    # order by random()
    # limit 10
    # """
    # con.sql(sql).show(max_width=100000)

    # con.table("truth_status").filter(
    #     "lower(distinguishability_category) like '%no match%'"
    # ).show(max_width=100000)

    score = con.sql("select sum(score) from truth_status").fetchall()[0][0]
    num_matches = con.sql(
        "select sum(truth_status_binary) from truth_status"
    ).fetchall()[0][0]

    sql = """
    select truth_status, count(*) as count, count(*)/(select count(*) from truth_status) as pct
    from truth_status
    group by truth_status
    """
    con.sql(sql).show(max_width=100000)

    return {"score": score, "num_matches": num_matches}


def get_params_dict(params):
    params_dict = {
        name: config["initial"]
        for name, config in param_config.items()
        if not config["optimize"]
    }
    optimized_params = dict(zip(param_names, params))
    params_dict.update(optimized_params)
    return params_dict


def black_box_reward(params):
    params_dict = get_params_dict(params)
    result = black_box(**params_dict)
    return result["score"]


def create_chart(history_df, iteration):
    line_chart = (
        alt.Chart(history_df)
        .mark_line(point=True)
        .encode(
            x=alt.X("iteration:O", title="Iteration"),
            y=alt.Y("value:Q", title="Value", scale=alt.Scale(zero=False)),
        )
    )
    text_chart = (
        alt.Chart(history_df)
        .mark_text(align="left", baseline="middle", dx=7, dy=-7, fontSize=10)
        .encode(
            x=alt.X("iteration:O"),
            y=alt.Y("value:Q"),
            text=alt.Text("value:Q", format=".3f"),
        )
    )
    combined_chart = alt.layer(line_chart, text_chart, data=history_df)

    # Define variable order with categories
    metrics = ["score", "num_matches"]
    optimization_params = [
        name for name, config in param_config.items() if config["optimize"]
    ]
    fixed_params = [
        name
        for name, config in param_config.items()
        if not config["optimize"] and name not in metrics
    ]

    variable_order = metrics + optimization_params + fixed_params

    # Filter to only show metrics and optimized parameters by default
    filtered_df = history_df[history_df["variable"].isin(metrics + optimization_params)]

    facet_chart = (
        combined_chart.properties(height=50)
        .facet(
            row=alt.Row("variable:N", sort=variable_order, title=None).header(
                labelAngle=0, labelAlign="left"
            )
        )
        .resolve_scale(y="independent")
        .properties(title="Parameter Values by Iteration")
    )
    return facet_chart


# Parameter configuration and initialization remain unchanged
param_config = {
    "IMPROVE_DISTINGUISHING_MWT": {
        "initial": -10,
        "optimize": False,
        "bounds": (-30, 5),
        "perturb": 1.0,
    },
    "REWARD_MULTIPLIER": {
        "initial": 3,
        "optimize": True,
        "bounds": (0, 20),
        "perturb": 0.5,
    },
    "PUNISHMENT_MULTIPLIER": {
        "initial": 1.5,
        "optimize": True,
        "bounds": (0.2, 20),
        "perturb": 0.5,
    },
    "BIGRAM_REWARD_MULTIPLIER": {
        "initial": 3,
        "optimize": True,
        "bounds": (0, 20),
        "perturb": 0.5,
    },
    "BIGRAM_PUNISHMENT_MULTIPLIER": {
        "initial": 1.5,
        "optimize": True,
        "bounds": (0.2, 20),
        "perturb": 0.5,
    },
    "MISSING_TOKEN_PENALTY": {
        "initial": 0.1,
        "optimize": True,
        "bounds": (0.01, 10),
        "perturb": 0.05,
    },
    "NUM_1_WEIGHT_1": {
        "initial": 6.57,
        "optimize": True,
        "bounds": (1, 30),
        "perturb": 1.0,
    },
    "NUM_1_WEIGHT_2": {
        "initial": 6.57,
        "optimize": True,
        "bounds": (1, 30),
        "perturb": 1.0,
    },
    "NUM_1_WEIGHT_3": {
        "initial": 2,
        "optimize": True,
        "bounds": (0.1, 20),
        "perturb": 1.0,
    },
    "NUM_1_WEIGHT_4": {
        "initial": -4,
        "optimize": True,
        "bounds": (-10, 1),
        "perturb": 1.0,
    },
    "NUM_1_WEIGHT_5": {
        "initial": -8,
        "optimize": True,
        "bounds": (-20, -0.1),
        "perturb": 1.0,
    },
    "NUM_2_WEIGHT_1": {
        "initial": 6.57,
        "optimize": True,
        "bounds": (1, 20),
        "perturb": 1.0,
    },
    "NUM_2_WEIGHT_2": {
        "initial": 0,
        "optimize": True,
        "bounds": (-5, 10),
        "perturb": 1.0,
    },
    "NUM_2_WEIGHT_3": {
        "initial": -2,
        "optimize": True,
        "bounds": (-10, 0),
        "perturb": 1.0,
    },
    "NUM_2_WEIGHT_4": {
        "initial": -4,
        "optimize": True,
        "bounds": (-10, 1),
        "perturb": 1.0,
    },
}
param_names = [name for name, config in param_config.items() if config["optimize"]]
initial_params_array = [param_config[name]["initial"] for name in param_names]
lower_bounds = np.array([param_config[name]["bounds"][0] for name in param_names])
upper_bounds = np.array([param_config[name]["bounds"][1] for name in param_names])
perturb_scale = np.array([param_config[name]["perturb"] for name in param_names])

alpha = 0.001
alpha_decay = 0.995
min_alpha = 0.0001
momentum = 0.3
num_iterations = 100

params = np.array(initial_params_array)
num_params = len(params)
velocity = np.zeros(num_params)

print("Parameter configuration:")
for name, config in param_config.items():
    status = "OPTIMIZING" if config["optimize"] else "FIXED (using initial value)"
    print(f"  {name}: {status}, initial value: {config['initial']}")
    if config["optimize"]:
        print(
            f"    bounds: {config['bounds']}, perturbation scale: {config['perturb']}"
        )

# Initial computation
initial_params_dict = get_params_dict(params)
initial_result = black_box(**initial_params_dict)
initial_score = initial_result["score"]
initial_num_matches = initial_result["num_matches"]

print(f"Initial Score: {initial_score:,.2f}")
print(f"Initial Num Matches: {initial_num_matches:,.0f}")
best_score = initial_score
best_params = params.copy()

history = [
    {"iteration": -1, "variable": "score", "value": initial_score},
    {"iteration": -1, "variable": "num_matches", "value": initial_num_matches},
]
for name in param_config:
    history.append(
        {"iteration": -1, "variable": name, "value": param_config[name]["initial"]}
    )

# Optimization loop
for iteration in range(num_iterations):
    alpha = max(alpha * alpha_decay, min_alpha)
    delta = np.random.choice([-1, 1], size=num_params) * perturb_scale
    params_plus = np.clip(params + delta, lower_bounds, upper_bounds)
    params_minus = np.clip(params - delta, lower_bounds, upper_bounds)
    reward_plus = black_box_reward(params_plus)
    reward_minus = black_box_reward(params_minus)
    gradient = -(reward_plus - reward_minus) / (2 * delta)

    print(f"  Iteration {iteration}:")
    print(f"    Gradient: {gradient.tolist()}")
    print(f"    Parameters (before update): {params.tolist()}")
    print(f"    Current alpha: {alpha:.6f}")

    velocity = momentum * velocity + alpha * gradient
    params -= velocity
    params = np.clip(params, lower_bounds, upper_bounds)
    params_dict = get_params_dict(params)
    result = black_box(**params_dict)
    score = result["score"]
    num_matches = result["num_matches"]

    history.append({"iteration": iteration, "variable": "score", "value": score})
    history.append(
        {"iteration": iteration, "variable": "num_matches", "value": num_matches}
    )
    for name in param_config:
        if param_config[name]["optimize"]:
            value = params[param_names.index(name)]
        else:
            value = param_config[name]["initial"]
        history.append({"iteration": iteration, "variable": name, "value": value})

    history_df = pd.DataFrame(history)
    clear_output(wait=True)
    chart = create_chart(history_df, iteration)
    display(chart)

    print(f"  Parameters (after update): {params.tolist()}")
    print(f"Score: {score:,.2f}")
    print(f"Num Matches: {num_matches:,.0f}")
    param_change_magnitude = np.linalg.norm(velocity)
    print(f"  Parameter change magnitude: {param_change_magnitude:.6f}")
    print(f"  Velocity: {velocity.tolist()}")

    if score > best_score:
        best_score = score
        best_params = params.copy()
        print(f"New best score found: {best_score:,.2f}")

    if param_change_magnitude < 1e-5 and iteration > 10:
        print(f"Converged at iteration {iteration} - parameter changes too small")
        break

# Final results
print(f"Optimization completed. Best Score: {best_score:,.2f}")
print(f"Parameter names: {param_names}")
print("Final best parameters:")
final_params_dict = {
    name: config["initial"]
    for name, config in param_config.items()
    if not config["optimize"]
}
best_params_dict = dict(zip(param_names, best_params))
final_params_dict.update(best_params_dict)

# Group parameters by category for better readability
print("\nOptimized Parameters:")
for name in param_names:
    value = best_params_dict[name]
    print(f"  {name}: {value:.4f}")

print("\nFixed Parameters:")
fixed_params = {
    name: value for name, value in final_params_dict.items() if name not in param_names
}
# Group numeric token weights
num_1_weights = {k: v for k, v in fixed_params.items() if k.startswith("NUM_1_")}
num_2_weights = {k: v for k, v in fixed_params.items() if k.startswith("NUM_2_")}
other_fixed = {
    k: v
    for k, v in fixed_params.items()
    if not (k.startswith("NUM_1_") or k.startswith("NUM_2_"))
}

if other_fixed:
    print("  Other:")
    for name, value in other_fixed.items():
        print(f"    {name}: {value:.4f}")

if num_1_weights:
    print("  Numeric Token 1 Weights:")
    for name, value in sorted(num_1_weights.items()):
        print(f"    {name}: {value:.4f}")

if num_2_weights:
    print("  Numeric Token 2 Weights:")
    for name, value in sorted(num_2_weights.items()):
        print(f"    {name}: {value:.4f}")
