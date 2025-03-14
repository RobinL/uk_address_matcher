import numpy as np
import inspect
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
import logging


def black_box(
    *,
    IMPROVE_DISTINGUISHING_MWT,
    REWARD_MULTIPLIER=3,
    PUNISHMENT_MULTIPLIER=1.5,
    BIGRAM_REWARD_MULTIPLIER=3,
    BIGRAM_PUNISHMENT_MULTIPLIER=1.5,
    MISSING_TOKEN_PENALTY=0.1,
):
    con = duckdb.connect(":memory:")

    epc_path = (
        "read_csv('secret_data/epc/raw/domestic-*/certificates.csv', filename=true)"
    )

    full_os_path = "read_parquet('secret_data/ord_surv/raw/add_gb_builtaddress_sorted_zstd.parquet')"

    labels_path = "read_csv('secret_data/epc/labels_2000.csv', all_varchar=true)"

    sql = f"""
    select * from {labels_path}
    where confidence = 'epc_splink_agree'
    and hash(messy_id) % 10 = 0
    UNION ALL
    select * from {labels_path}
    where confidence in ('likely', 'certain')
    """
    labels_filtered = con.sql(sql)
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
    con.execute(sql)

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
    con.execute(sql)
    df_os = con.table("os")

    df_epc_data = con.sql("select * exclude (uprn,uprn_source) from epc_data_raw")

    # messy_count = df_epc_data.count("*").fetchall()[0][0]
    # canonical_count = df_os.count("*").fetchall()[0][0]
    # print(f"messy_count: {messy_count:,}, canonical_count: {canonical_count:,}")

    # -----------------------------------------------------------------------------
    # Step 2: Clean data
    # -----------------------------------------------------------------------------

    df_epc_data_clean = clean_data_using_precomputed_rel_tok_freq(df_epc_data, con=con)

    df_os_clean = clean_data_using_precomputed_rel_tok_freq(df_os, con=con)

    # -----------------------------------------------------------------------------
    # Step 3: Link data - pass 1
    # -----------------------------------------------------------------------------

    linker = get_linker(
        df_addresses_to_match=df_epc_data_clean,
        df_addresses_to_search_within=df_os_clean,
        con=con,
        include_full_postcode_block=False,
        include_outside_postcode_block=True,
        retain_intermediate_calculation_columns=True,
    )
    logging.getLogger("splink").setLevel(logging.WARNING)

    df_predict = linker.inference.predict(
        threshold_match_weight=-50, experimental_optimisation=True
    )
    df_predict_ddb = df_predict.as_duckdbpyrelation()

    # display(linker.visualisations.match_weights_chart())

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
        then 1.0
    when truth_status = 'false positive'
        then 0.0
    else 0.0
    end as truth_status_binary
    from joined_labels


    """
    con.execute(sql)

    # print(con.table("truth_status").count("*").fetchall()[0][0])

    sql = """
    select * from truth_status
    where 1=1 and
    unique_id_r = '53242442132009020316233502068709' and unique_id_l = '34094897'

    order by random()
    limit 10
    """
    # con.sql(sql).show(max_width=100000)

    # con.table("truth_status").filter(
    #     "lower(distinguishability_category) like '%no match%'"
    # ).show(max_width=100000)

    score = con.sql("select sum(score) from truth_status").fetchall()[0][0]
    num_matches = con.sql(
        "select sum(truth_status_binary) from truth_status"
    ).fetchall()[0][0]

    return score


# Unified parameter configuration
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
}


# Extract parameters to optimize
param_names = [name for name, config in param_config.items() if config["optimize"]]
initial_params_array = [param_config[name]["initial"] for name in param_names]
lower_bounds = np.array([param_config[name]["bounds"][0] for name in param_names])
upper_bounds = np.array([param_config[name]["bounds"][1] for name in param_names])
perturb_scale = np.array([param_config[name]["perturb"] for name in param_names])


# Define the reward function
def black_box_reward(params):
    params_dict = {}
    for name, config in param_config.items():
        if not config["optimize"]:
            params_dict[name] = config["initial"]
    optimized_params = dict(zip(param_names, params))
    params_dict.update(optimized_params)
    return black_box(**params_dict)


def create_chart(history_df, iteration):
    # Create a line chart with points
    line_chart = (
        alt.Chart(history_df)
        .mark_line(point=True)
        .encode(
            x=alt.X("iteration:Q", title="Iteration"),
            y=alt.Y("value:Q", title="Value", scale=alt.Scale(zero=False)),
        )
    )

    # Create a text chart for the labels
    text_chart = (
        alt.Chart(history_df)
        .mark_text(
            align="left",
            baseline="middle",
            dx=7,  # Offset the text to the right of the points
            dy=-7,
            fontSize=10,
        )
        .encode(
            x=alt.X("iteration:Q"),
            y=alt.Y("value:Q"),
            text=alt.Text("value:Q", format=".3f"),  # Format to 3 decimal places
        )
    )

    # Combine the line chart and text chart
    combined_chart = alt.layer(line_chart, text_chart, data=history_df)

    # Define facet order
    variable_order = ["score"] + list(param_config.keys())

    # Create the faceted chart with the combined layers
    facet_chart = (
        combined_chart.properties(height=150)
        .facet(row=alt.Row("variable:N", sort=variable_order, title=None))
        .resolve_scale(y="independent")
        .properties(title="Parameter Values by Iteration")
    )

    return facet_chart


# Set hyperparameters
alpha = 0.001
alpha_decay = 0.995
min_alpha = 0.0001
momentum = 0.3
num_iterations = 100  # Adjust as needed

# Initialize parameters
params = np.array(initial_params_array)
num_params = len(params)
velocity = np.zeros(num_params)

# Print initial configuration
print("Parameter configuration:")
for name, config in param_config.items():
    status = "OPTIMIZING" if config["optimize"] else "FIXED (using initial value)"
    print(f"  {name}: {status}, initial value: {config['initial']}")
    if config["optimize"]:
        print(
            f"    bounds: {config['bounds']}, perturbation scale: {config['perturb']}"
        )

# Compute and log initial score
initial_score = black_box_reward(params)
print(f"Initial Score: {initial_score:,.2f}")
best_score = initial_score
best_params = params.copy()

# Initialize history list with initial values (iteration -1)
history = [{"iteration": -1, "variable": "score", "value": initial_score}]
for name in param_config:
    history.append(
        {"iteration": -1, "variable": name, "value": param_config[name]["initial"]}
    )

# SPSA optimization loop with chart display
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
    score = black_box_reward(params)

    # Append to history
    history.append({"iteration": iteration, "variable": "score", "value": score})
    for name in param_config:
        if param_config[name]["optimize"]:
            value = params[param_names.index(name)]
        else:
            value = param_config[name]["initial"]
        history.append({"iteration": iteration, "variable": name, "value": value})

    # Create DataFrame and display chart
    history_df = pd.DataFrame(history)
    clear_output(wait=True)  # Clear previous chart
    chart = create_chart(history_df, iteration)
    display(chart)

    # Log details
    print(f"  Parameters (after update): {params.tolist()}")
    print(f"Score: {score:,.2f}")
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

# Print final results
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
for name, value in final_params_dict.items():
    status = (
        "OPTIMIZED" if param_config[name]["optimize"] else "FIXED (used initial value)"
    )
    print(f"  {name}: {value:.4f} - {status}")
