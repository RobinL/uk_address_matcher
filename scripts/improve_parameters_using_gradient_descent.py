import time
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
    end as score
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
    # print(f"Score: {score:,.2f}")
    return score


# initial_params
initial_params = {
    "IMPROVE_DISTINGUISHING_MWT": -10,
    "REWARD_MULTIPLIER": 3,
    "PUNISHMENT_MULTIPLIER": 1.5,
    "BIGRAM_REWARD_MULTIPLIER": 3,
    "BIGRAM_PUNISHMENT_MULTIPLIER": 1.5,
}


import numpy as np

# Define parameter names and initial values
param_names = [
    "IMPROVE_DISTINGUISHING_MWT",
    "REWARD_MULTIPLIER",
    "PUNISHMENT_MULTIPLIER",
    "BIGRAM_REWARD_MULTIPLIER",
    "BIGRAM_PUNISHMENT_MULTIPLIER",
]
initial_params_array = [initial_params[name] for name in param_names]

# Define parameter bounds - setting minimum values for punishment multipliers
lower_bounds = np.array(
    [-30, 0, 0.2, 0, 0.2]
)  # Minimum of 0.2 for punishment multipliers
upper_bounds = np.array([5, 20, 20, 20, 20])


# Define the reward function to interface with black_box
def black_box_reward(params):
    params_dict = dict(zip(param_names, params))
    return black_box(**params_dict)


# Set hyperparameters - much more conservative
alpha = 0.0001  # Much smaller initial learning rate
alpha_decay = 0.99  # Learning rate decay factor
min_alpha = 0.00001  # Minimum learning rate
momentum = 0.1  # Lower momentum to prevent overshooting
num_iterations = 100  # Number of iterations


# Function to normalize gradients (clip by norm)
def clip_gradient(grad, max_norm=1.0):
    norm = np.linalg.norm(grad)
    if norm > max_norm:
        return grad * (max_norm / norm)
    return grad


# Initialize parameters
params = np.array(initial_params_array)
num_params = len(params)
velocity = np.zeros(num_params)

# Fixed perturbation scales based on parameter magnitudes
# Use smaller perturbations for parameters with greater sensitivity
base_perturb = np.array([0.1, 0.05, 0.05, 0.05, 0.05])  # Customize per parameter
perturb_scale = base_perturb * np.abs(params)  # Scale by parameter magnitude
min_perturb = 0.01 * np.abs(initial_params_array)  # Minimum perturbation size

# Compute and log initial score
initial_score = black_box_reward(params)
print(f"Initial Score: {initial_score:,.2f}")
best_score = initial_score
best_params = params.copy()

# SPSA optimization loop
for iteration in range(num_iterations):
    # Apply learning rate decay
    alpha = max(alpha * alpha_decay, min_alpha)

    # Use non-zero minimum perturbation to avoid tiny denominators
    current_perturb = np.maximum(perturb_scale, min_perturb)

    # Generate perturbation vector
    delta = np.random.choice([-1, 1], size=num_params) * current_perturb

    # Ensure perturbed parameters stay within bounds
    params_plus = np.clip(params + delta, lower_bounds, upper_bounds)
    params_minus = np.clip(params - delta, lower_bounds, upper_bounds)

    # Recalculate effective delta after clipping
    effective_delta = (params_plus - params_minus) / 2.0
    # Avoid division by zero
    effective_delta = np.where(
        np.abs(effective_delta) < 1e-10,
        1e-10 * np.sign(effective_delta),
        effective_delta,
    )

    # Evaluate the function at perturbed points
    reward_plus = black_box_reward(params_plus)
    reward_minus = black_box_reward(params_minus)

    # Estimate gradient and clip to reasonable values
    raw_gradient = -(reward_plus - reward_minus) / (2 * effective_delta)
    gradient = clip_gradient(raw_gradient, max_norm=5.0)  # Limit gradient magnitude

    # Log gradient and current parameters
    print(f"  Iteration {iteration}:")
    print(f"    Raw Gradient: {raw_gradient.tolist()}")
    print(f"    Clipped Gradient: {gradient.tolist()}")
    print(f"    Parameters (before update): {params.tolist()}")
    print(f"    Current alpha: {alpha:.6f}")

    # Apply per-parameter adaptive learning rates
    # Parameters with larger gradients get smaller learning rates
    adaptive_alpha = alpha / (1.0 + np.abs(gradient))

    # Update velocity with momentum
    velocity = momentum * velocity + adaptive_alpha * gradient

    # Update parameters
    params -= velocity

    # Clip parameters to bounds
    old_params = params.copy()
    params = np.clip(params, lower_bounds, upper_bounds)

    # If parameters hit bounds, reflect velocity
    at_bound = (params == lower_bounds) | (params == upper_bounds)
    hit_bound = at_bound & (old_params != params)
    velocity[hit_bound] = -0.5 * velocity[hit_bound]  # Partial reflection

    # Update perturbation scale conservatively
    perturb_scale = 0.98 * perturb_scale + 0.02 * np.minimum(
        0.1 * np.abs(params), np.abs(gradient)
    )
    # Cap maximum perturbation to avoid huge steps
    max_perturb = 0.05 * np.abs(params)
    perturb_scale = np.minimum(perturb_scale, max_perturb)

    # Calculate parameter change magnitude
    param_change_magnitude = np.linalg.norm(velocity)

    # Log updated parameters and additional details
    score = black_box_reward(params)
    print(f"  Parameters (after update): {params.tolist()}")
    print(f"Score: {score:,.2f}")
    print(f"  Parameter change magnitude: {param_change_magnitude:.6f}")
    print(f"  Perturbation Scale: {perturb_scale.tolist()}")
    print(f"  Velocity: {velocity.tolist()}")

    # Check for best parameters
    if score > best_score:
        best_score = score
        best_params = params.copy()
        print(f"New best score found: {best_score:,.2f}")

    # Early stopping if very small parameter changes
    if param_change_magnitude < 1e-6 and iteration > 10:
        print(f"Converged at iteration {iteration} - parameter changes too small")
        break

# Print final results
print(
    f"Optimization completed. Best Score: {best_score:,.2f}, Best Params: {best_params.tolist()}"
)
print(f"Parameter names: {param_names}")
print(f"Final best parameters dictionary:")
best_params_dict = dict(zip(param_names, best_params))
for name, value in best_params_dict.items():
    print(f"  {name}: {value:.4f}")
