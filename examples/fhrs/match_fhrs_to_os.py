import altair as alt
import time
import duckdb
from uk_address_matcher import (
    clean_data_using_precomputed_rel_tok_freq,
    get_linker,
)
from uk_address_matcher.post_linkage.analyse_results import (
    best_matches_summary,
    best_matches_with_distinguishability,
)
from uk_address_matcher.post_linkage.identify_distinguishing_tokens import (
    improve_predictions_using_distinguishing_tokens,
)

from IPython.display import display
# -----------------------------------------------------------------------------
# Step 1: Load data
# -----------------------------------------------------------------------------

overall_start_time = time.time()
con = duckdb.connect(":default:")
messy_path = "secret_data/fhrs/fhrs_data.parquet"
full_os_path = "secret_data/ord_surv/raw/add_gb_builtaddress_sorted_zstd.parquet"

sql = f"""
create or replace table df_messy as
select
    fhrsid as unique_id,
    'messy' as source_dataset,
    concat_ws(' ', AddressLine1, AddressLine2, AddressLine3, AddressLine4)
        as address_concat,
    PostCode,
    lat as lat,
    lng as lng
from read_parquet('{messy_path}')
order by random()
limit 10000
"""

con.execute(sql)
df_messy = con.table("df_messy")


## If you've pre-cleaned the OS data, you can load it directly without having to re-clean
sql = """
create or replace view os_clean as
select * from read_parquet('secret_data/ord_surv/os_clean.parquet')
where postcode in (
select distinct postcode from df_messy
)
"""
con.execute(sql)
df_os_clean = con.table("os_clean")


# Otherwise load raw os data and clean it
# sql = f"""
# create or replace table os as
# select
#     uprn as unique_id,
#     'canonical' as source_dataset,
#    regexp_replace(fulladdress, ',[^,]*$', '') AS address_concat,
#    postcode,
#     latitude as lat,
#     longitude as lng
# from read_parquet('{full_os_path}')
# where postcode in
# (select distinct postcode from df_messy)

# and description != 'Non Addressable Object'

# """
# con.execute(sql)
# df_os = con.table("os")
# df_os_clean = clean_data_using_precomputed_rel_tok_freq(df_os, con=con)

# -----------------------------------------------------------------------------
# Step 2: Clean data
# -----------------------------------------------------------------------------


messy_count = df_messy.count("*").fetchall()[0][0]
canonical_count = df_os_clean.count("*").fetchall()[0][0]

df_messy_clean = clean_data_using_precomputed_rel_tok_freq(df_messy, con=con)


end_time = time.time()
print(f"Time to load/clean: {end_time - overall_start_time} seconds")


# -----------------------------------------------------------------------------
# Step 3: Link data - pass 1
# -----------------------------------------------------------------------------

linker = get_linker(
    df_addresses_to_match=df_messy_clean,
    df_addresses_to_search_within=df_os_clean,
    con=con,
    include_full_postcode_block=True,
    include_outside_postcode_block=True,
)


df_predict = linker.inference.predict(
    threshold_match_weight=-100, experimental_optimisation=True
)
df_predict_ddb = df_predict.as_duckdbpyrelation()


# -----------------------------------------------------------------------------
# Step 4: Pass 2: There's an optimisation we can do post-linking to improve score
# described here https://github.com/RobinL/uk_address_matcher/issues/14
# -----------------------------------------------------------------------------


start_time = time.time()

USE_BIGRAMS = True


df_predict_improved = improve_predictions_using_distinguishing_tokens(
    df_predict=df_predict_ddb,
    con=con,
    match_weight_threshold=-15,
    top_n_matches=5,
    use_bigrams=USE_BIGRAMS,
)

end_time = time.time()
print(f"Improve time taken: {end_time - start_time} seconds")
print(
    f"Full time taken: {end_time - overall_start_time} seconds to match {messy_count:,.0f} messy addresses to {canonical_count:,.0f} canonical addresses at a rate of {messy_count / (end_time - overall_start_time):,.0f} addresses per second"
)

# -----------------------------------------------------------------------------
# Step 5: Inspect results
# -----------------------------------------------------------------------------

best_matches = best_matches_with_distinguishability(
    df_predict=df_predict_ddb, df_addresses_to_match=df_messy, con=con
)


dsum_1 = best_matches_summary(
    df_predict=df_predict_ddb, df_addresses_to_match=df_messy, con=con
)
dsum_1.show(max_width=500)


best_matches.filter("distinguishability_category =  '99: No match'").show(max_width=500)
# -----------------------------------------------------------------------------
# Step 6: Compare geocoding of original data vs new matches
# -----------------------------------------------------------------------------

record_number_to_view = 1

sql = """
SELECT
    m.unique_id AS messy_id,
    c.unique_id AS canonical_id,
    m.address_concat AS messy_address,
    bm.original_address_concat_l AS canonical_address,
    m.postcode AS messy_postcode,
    c.postcode AS canonical_postcode,
    m.lat AS messy_lat,
    m.lng AS messy_lng,
    c.lat AS canonical_lat,
    c.lng AS canonical_lng,
    -- Calculate great circle distance in kilometers using the Haversine formula
    6371 * 2 * ASIN(
        SQRT(
            POWER(SIN((RADIANS(c.lat) - RADIANS(m.lat)) / 2), 2) +
            COS(RADIANS(m.lat)) * COS(RADIANS(c.lat)) *
            POWER(SIN((RADIANS(c.lng) - RADIANS(m.lng)) / 2), 2)
        )
    ) AS distance_km,
    bm.match_weight,
    bm.distinguishability,
FROM df_messy m
LEFT JOIN best_matches bm ON m.unique_id = bm.unique_id_r
LEFT JOIN df_os_clean c ON bm.unique_id_l = c.unique_id
ORDER BY distance_km DESC
"""

geocoding_comparison = con.sql(sql)


# Convert to pandas DataFrame
sql = """
select * from geocoding_comparison
where match_weight > -2
and distinguishability > 5
and distance_km is not null

"""
df_distances = con.sql(sql).to_df()

# Create histogram of distances
distance_histogram = (
    alt.Chart(df_distances)
    .mark_bar()
    .encode(
        alt.X("distance_km:Q", bin=alt.Bin(maxbins=30), title="Distance (km)"),
        alt.Y("count()", title="Number of Addresses"),
        tooltip=["count()", alt.Tooltip("distance_km:Q", bin=alt.Bin(maxbins=30))],
    )
    .properties(
        title="Distribution of Distances Between Matched Addresses",
        width=600,
        height=400,
    )
)

display(distance_histogram)

# Pull out a list of addresses that we matched that didn't have lat lng
# and a list of addresses we matched where we think we have a better geocode

sql = """
select *
from geocoding_comparison

where

-- they don't have a geocode
(

    (messy_lat is null or distance_km > 0.05)
    and
    (
    (match_weight > 10 and distinguishability > 8)
    or
    (match_weight > 15 and distinguishability is null)
    )
)
order by random()
"""
con.sql(sql).show(max_width=5000)


# -----------------------------------------------------------------------------
# Step 7: Compare geocoding of original data vs new matches, single records
# -----------------------------------------------------------------------------


sql = """
select messy_id
from geocoding_comparison
where distance_km is not null
order by random()
limit 1
"""
record_to_view = con.sql(sql).fetchone()[0]


sql = f"""
select unique_id_l
from df_predict_improved
where unique_id_r = '{record_to_view}'
order by match_weight desc
limit 1
"""
best_match_id = con.sql(sql).fetchone()[0]
best_match_id


sql = f"""
with t1 as (
select *
 from geocoding_comparison


where messy_id = '{record_to_view}'
)
select
    'messy' as source,
    concat(messy_address, ' ', messy_postcode) as address,
    messy_lat as lat,
    messy_lng as lng,
    match_weight,
    distinguishability,


from t1

UNION ALL
select
    'canonical' as source,
    concat(canonical_address, ' ', canonical_postcode) as address,
    canonical_lat as lat,
    canonical_lng as lng,
    match_weight,
    distinguishability,

from t1
"""
con.sql(sql).show(max_width=700)

# Create improved map URLs with centered view and appropriate zoom level
sql_improved_map = f"""
SELECT
    messy_address,
    canonical_address,
    messy_lat,
    messy_lng,
    canonical_lat,
    canonical_lng,
    distance_km,
    match_weight,
    distinguishability,
    -- Calculate center point as average of coordinates
    (messy_lat + canonical_lat) / 2 AS center_lat,
    (messy_lng + canonical_lng) / 2 AS center_lng
FROM geocoding_comparison

where messy_id = '{record_to_view}'

"""

map_examples = con.sql(sql_improved_map).to_df()

map_examples


for i, row in map_examples.iterrows():
    center_lat = row["center_lat"]
    center_lng = row["center_lng"]
    messy_lat = row["messy_lat"]
    messy_lng = row["messy_lng"]
    canonical_lat = row["canonical_lat"]
    canonical_lng = row["canonical_lng"]

    map_url = (
        f"https://www.robinlinacre.com/url-map/"
        f"?marker={messy_lat},{messy_lng}"
        f"&marker={canonical_lat},{canonical_lng}"
        f"&center={center_lat},{center_lng}"
        f"&zoom=16"
    )

    google_maps_url = (
        f"https://www.google.com/maps/dir/?api=1"
        f"&origin={messy_lat},{messy_lng}"
        f"&destination={canonical_lat},{canonical_lng}"
    )

    print(f"\nDistance: {row['distance_km']:.2f} km")
    print(f"Messy address: {row['messy_address']}")
    print(f"Canonical address: {row['canonical_address']}")
    print(f"Map URL (red=messy, blue=canonical) {map_url}")
    print(f"Google Maps directions: {google_maps_url}")

    # Also print map url of just canonical
