# A first stab at address matching using Splink

This repo contains generic code for address matching using Splink. At the moment it assumes you block on postcode.

It's assumed you have two input files of addresses with the following columns:

- `unique_id`
- `address_concat`
- `postcode`

Refer to [the example](example.py) for how to match your data.

There are also some folders (epc, fhrs, fhrs_companies_house) with some example code of how to collect additional data. Please ensure you read and respect the licence of the respective datasets if you use them.

This is very much work in progress!

## Step by step

In the [the example](example.py) we use a subset of data from fhrs and companies house, in total around 27,000 rows.

This step by step guide annotates the code in [the example](example.py).

## Step 1: Concat

We first concat the two datasets.  We'll only take the first 100 records to make everything run fast

```
sql = f"""
SELECT *, address_concat AS original_address_concat
FROM (
    SELECT *, address_concat AS original_address_concat
    FROM read_parquet('./example_data/companies_house_addresess_postcode_overlap.parquet')
    order by postcode
    LIMIT 100
) AS companies_house
UNION ALL
SELECT *, address_concat AS original_address_concat
FROM (
    SELECT *, address_concat AS original_address_concat
    FROM read_parquet('./example_data/fhrs_addresses_sample.parquet')
    order by postcode
    LIMIT 100
) AS fhrs

"""
df_unioned = duckdb.sql(sql)
```

## Step 1: Clean

Next, we use `address_matching.cleaning` functions in a pipeline to clean up and standarise the data.

In this case, we're going to use a precomputed table of token frequencies.  There are two reasons you may wish to do this:
1. You have a small dataset for matching, so token frequencies in your small sample are not representative of the global dataset
2. You want to make the pipeline run faster

You can use `address_matching.cleaning_pipelines.clean_data_on_the_fly` instead if you want to compute everything on the fly

```python
# Load in pre-computed token frequencies.  This allows the model to operatre
# even if your dataset is small/unrepresentative
# Created using
# .token_and_term_frequencies.get_address_token_frequencies_from_address_table
path = "./example_data/rel_tok_freq.parquet"
rel_tok_freq = duckdb.sql(f"SELECT token, rel_freq FROM read_parquet('{path}')")

ddb_df = clean_data_using_precomputed_rel_tok_freq(
    address_table, rel_tok_freq_table=rel_tok_freq
)
```

# Step 2: Split back into two dataset to put into splink

```python
df_1 = df_cleaned.filter("source_dataset == 'companies_house'").df()
df_2 = df_cleaned.filter("source_dataset == 'fhrs'").df()
```

# Step 3: Load in pretrained splink model

First we're going to load in a precomputed table of token frequencies, for the same reason as above.

```
# Created using
# .token_and_term_frequenciesget_numeric_term_frequencies_from_address_table
path = "./example_data/numeric_token_tf_table.parquet"
sql = f"""
SELECT *
FROM read_parquet('{path}')
"""
numeric_token_freq = duckdb.sql(sql)
```


```python
linker = get_pretrained_linker(
    [df_1, df_2], precomputed_numeric_tf_table=numeric_token_freq
)
```

You can omit `precomputed_numeric_tf_table` if you want to compute on the fly.

The linker object is a splink.linker object.  Refer to the [splink documentation](https://moj-analytical-services.github.io/splink/) for more information.

# Step 4: Investigate results

We now use Splink to predict matches and investigate results

```python

df_predict = linker.predict(threshold_match_probability=0.5)

sql = f"""
select
    unique_id_l,
    unique_id_r,
    source_dataset_l,
    source_dataset_r,
    match_probability,
    match_weight,
    original_address_concat_l,
    original_address_concat_r
    from {df_predict.physical_name}
where match_weight > 0.5
order by random()
limit 10
"""

res = linker.query_sql(sql)
res

sql = f"""
select * from {df_predict.physical_name}
where match_weight > 0.5
order by random()
limit 3
"""
recs = linker.query_sql(sql).to_dict(orient="records")


for rec in recs:
    print("-" * 80)
    print(rec["unique_id_l"], rec["original_address_concat_l"])
    print(rec["unique_id_r"], rec["original_address_concat_r"])
    display(linker.waterfall_chart([rec]))

```

#Some other notes:

## Feature engineering

I've had a rough stab at feature engineering in the cleaning functions

Due to the [unusual nature of addresses](https://www.mjt.me.uk/posts/falsehoods-programmers-believe-about-addresses/) this pre-processing is a lot more sophisticated than would be done with many other entity types such as peoples' names.

Most of the decisions are fairly arbitrary, and were arrived at by experimentation - an iterative cycle of feature engineering, model training, and inspecting what the model was getting wrong.

## Model training

Splink is used to train a model. I found that training the m values did more harm than good, so I've only bothered to train the u values.

From inspecting some results, this model does a fairly decent job. Where we see errors, it's seems to be mostly because the true address does not exist. This would happen when a property has an EPC but is not in the price paid data.

But I haven't looked in any depth so your mileage may vary.

## Match weights and dasbhboard

See [comparison_viewer_dashboard](comparison_viewer_dashboard.html) and [match_weights](match_weights.html)

![image](https://github.com/RobinL/address_matching_example/assets/2608005/7f025849-c7e2-4687-b0ae-8cabbe3977b9)
