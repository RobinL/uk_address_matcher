# A first stab at address matching using Splink

# Data sources

There are two data sources. You need an account to download the datasets.

### 1. Price paid data

https://www.gov.uk/government/statistical-data-sets/price-paid-data-downloads
I'm using the complete dataset, at the time of writing this is at:
http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-complete.csv

### 2. Energy performance certificate data

https://epc.opendatacommunities.org/downloads/domestic
I'm used the Adur dataset, at the time of writing this is at:
https://epc.opendatacommunities.org/files/domestic-E07000223-Adur.zip

Note, you need an account to download both.

## Data preparation

See [01_process_price_paid.py](01_process_price_paid.py) to prep the csv files

Note that you'll need to edit the file_path variables to point to the correct location of the csv files on your machine.

## Feature engineering

I've had a rough stab at feature engineering.

Due to the [unusual nature of addresses](https://www.mjt.me.uk/posts/falsehoods-programmers-believe-about-addresses/) this pre-processing is a lot more sophisticated than would be done with many other entity types such as peoples' names.

Most of the decisions are fairly arbitrary, and were arrived at by experimentation - an iterative cycle of feature engineering, model training, and inspecting what the model was getting wrong.

See [02_feature_engineering.py](02_feature_engineering.py) for the code.

## Model training

Splink is used to train a model. I found that training the m values did more harm than good, so I've only bothered to train the u values.

From inspecting some results, this model does a fairly decent job. Where we see errors, it's seems to be mostly because the true address does not exist. This would happen when a property has an EPC but is not in the price paid data.

But I haven't looked in any depth so your mileage may vary.

## Match weights and dasbhboard

See [comparison_viewer_dashboard](comparison_viewer_dashboard.html) and [match_weights](match_weights.html)

![image](https://github.com/RobinL/address_matching_example/assets/2608005/7f025849-c7e2-4687-b0ae-8cabbe3977b9)


# Applying this methodology to a different dataset

The main assumption made in this methodology is that the postcode is correct.

Apart from that, the main input into the [02_feature_engineering.py](02_feature_engineering.py) script is a dataframe with the full address separated into two parts: address_concat and postcode.

To use with different data, modify the [02_feature_engineering.py](02_feature_engineering.py) script up to [df_vertical_concat](https://github.com/RobinL/address_matching_example/blob/eeeb95c1c869586217496cba84c86112312ae295/02_feature_eng.py#L92) such that your data is in the same format (unique_id, source_daaset, address_concat, postcode).

The rest of the script should work as is.

I would recommend starting fairly small in terms of number of rows, say about 10k-50k rows. Then scale up if everything is working fine.
