# A first stab at address matching using Splink

This repo contains generic code for address matching using Splink. At the moment it assumes you block on postcode.

It's assumed you have two input files of addresses in this format:

| unique_id | address_concat               | postcode  |
|-----------|------------------------------|-----------|
| 1         | 123 Fake Street, Faketown    | FA1 2KE   |
| 2         | 456 Other Road, Otherville   | NO1 3WY   |
| ...       | ...                          | ...       |

Refer to [the example](example.py), which has detailed comments, for how to match your data.


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
