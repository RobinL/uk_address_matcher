# Matching UK addresses using Splink

This repo contains generic code for address matching using a pre-trained Splink model.

It's assumed you have two input files of addresses in this format:

| unique_id | address_concat               | postcode  |
|-----------|------------------------------|-----------|
| 1         | 123 Fake Street, Faketown    | FA1 2KE   |
| 2         | 456 Other Road, Otherville   | NO1 3WY   |
| ...       | ...                          | ...       |

Refer to [the example](example.py), which has detailed comments, for how to match your data.

See [an example of comparing two addresses](example_compare_two.py) to get a sense of what it does/how it scores

Run an interactive example in your browser
[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/RobinL/uk_address_matcher/blob/main/interactive_comparison.ipynb)
