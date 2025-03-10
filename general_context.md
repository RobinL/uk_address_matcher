This package a tool for matching addresses.

It uses SQL for performance, so we should restrict our solutions to ones which can be implemented in DuckDB SQL

It expect two input datasets, called df_addresses_to_match
df_addresses_to_search_within

| Column          | Description                                      |
|-----------------|--------------------------------------------------|
| `unique_id`     | Unique identifier for each record                |
| `source_dataset`| Constant string identifying the dataset, e.g., `epc` |
| `address_concat`| Full address concatenated without postcode       |
| `postcode`      | Postcode                                         |

This makes the package very flexible as the user doesn't need to worry about the format of the address data.

In many cases df_addresses_to_search_within would be a canonical, deduplicated list of addrresses, sometimes called a gazetteer.

The package then proceeds to:
1. Clean and format the address data, splitting it into features for matching.  These are NOT similar to a human readable address but rather split out components like the various numeric tokens, the flat_positional and so on, suitable for a linkage model
2. Apply a Splink probabilistic linkage model to link the data.  The output of this step can be thought of a first pass or approximation of matching.  Each input address has a group of scored candidate matches.  Call these groups 'candidate groups'
3. The the second pass, we perform additional matching within these candidate groups, based on the idea that there's a single match within the group. This knowledge allows us to do additional analysis, such as look for which tokens are unique to only one address in the candidate group.  SO this group-specific contextual information adds greater accuracy.




# Step 3 details

Note that tokens_r are from the messy address, and tokens_l are from the canonical address.
```
┬─────────────┬─────────────┬────────────────────────────────────────────────┬───────────────────────────────────────────────┬
│ unique_id_l │ unique_id_r │                        tokens_l                │                                tokens_r       │
│    int64    │    int64    │                        varchar[]               │                                varchar[]      │
┼─────────────┼─────────────┼────────────────────────────────────────────────┼───────────────────────────────────────────────┼
│           1 │         101 │ [57, GUNTERSTONE, ROAD, LONDON]                │ [57, GUNTERSTONE, MESSY, ROAD, LONDON]        │ <- 57 is a distinguishing token within tokens_l because it only appears once in the group
│           3 │         101 │ [71, GUNTERSTONE, ROAD, LONDON]                │ [57, GUNTERSTONE, MESSY, ROAD, LONDON]        │
│           2 │         101 │ [41, GUNTERSTONE, ROAD, LONDON]                │ [57, GUNTERSTONE, MESSY, ROAD, LONDON]        │

```
```
┬─────────────┬─────────────┬────────────────────────────────────────────────┬───────────────────────────────────────────────┬
│ unique_id_l │ unique_id_r │                        tokens_l                │                                tokens_r       │
│    int64    │    int64    │                        varchar[]               │                                varchar[]      │
┼─────────────┼─────────────┼────────────────────────────────────────────────┼───────────────────────────────────────────────┼
│           6 │         202 │ [SUES, NAILS DEEZER, ROAD, LONDON]             │ [SUES, NAILS 71, DEEZER, ROAD, LONDON]        │
│           7 │         202 │ [57, DEEZER, ROAD, LONDON]                     │ [SUES, NAILS 71, DEEZER, ROAD, LONDON]        │<- This row should be punished because tokens_l DOES NOT contain the tokens SUES NAILS that DO appear in other candidates
│           8 │         202 │ [41, DEEZER, ROAD, LONDON]                     │ [SUES, NAILS 71, DEEZER, ROAD, LONDON]        │
│           9 │         202 │ [41, DEEZER, ROAD, LONDON]                     │ [SUES, NAILS 71, DEEZER, ROAD, LONDON]        │

```
