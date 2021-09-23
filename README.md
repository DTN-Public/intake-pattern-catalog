# Intake Pattern Catalog

intake-pattern-catalog is a [plugin](https://intake.readthedocs.io/en/latest/plugin-directory.html) for Intake
which allows you to specify a file-path pattern which can represent a number of different entries.

_Note that this is different from the patterns you can write with the csv driver which get turned into a single entry_

![](dtn.png)

## Installation instructions

```bash
pip install intake-pattern-catalog
# or
conda install intake-pattern-catalog
```

## Usage

Use `driver: pattern_cat` to use this driver in your catalogs.

Consider the following list of files in an S3 bucket:

* bucket-name/folder/a_1.csv
* bucket-name/folder/b_1.csv
* bucket-name/folder/c_1.csv
* bucket-name/folder/a_2.csv
* bucket-name/folder/b_2.csv

And the following catalog definition yaml file:
```yaml
---
metadata:
  version: 1
sources:
  stuff:
    description: Stuff and things
    driver: pattern_cat
    args:
      urlpath: "s3://bucket-name/folder/{foo}_{bar}.csv"
      driver: csv
```

### Derived datasets

If you would like to create a
[derived dataset](https://intake.readthedocs.io/en/latest/transforms.html) based on a
`pattern_cat` dataset, you can use `driver: pattern_cat_transform`, which will apply
a transformation function to each entry returned by `get_entry`. For example, you can
add to the above example yaml file:
```yaml
  stuff_transformed:
    description: Everything in stuff, doubled
    driver: pattern_cat_transform
    args:
      targets:
        - stuff
      transform: "path.to.doubling_function"
```

## Catalog API

### Access entry by kwargs:
```python
> catalog.stuff.get_entry(foo='a', bar=1)
sources:
  foo_a_bar_1:
    args:
      storage_options:
        use_listings_cache: false
      urlpath: s3://bucket-name/folder/a_1.csv
    description: ''
    driver: intake.source.csv.CSVSource
    metadata:
      catalog_dir: ...
```
_Note that this could also be accessed with `catalog.stuff.foo_a_bar_1`_

### See all valid kwarg combinations:
```python
> catalog.stuff.get_entry_kwarg_sets()
[
    {"foo": "a", "bar": "1"},
    {"foo": "b", "bar": "1"},
    {"foo": "c", "bar": "1"},
    {"foo": "a", "bar": "2"},
    {"foo": "b", "bar": "2"},
]
```

## Caching

The default way of controlling any caching with a pattern-catalog is using a `ttl` (in seconds),
which is an optional value under `args` which specifies how long should wait after fetching a list of files
which match the pattern before it loads them again. The default `ttl` is 60 seconds.
If you want to force it to always get the latest list of available entries, set the `ttl` to 0.