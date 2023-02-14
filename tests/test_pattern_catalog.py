from pathlib import Path
from tempfile import TemporaryDirectory
from time import sleep
from typing import Generator

import intake
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from intake_pattern_catalog import PatternCatalog, PatternCatalogTransform


@pytest.fixture(
    params=["file://{file}.csv", "simplecache::file://{file}.csv", "{file}.csv"]
)
def empty_catalog(request):
    return PatternCatalog(urlpath=request.param, driver="csv")


@pytest.fixture(scope="function")
def example_bucket(s3):
    bucket_name = "example-bucket"
    s3.create_bucket(Bucket=bucket_name)
    return bucket_name


def test_pattern_generation(empty_catalog):
    """_pattern property removes filesystem prefix and caching prefix"""
    actual = Path.cwd() / "{file}.csv"
    assert empty_catalog._pattern == str(actual)


def test_no_ttl_s3(example_bucket, s3):

    cat = PatternCatalog(
        name="cat",
        urlpath="s3://" + example_bucket + "/{num}.csv",
        driver="csv",
        ttl=-1,
    )
    assert cat.get_entry_kwarg_sets() == []
    s3.put_object(Body="", Bucket=example_bucket, Key="1.csv")
    s3.put_object(Body="", Bucket=example_bucket, Key="2.csv")
    assert cat.get_entry_kwarg_sets() == [{"num": "1"}, {"num": "2"}]


def test_ttl_s3(example_bucket, s3):
    cat = PatternCatalog(
        name="cat",
        urlpath="s3://" + example_bucket + "/{num}.csv",
        driver="csv",
        ttl=0.1,
    )
    assert cat.get_entry_kwarg_sets() == []
    s3.put_object(Body="", Bucket=example_bucket, Key="1.csv")
    s3.put_object(Body="", Bucket=example_bucket, Key="2.csv")
    assert cat.get_entry_kwarg_sets() == []
    sleep(0.11)
    assert cat.get_entry_kwarg_sets() == [{"num": "1"}, {"num": "2"}]


def test_ttl_s3_parquet(example_bucket, s3):
    cat = PatternCatalog(
        name="cat",
        urlpath="s3://" + example_bucket + "/{num}.parquet",
        driver="parquet",
        ttl=0.1,
    )
    assert cat.get_entry_kwarg_sets() == []
    s3.put_object(Body="", Bucket=example_bucket, Key="1.parquet")
    s3.put_object(Body="", Bucket=example_bucket, Key="2.parquet")
    assert cat.get_entry_kwarg_sets() == []
    sleep(0.11)
    assert cat.get_entry_kwarg_sets() == [{"num": "1"}, {"num": "2"}]


@pytest.fixture
def folder_with_csvs() -> Generator[str, None, None]:
    with TemporaryDirectory() as tempdir:
        for i in range(10):
            Path(tempdir, f"{i}.csv").write_text(f"a\n{i}")
        yield str(tempdir)


def test_unlistable_cat(folder_with_csvs: str):
    cat = PatternCatalog(
        name="cat",
        urlpath=str(Path(folder_with_csvs, "{num}.csv")),
        driver="csv",
        listable=False,
    )
    # Make sure an unlistable catalog doesn't have any entries initially
    assert len(list(cat)) == 0
    # Make sure I can access a valid entry without error
    assert cat.get_entry(num=1)
    # After valid entry is accessed, make sure entries has been populated
    assert len(list(cat)) == 1
    assert cat.get_entry(num=1)
    # After valid entry is accessed again,
    # make sure entry hasn't been duplicated in list
    assert len(list(cat)) == 1
    # Check other valid entry
    assert cat.get_entry(num=5)
    # After 2 valid entries are accessed,
    # make sure entries has been populated with 2 entries
    assert len(list(cat)) == 2
    # Make sure accessing invalid entry raises a KeyError
    with pytest.raises(KeyError):
        cat.get_entry(num=-1)


@pytest.fixture
def recursive_s3(s3) -> str:
    bucket_name = "recursive"
    s3.create_bucket(Bucket=bucket_name)
    s3.put_object(Body="", Bucket=bucket_name, Key="nested/path/1.csv")
    s3.put_object(Body="", Bucket=bucket_name, Key="nested/path/2.csv")
    s3.put_object(Body="", Bucket=bucket_name, Key="3.csv")
    return "s3://" + bucket_name + "/{path}.csv"


def test_recursive_s3(recursive_s3: str):
    cat = PatternCatalog(
        name="cat",
        urlpath=recursive_s3,
        driver="csv",
        recursive_glob=True,
    )
    assert cat.get_entry_kwarg_sets() == [
        {"path": "3"},
        {"path": "nested/path/1"},
        {"path": "nested/path/2"},
    ]


def test_non_recursive_s3(recursive_s3: str):
    cat = PatternCatalog(
        name="cat",
        urlpath=recursive_s3,
        driver="csv",
        recursive_glob=False,
    )
    assert cat.get_entry_kwarg_sets() == [
        {"path": "3"},
    ]


def test_warn_on_duplicates(example_bucket, s3):
    s3.put_object(Body="", Bucket=example_bucket, Key="🧨.csv")
    s3.put_object(Body="", Bucket=example_bucket, Key="💣.csv")
    with pytest.warns(UserWarning, match="failed to generate an entry"):
        cat = PatternCatalog(
            name="cat",
            urlpath="s3://" + example_bucket + "/{num}.csv",
            driver="csv",
            autoreload=True,
        )
    assert len(cat) == 1


def test_walk(example_bucket, s3):

    s3.put_object(Body="", Bucket=example_bucket, Key="abcd.csv")
    s3.put_object(Body="", Bucket=example_bucket, Key="efgh.csv")

    cat = PatternCatalog(
        name="cat",
        urlpath="s3://" + example_bucket + "/{num}.csv",
        driver="csv",
        autoreload=True,
    )
    assert len(cat.walk()) == 2
    assert "num_abcd" in cat.walk()
    assert "num_efgh" in cat.walk()


def test_search(example_bucket, s3):

    s3.put_object(Body="", Bucket=example_bucket, Key="abcd.csv")
    s3.put_object(Body="", Bucket=example_bucket, Key="efgh.csv")

    cat = PatternCatalog(
        name="search_requires_a_name",
        urlpath="s3://" + example_bucket + "/{num}.csv",
        driver="csv",
        autoreload=True,
    )
    assert len(cat.search("abcd")) == 1
    assert len(cat.search("efgh")) == 1
    assert len(cat.search("abcd efgh")) == 2
    assert len(cat.search("wxyz")) == 0


def unity_transform(df: pd.DataFrame) -> pd.DataFrame:
    return df


def test_derived_dataset_unity(folder_with_csvs: str):
    cat = PatternCatalog(
        name="catalog_to_transform",
        urlpath=str(Path(folder_with_csvs, "{num}.csv")),
        driver="csv",
    )

    derived_cat = PatternCatalogTransform(
        targets=[cat],
        transform=unity_transform,
        target_kwargs=None,
        transform_kwargs=None,
        metadata=None,
    )

    assert_frame_equal(derived_cat.get_entry(num=1).read(), cat.get_entry(num=1).read())


def double_transform(df: pd.DataFrame) -> pd.DataFrame:
    return df * 2


def test_derived_dataset_with_transform(folder_with_csvs: str):
    cat = PatternCatalog(
        name="catalog_to_transform",
        urlpath=str(Path(folder_with_csvs, "{num}.csv")),
        driver="csv",
    )

    derived_cat = PatternCatalogTransform(
        targets=[cat],
        transform=double_transform,
        target_kwargs=None,
        transform_kwargs=None,
        metadata=None,
    )

    assert_frame_equal(
        derived_cat.get_entry(num=1).read(),
        double_transform(cat.get_entry(num=1).read()),
    )


def test_derived_dataset_with_transform_to_dask(folder_with_csvs: str):
    cat = PatternCatalog(
        name="catalog_to_transform",
        urlpath=str(Path(folder_with_csvs, "{num}.csv")),
        driver="csv",
    )

    derived_cat = PatternCatalogTransform(
        targets=[cat],
        transform=double_transform,
        target_kwargs=None,
        transform_kwargs=None,
        metadata=None,
    )

    assert_frame_equal(
        derived_cat.get_entry(num=1).to_dask().compute(),
        double_transform(cat.get_entry(num=1).to_dask().compute()),
    )


def test_derived_dataset_with_transform_other_properties(folder_with_csvs: str):
    cat = PatternCatalog(
        name="catalog_to_transform",
        urlpath=str(Path(folder_with_csvs, "{num}.csv")),
        driver="csv",
        description="Description",
    )

    derived_cat = PatternCatalogTransform(
        targets=[cat],
        transform=double_transform,
        target_kwargs=None,
        transform_kwargs=None,
        metadata=None,
    )

    derived_entry = derived_cat.get_entry(num=1)
    entry = cat.get_entry(num=1)

    for attr in ["urlpath", "storage_options", "yaml", "description"]:
        assert getattr(derived_entry, attr) == getattr(entry, attr)


def multiply_transform(df: pd.DataFrame, multiply_by: float) -> pd.DataFrame:
    return df * multiply_by


def test_derived_dataset_with_kwargs(folder_with_csvs: str):
    cat = PatternCatalog(
        name="catalog_to_transform",
        urlpath=str(Path(folder_with_csvs, "{num}.csv")),
        driver="csv",
    )

    transform_kwargs = {"multiply_by": 5}
    derived_cat = PatternCatalogTransform(
        targets=[cat],
        transform=multiply_transform,
        target_kwargs=None,
        transform_kwargs=transform_kwargs,
        metadata=None,
    )

    assert_frame_equal(
        derived_cat.get_entry(num=1).read(),
        multiply_transform(cat.get_entry(num=1).read(), **transform_kwargs),
    )


@pytest.fixture
def yaml_catalog():
    return intake.open_catalog("tests/test.yaml")


def test_yaml(yaml_catalog):
    entry = yaml_catalog.folder_with_csvs.get_entry(num=1)
    assert entry.read()["a"][0] == 1


def test_yaml_transformed(yaml_catalog):
    entry = yaml_catalog.folder_with_csvs_transformed.get_entry(num=1)
    assert entry.read()["a"][0] == 2


def test_globbed_files(tmp_path):
    (tmp_path / "a").mkdir()
    pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]}).to_csv(
        tmp_path / "a" / "df1.csv"
    )
    pd.DataFrame(data={"col1": [3, 4], "col2": [5, 6]}).to_csv(
        tmp_path / "a" / "df2.csv"
    )
    globbed_df = (
        intake.open_pattern_cat(
            urlpath=f"{tmp_path}/{{folder}}/*.csv", listable=False, driver="csv"
        )
        .get_entry(folder="a")
        .read()
    )
    assert len(globbed_df) == 4
