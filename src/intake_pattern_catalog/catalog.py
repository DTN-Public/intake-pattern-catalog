import warnings
from typing import Any, Callable, Dict, List, Mapping

from fsspec.core import strip_protocol, url_to_fs
from intake.catalog import Catalog, local
from intake.catalog.utils import reload_on_change
from intake.source.base import DataSource
from intake.source.derived import GenericTransform, first
from intake.source.utils import path_to_glob, reverse_formats


class PatternCatalog(Catalog):
    """Catalog of entries as described by a path pattern (e.g. folder/{a}/{b}.csv)"""

    container = "catalog"
    partition_access = None
    name = "pattern_cat"

    def __init__(
        self,
        urlpath: str,
        driver: str,
        autoreload: bool = True,
        ttl: int = 60,
        recursive_glob: bool = False,
        listable: bool = True,
        **kwargs,
    ):
        """
        Parameters
        ----------
        urlpath: str
            Location of the file to parse (can be remote)
        driver: str
            What driver to use for each entry in the catalog (e.g. "csv")
        autoreload: bool
            Whether to watch the source file for changes; make False if you want
            an editable Catalog
        ttl: int
            How long to use the cached list of files before reloading.
        recursive_glob: bool
            Whether or not to search in nested folders to look for matching items
            (replaces * with ** for globbing purposes). Necessary if one of the pattern
            items has `/`'s in it.
        listable: bool
            Whether or not to construct a list of all the matching entries when the
            catalog is instantiated
        """
        self.urlpath = PatternCatalog._trim_prefix(urlpath)
        self.urlpath_with_fsspec_prefix = urlpath
        self.text = None
        self.autoreload = autoreload  # set this to False if don't want reloads
        self.filesystem = kwargs.pop("fs", None)
        self.driver_kwargs = kwargs.pop("driver_kwargs", {})
        self.driver = driver
        self.listable = listable
        self.recursive_glob = recursive_glob
        self.metadata = kwargs.get("metadata", {})

        self._kwarg_sets: List[Dict[str, str]] = []

        self._glob_path = path_to_glob(self.urlpath)
        if self.recursive_glob:
            self._glob_path = self._glob_path.replace("*", "**")
        if self.urlpath == self._glob_path:
            raise ValueError("Path must contain one or more `{}` patterns.")

        storage_options = kwargs.pop("storage_options", {})

        # Set use_listing_cache to False so that once the ttl runs
        # out, the fsspec cache doesn't keep the entry list from getting updated
        if "use_listings_cache" not in storage_options:
            storage_options["use_listings_cache"] = False
        super(PatternCatalog, self).__init__(
            ttl=ttl, storage_options=storage_options, **kwargs
        )

    @property
    def _pattern(self):
        return strip_protocol(self.urlpath)  # removes s3://

    @staticmethod
    def _entry_name(value_map: Mapping[str, str]) -> str:
        name = "_".join(f"{k}_{v}" for k, v in value_map.items() if v is not None)

        # Replace all non-alphanumeric characters with _
        name = "".join(c if c.isalnum() else "_" for c in name)

        # Ensure this is a valid python identifier
        assert name.isidentifier()
        return name

    def get_entry(self, **kwargs) -> DataSource:
        """
        Given a kwarg set, return the related catalog entry

        Raises a KeyError if the entry is not found
        """
        name = PatternCatalog._entry_name(kwargs)
        if not self.listable and name not in self._get_entries():
            urlpath = self.get_entry_path(**kwargs)
            if self._exists(urlpath) is False:
                raise KeyError
            entry = _local_catalog_entry(
                name=name,
                urlpath=urlpath,
                description=self.description,
                filesystem=self.filesystem,
                driver=self.driver,
                metadata=self.metadata,
                driver_kwargs=self.driver_kwargs,
                storage_options=self.storage_options,
            )
            self._entries[name] = entry
            self._kwarg_sets.append(kwargs)
        return self._get_entries()[name].get()

    def get_fs(self):
        if self.filesystem is None:
            self.filesystem = url_to_fs(self._glob_path, **self.storage_options)[0]
        return self.filesystem

    @reload_on_change
    def get_entry_kwarg_sets(self) -> List[Dict[str, str]]:
        """
        Return all the valid kwarg sets, which can be passed to get_entry to get a
        particular catalog entry
        """
        return self._kwarg_sets

    def get_entry_path(self, **kwargs) -> DataSource:
        return self.urlpath_with_fsspec_prefix.format(**kwargs)

    def _load(self, reload=False):
        # Don't try and get all the entries for very large patterns
        if not self.listable:
            return
        if self.autoreload or reload:
            try:
                # Check for permission to inspect path before attempting to expand
                # the glob. (Async globbing doesn't always raise exception.)
                self._exists(self._glob_path)
            except PermissionError as e:
                raise e

            paths = self.get_fs().glob(self._glob_path)

            patterns: Dict[str, List[str]] = reverse_formats(self._pattern, paths)
            value_names = list(patterns.keys())
            self._entries = {}
            self._kwarg_sets = []
            for values in zip(*patterns.values()):
                value_map = {k: v for k, v in zip(value_names, values)}
                self._kwarg_sets.append(value_map)
                urlpath = self.get_entry_path(**value_map)
                name = PatternCatalog._entry_name(value_map)
                entry = _local_catalog_entry(
                    name=name,
                    urlpath=urlpath,
                    description=self.description,
                    filesystem=self.filesystem,
                    driver=self.driver,
                    metadata=self.metadata,
                    driver_kwargs=self.driver_kwargs,
                    storage_options=self.storage_options,
                )
                if entry.name in self._entries:
                    warnings.warn(
                        "intake-pattern-catalog failed to generate an entry for "
                        f"pattern {value_map} because entry named {entry.name} "
                        "already exists. (Non-alphanumeric characters "
                        "are converted to underscores by Pattern Catalog driver.)"
                    )
                    continue
                self._entries[entry.name] = entry

    @staticmethod
    def _trim_prefix(urlpath):
        # Remove fsspec special prefixes from url (e.g. `simplecache::`)
        return urlpath.split("::")[-1]

    def _exists(self, urlpath: str) -> bool:
        return self.get_fs().exists(PatternCatalog._trim_prefix(urlpath))


def _local_catalog_entry(
    name: str,
    urlpath: str,
    description: str,
    filesystem: str,
    driver: str,
    metadata: Mapping[object, object],
    driver_kwargs: Mapping[str, Any],
    storage_options: Mapping[object, object],
):
    entry = local.LocalCatalogEntry(
        name=name,
        description=description,
        driver=driver,
        metadata=metadata,
        args={
            "urlpath": urlpath,
            **driver_kwargs,
            "storage_options": storage_options,
        },
    )
    entry._filesystem = filesystem
    return entry


class PatternCatalogTransformedObject:
    """
    Thin wrapper around a Datasource that overrides the read and to_dask methods
    by applying a transform to the result
    """

    def __init__(
        self,
        base_object: DataSource,
        transform: Callable,
        transform_kwargs: Mapping[str, Any] = None,
    ) -> None:
        self.base_object = base_object
        self.transform = transform
        self.transform_kwargs = transform_kwargs or {}

    def __repr__(self) -> str:
        return f"Transform wrapper around:\n{repr(self.base_object)}"

    def __str__(self) -> str:
        return f"Transform wrapper around:\n{self.base_object}"

    def read(self):
        raw = self.base_object.read()
        return self.transform(raw, **self.transform_kwargs)

    def to_dask(self):
        raw = self.base_object.to_dask()
        return self.transform(raw, **self.transform_kwargs)

    # For anything besides read and to_dask, use the underlying
    # object's properties
    def __getattr__(self, name):
        if name not in ["read", "to_dask"]:
            return getattr(self.base_object, name)


class PatternCatalogTransform(GenericTransform):
    name = "pattern_cat_transform"
    container = "catalog"
    partition_access = None
    input_container = "catalog"

    def __init__(
        self,
        targets,
        target_kwargs=None,
        metadata=None,
        transform_kwargs=None,
        target_chooser=first,
        **kwargs,
    ):
        super().__init__(
            targets,
            target_chooser=target_chooser,
            target_kwargs=target_kwargs,
            container=self.container,
            input_container=self.input_container,
            metadata=metadata,
            transform_kwargs=transform_kwargs,
            **kwargs,
        )
        self._source_picked = False

    def read(self):
        raise NotImplementedError("Must use get_entry(...).read()")

    def to_dask(self):
        raise NotImplementedError("Must use get_entry(...).to_dask()")

    def get_entry(self, **kwargs):
        if not self._source_picked:
            self._pick()
            if not isinstance(self._source, PatternCatalog):
                raise ValueError(
                    "PatternCatalogTransform only works with PatternCatalog targets"
                )
            self._source_picked = True

        entry = self._source.get_entry(**kwargs)

        transformed = PatternCatalogTransformedObject(
            entry, self._transform, self._params["transform_kwargs"]
        )

        return transformed
