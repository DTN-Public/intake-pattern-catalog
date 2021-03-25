from typing import Dict, List, Mapping

import fsspec
from fsspec.core import strip_protocol
from intake import registry
from intake.catalog import Catalog
from intake.catalog.utils import reload_on_change
from intake.source.base import DataSource, PatternMixin
from intake.source.utils import path_to_glob, reverse_formats


class PatternCatalog(Catalog, PatternMixin):
    """Catalog as described by a pattern Parquet Path"""

    version = "0.0.2"
    container = "catalog"
    partition_access = None
    name = "pattern_cat"

    def __init__(self, path, driver, autoreload=True, ttl=60, **kwargs):
        """
        Parameters
        ----------
        path: str
            Location of the file to parse (can be remote)
        reload: bool
            Whether to watch the source file for changes; make False if you want
            an editable Catalog
        ttl: int
            How long to use the cached list of files before reloading.
        """
        self.path = path
        self.text = None
        self.autoreload = autoreload  # set this to False if don't want reloads
        self.filesystem = kwargs.pop("fs", None)
        self.driver_kwargs = kwargs.pop("driver_kwargs", {})
        self.access = "name" not in kwargs
        self.driver = driver
        self.metadata = kwargs.get("metadata", {})

        self._kwarg_sets: List[Dict[str, str]] = []

        self._loaded_once = False
        self._glob_path = path_to_glob(path)
        if path == self._glob_path:
            raise ValueError("Path must contain one or more `{}` patterns.")

        storage_options = kwargs.get("storage_options", {})

        # Set use_listing_cache to False so that once the ttl runs
        # out, the fsspec cache doesn't keep the entry list from getting updated
        if "listings_expiry_time" not in storage_options:
            storage_options["use_listings_cache"] = False

        print("STORAGE OPTIONS", storage_options)

        super(PatternCatalog, self).__init__(
            ttl=ttl, storage_options=storage_options, **kwargs
        )

    @property
    def _pattern(self):
        if "::" not in self.path:
            path = self.path
        else:
            # removes simplecache:: or similar
            path = self.path.split("::")[1]
        return strip_protocol(path)  # removes s3://

    @staticmethod
    def _entry_name(value_map: Mapping[str, str]) -> str:
        name = "_".join(f"{k}_{v}" for k, v in value_map.items() if v is not None)

        # Replace all non-alphanumeric characters with _
        name = "".join([c if c.isalnum() else "_" for c in name])

        # Ensure this is a valid python identifier
        assert name.isidentifier()
        return name

    def get_entry(self, **kwargs) -> DataSource:
        """
        Given a kwarg set, return the related catalog entry
        """
        name = self._entry_name(kwargs)
        return self._get_entries()[name]

    @reload_on_change
    def get_entry_kwarg_sets(self) -> List[Dict[str, str]]:
        """
        Return all the valid kwarg sets, which can be passed to get_entry to get a
        particular catalog entry
        """
        return self._kwarg_sets

    def get_entry_path(self, **kwargs) -> DataSource:
        return self.path.format(**kwargs)

    def _load(self, reload=False):
        if self.access is False:
            # skip first load, if cat has given name (i.e., is subcat)
            self.updated = 0
            self.access = True
            return
        if self.autoreload or reload:
            fs, _, paths = fsspec.get_fs_token_paths(
                self._glob_path, storage_options=self.storage_options
            )
            patterns: Dict[str, List[str]] = reverse_formats(self._pattern, paths)
            value_names = list(patterns.keys())
            self._entries = {}
            self._kwarg_sets = []
            for values in zip(*patterns.values()):
                value_map = {k: v for k, v in zip(value_names, values)}
                self._kwarg_sets.append(value_map)
                path = self.path.format(**value_map)
                entry = registry[self.driver](
                    urlpath=path,
                    metadata=self.metadata,
                    storage_options=self.storage_options,
                    **self.driver_kwargs,
                )

                entry._catalog = self
                entry.name = PatternCatalog._entry_name(value_map)
                self._entries[entry.name] = entry
                entry._filesystem = self.filesystem
