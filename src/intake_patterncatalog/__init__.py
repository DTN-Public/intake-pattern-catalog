from typing import Dict, List, Mapping

import fsspec
from fsspec.core import strip_protocol
from intake import registry
from intake.catalog import Catalog
from intake.source.base import DataSource, PatternMixin
from intake.source.utils import path_to_glob, reverse_formats


class PatternCatalog(Catalog, PatternMixin):
    """Catalog as described by a pattern Parquet Path"""

    version = "0.0.1"
    container = "catalog"
    partition_access = None
    name = "pattern_cat"

    def __init__(self, path, driver, autoreload=True, **kwargs):
        """
        Parameters
        ----------
        path: str
            Location of the file to parse (can be remote)
        reload : bool
            Whether to watch the source file for changes; make False if you want
            an editable Catalog
        """
        self.path = path
        self.text = None
        self.autoreload = autoreload  # set this to False if don't want reloads
        self.filesystem = kwargs.pop("fs", None)
        self.driver_kwargs = kwargs.pop("driver_kwargs", {})
        self.access = "name" not in kwargs
        self.driver = driver
        self.metadata = kwargs.get("metadata", {})
        self.storage_options = kwargs.get("storage_options", None)

        self._loaded_once = False
        self._glob_path = path_to_glob(path)
        if path == self._glob_path:
            raise ValueError("Path must contain one or more `{}` patterns.")
        self._pattern = strip_protocol(path)
        super(PatternCatalog, self).__init__(**kwargs)

    @staticmethod
    def _entry_name(value_map: Mapping[str, str]) -> str:
        name = "_".join(f"{k}_{v}" for k, v in value_map.items() if v is not None)

        # Replace all non-alphanumeric characters with _
        name = "".join([c if c.isalnum() else "_" for c in name])

        # Ensure this is a valid python identifier
        assert name.isidentifier()
        return name

    def get_entry(self, **kwargs) -> DataSource:
        name = self._entry_name(kwargs)
        return self._entries[name]

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
                self._glob_path,
                storage_options=self.storage_options
            )
            patterns: Dict[str, List[str]] = reverse_formats(self._pattern, paths)
            value_names = list(patterns.keys())
            self._entries = {}
            for values in zip(*patterns.values()):
                value_map = {k: v for k, v in zip(value_names, values)}
                path = self.path.format(**value_map)
                entry = registry[self.driver](
                    urlpath=path, metadata=self.metadata, storage_options=self.storage_options, **self.driver_kwargs
                )

                entry._catalog = self
                entry.name = PatternCatalog._entry_name(value_map)
                self._entries[entry.name] = entry
                entry._filesystem = self.filesystem
