import warnings
from typing import Dict, List, Mapping

from fsspec.core import strip_protocol, url_to_fs
from intake import registry
from intake.catalog import Catalog, local
from intake.catalog.utils import reload_on_change
from intake.source.base import DataSource
from intake.source.utils import path_to_glob, reverse_formats

from ._version import __version__

try:
    # Hack thing from miniver to avoid confusion
    # with __version__
    del _version  # type: ignore # noqa
except (AttributeError, NameError):
    pass


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
        self.urlpath = urlpath
        self.text = None
        self.autoreload = autoreload  # set this to False if don't want reloads
        self.filesystem = kwargs.pop("fs", None)
        self.driver_kwargs = kwargs.pop("driver_kwargs", {})
        self.access = "name" not in kwargs
        self.driver = driver
        self.listable = listable
        self.recursive_glob = recursive_glob
        self.metadata = kwargs.get("metadata", {})

        self._kwarg_sets: List[Dict[str, str]] = []

        self._loaded_once = False
        self._glob_path = path_to_glob(urlpath)
        if self.recursive_glob:
            self._glob_path = self._glob_path.replace("*", "**")
        if urlpath == self._glob_path:
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
        if "::" not in self.urlpath:
            urlpath = self.urlpath
        else:
            # removes simplecache:: or similar
            urlpath = self.urlpath.split("::")[1]
        return strip_protocol(urlpath)  # removes s3://

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
            if not self.get_fs().exists(urlpath):
                raise KeyError
            entry = local.LocalCatalogEntry(
                name=name,
                description=self.description,
                driver=self.driver,
                metadata=self.metadata,
                args={
                    "urlpath": urlpath,
                    **self.driver_kwargs,
                    "storage_options": self.storage_options,
                },
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
        return self.urlpath.format(**kwargs)

    def _load(self, reload=False):
        # Don't try and get all the entries for very large patterns
        if not self.listable:
            return
        if self.access is False:
            # skip first load, if cat has given name (i.e., is subcat)
            self.updated = 0
            self.access = True
            return
        if self.autoreload or reload:
            try:
                # Check for permission to inspect path before attempting to expand
                # the glob. (Async globbing doesn't always raise exception.)
                self.get_fs().exists(self._glob_path)
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
                urlpath = self.urlpath.format(**value_map)
                entry = registry[self.driver](
                    urlpath=urlpath,
                    metadata=self.metadata,
                    storage_options=self.storage_options,
                    **self.driver_kwargs,
                )

                entry._catalog = self
                entry.name = PatternCatalog._entry_name(value_map)
                if entry.name in self._entries:
                    warnings.warn(
                        "intake-patterncatalog failed to generate an entry for "
                        f"pattern {value_map} because entry named {entry.name} "
                        "already exists. (Non-alphanumeric characters "
                        "are converted to underscores by Pattern Catalog driver.)"
                    )
                    continue
                self._entries[entry.name] = entry
                entry._filesystem = self.filesystem
