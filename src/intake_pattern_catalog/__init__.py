from ._version import __version__  # noqa
from .catalog import PatternCatalog, PatternCatalogTransform  # noqa

try:
    # Hack thing from miniver to avoid confusion
    # with __version__
    del _version  # type: ignore # noqa
except (AttributeError, NameError):
    pass
