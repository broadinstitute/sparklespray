import importlib.metadata

try:
    __version__ = importlib.metadata.version("sparklespray")
except importlib.metadata.PackageNotFoundError:
    __version__ = "unknown"

