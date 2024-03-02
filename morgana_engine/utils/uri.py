from pathlib import Path
from urllib.parse import urlparse, unquote
from urllib.request import url2pathname


def path_to_uri(path: str, scheme: str) -> str:
    """Convert a path to a URI"""
    if scheme == "file":
        uri = Path(path).as_uri()
    elif scheme == "s3":
        uri = f"s3://{path}"
    return uri


def uri_to_path(uri: str) -> str:
    """Convert a URI to a path"""
    parsed = urlparse(uri)
    normpath = parsed.netloc + url2pathname(unquote(parsed.path))
    return normpath


def ensure_absolute_path(path: str, parent: str) -> str:
    """Ensures a given path is absolute, converting it if necessary"""
    if Path(path).is_absolute():
        return path
    else:
        return str(Path(parent).joinpath(path).absolute().resolve())


def uri_scheme(uri: str) -> str:
    """Return the scheme of a URI"""
    parsed = urlparse(uri)
    return parsed.scheme


def is_uri(uri: str) -> bool:
    """Check if a string is a valid URI"""
    try:
        result = urlparse(uri)
        return all([result.scheme, result.path])
    except Exception:
        return False
