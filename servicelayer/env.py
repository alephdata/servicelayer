from os import environ
from banal.bools import as_bool


def get(name, default=None):
    value = environ.get(name)
    if value is not None:
        return str(value)
    if default is not None:
        return str(default)


def to_int(name, default=0):
    """Extract an integer from the environment."""
    try:
        return int(get(name))
    except Exception:
        return default


def to_bool(name, default=False):
    """Extract a boolean value from the environment consistently."""
    return as_bool(get(name), default=default)


def to_list(name, default=[], separator=':'):
    """Extract a list of values from the environment consistently.
    Multiple values are by default expected to be separated by a colon (':'),
    like in the UNIX $PATH variable.
    """
    value = get(name)
    if value is None:
        return default
    return [e.strip() for e in value.split(separator)]
