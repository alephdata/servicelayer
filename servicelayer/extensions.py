from pkg_resources import iter_entry_points

EXTENSIONS = {}


def get_extensions(section):
    """Load all Python classes registered at a given entry point."""
    if section not in EXTENSIONS:
        EXTENSIONS[section] = {}
    if not EXTENSIONS[section]:
        for ep in iter_entry_points(section):
            EXTENSIONS[section][ep.name] = ep.load()
    return list(EXTENSIONS[section].values())
