import logging

from servicelayer import settings


def configure_logging():
    """setup logging configuration and return current log level"""
    level = getattr(logging, settings.LOGGING_LEVEL, None)
    if level is None:
        raise ValueError('Invalid log level: %s' % settings.LOGGING_LEVEL)
    logging.basicConfig(level=level, format=settings.LOGGING_FORMAT)
    return level
