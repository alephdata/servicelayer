from servicelayer import settings
from servicelayer.archive.file import FileArchive

ARCHIVE_FILE = 'file'
ARCHIVE_S3 = 's3'
ARCHIVE_GS = 'gs'


def init_archive(archive_type=settings.ARCHIVE_TYPE,
                 path=settings.ARCHIVE_PATH,
                 bucket=settings.ARCHIVE_BUCKET):
    """Instantiate an archive object."""
    if archive_type == ARCHIVE_S3:
        from servicelayer.archive.s3 import S3Archive
        return S3Archive(bucket=bucket)

    if archive_type == ARCHIVE_GS:
        from servicelayer.archive.gs import GoogleStorageArchive
        return GoogleStorageArchive(bucket=bucket)

    return FileArchive(path=path)


def init_publication_archive(archive_type=settings.ARCHIVE_TYPE,
                             path=settings.PUBLICATION_PATH,
                             bucket=settings.PUBLICATION_BUCKET):
    """Instatiate an archive to publish files"""
    return init_archive(archive_type, path, bucket)
