"""Error types"""


class CumulusLibraryError(Exception):
    """
    Generic package level error
    """


class CountsBuilderError(Exception):
    """Basic error for CountsBuilder"""


class StudyManifestFilesystemError(Exception):
    """Errors related to files on disk in StudyManifestParser"""


class StudyManifestParsingError(Exception):
    """Errors related to manifest parsing in StudyManifestParser"""


class StudyManifestQueryError(Exception):
    """Errors related to data queries from StudyManifestParser"""


class AWSError(Exception):
    """Errors from interacting with AWS"""


class ApiError(Exception):
    """Errors from external API calls"""
