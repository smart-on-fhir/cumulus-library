"""Error types"""


class CumulusLibraryError(Exception):
    """
    Generic package level error
    """


class AWSError(CumulusLibraryError):
    """Errors from interacting with AWS"""


class ApiError(CumulusLibraryError):
    """Errors from external API calls"""


class CountsBuilderError(CumulusLibraryError):
    """Basic error for CountsBuilder"""


class StudyImportError(CumulusLibraryError):
    """Basic error for CountsBuilder"""


class StudyManifestFilesystemError(CumulusLibraryError):
    """Errors related to files on disk in StudyManifestParser"""


class StudyManifestParsingError(CumulusLibraryError):
    """Errors related to manifest parsing in StudyManifestParser"""


class StudyManifestQueryError(CumulusLibraryError):
    """Errors related to data queries from StudyManifestParser"""
