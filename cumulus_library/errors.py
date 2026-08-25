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


class FileUploadError(CumulusLibraryError):
    """Basic error for FileUploadBuilder"""


class StudyImportError(CumulusLibraryError):
    """Basic error for study importing"""


class StudyManifestFilesystemError(CumulusLibraryError):
    """Errors related to files on disk in StudyManifestParser"""


class StudyManifestParsingError(CumulusLibraryError):
    """Errors related to manifest parsing in StudyManifestParser"""


class StudyManifestQueryError(CumulusLibraryError):
    """Errors related to data queries from StudyManifestParser"""


class NlpExecutionError(CumulusLibraryError):
    """Errors related to NLP runs"""


# More specific than NLP errors are mlflow errors
class MlflowExecutionError(NlpExecutionError):
    """Errors related to NLP runs"""
