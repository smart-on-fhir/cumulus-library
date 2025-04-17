"""Package metadata"""

from cumulus_library.base_utils import StudyConfig
from cumulus_library.builders.base_table_builder import BaseTableBuilder
from cumulus_library.builders.counts import CountsBuilder
from cumulus_library.study_manifest import StudyManifest
from cumulus_library.template_sql.base_templates import get_template

# A note about the `get_template` function:
# As of this writing, the get_template function is used internally by the
# Cumulus team across multiple study repos for creating SQL in databases, both
# from the templates in this repo and in the various other projects (while leveraging
# some of our syntax helper macros, which are auto loaded by this function).
# Breaking changes to these templates will result in a major version bump.

# This API should be usable for your own study efforts - but the documentation
# is all code level. See template_sql for more information.

__all__ = ["BaseTableBuilder", "CountsBuilder", "StudyConfig", "StudyManifest", "get_template"]
__version__ = "4.5.1"
