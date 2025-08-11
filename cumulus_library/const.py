"""A list of files/filename partials to be allowed to be uploaded to an aggregator

While a valid generation target, `.archive.` should never be part of this list, since
that explicitly contains line level data.
"""

ALLOWED_UPLOADS = [".cube.", ".annotated_cube.", ".flat.", ".meta.", "manifest.toml"]
