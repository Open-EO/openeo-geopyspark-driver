class UDF_DEPENDENCIES_INSTALL_MODE:
    """UDF dependency install modes"""

    # TODO: migrate to StrEnum once >=py3.11

    "Don't handle UDF dependencies"
    DISABLED = "disabled"

    "Install dependencies directly in job's work folder"
    DIRECT = "direct"

    "Provide UDF dependencies as zip archive in job's work folder, to be extracted on the fly"
    ZIP = "zip"
