"""Configuration of the documentation."""

project = "datakiln"
author = "Lucas Bulgarelli"
copyright = "2023, Lucas Bulgarelli"  # pylint: disable=redefined-builtin

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.viewcode",
]
autodoc_member_order = "bysource"

html_theme = "alabaster"
exclude_patterns = ["_build"]
