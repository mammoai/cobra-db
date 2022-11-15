# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------

project = "cobra_db"
copyright = "2022, Fernando Cossio, Apostolia Tsirikoglou, Haiko Schurz, Fredrik Strand"
author = "Fernando Cossio, Apostolia Tsirikoglou, Haiko Schurz, Fredrik Strand"

# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    "myst_nb",
    "autoapi.extension",
    "sphinx.ext.napoleon",
    "sphinx.ext.viewcode",
]
autoapi_dirs = ["../src"]

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

myst_enable_extensions = ["html_image"]  # Allows to use html images in markdown
# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = "sphinx_rtd_theme"

html_static_path = ["static"]
html_logo = "static/img/cobra_db.png"
html_theme_options = {
    "logo_only": False,
    "display_version": False,
}

# Myst cache output of notebooks
jupyter_execute_notebooks = "force"
