# Configuration file for the Sphinx documentation builder.

# -- Project information

project = 'DBMSBenchmarker'
copyright = '2021, Patrick K. Erdelt'
author = 'Patrick K. Erdelt'

import importlib.metadata

# Replace 'your-package-name' with the actual name of your package
release = importlib.metadata.version('dbmsbenchmarker')
version = release

#release = '0.13.2'
#version = '0.13.2'

# -- General configuration

extensions = [
    'sphinx.ext.duration',
    'sphinx.ext.doctest',
    'sphinx.ext.autodoc',
    'sphinx.ext.autosummary',
    'sphinx.ext.intersphinx',
    'sphinx_mdinclude',
    #'myst_parser',
    #'m2r2',
]


intersphinx_mapping = {
    'python': ('https://docs.python.org/3/', None),
    'sphinx': ('https://www.sphinx-doc.org/en/master/', None),
}
intersphinx_disabled_domains = ['std']

templates_path = ['_templates']

# -- Options for HTML output

html_theme = 'sphinx_rtd_theme'
html_static_path = ['_static']

# -- Options for EPUB output
epub_show_urls = 'footnote'

#source_suffix = ['.rst', '.md']

todo_include_todos = True

import os
import sys
#sys.path.insert(0, os.path.abspath('.'))
sys.path.insert(0, os.path.abspath('../'))
