# Configuration file for the Sphinx documentation builder.

# -- Project information

project = 'DBMSBenchmarker'
copyright = '2021, Patrick Erdelt'
author = 'Patrick Erdelt'

release = '0.1'
version = '0.1.0'

# -- General configuration

extensions = [
    'sphinx.ext.duration',
    'sphinx.ext.doctest',
    'sphinx.ext.autodoc',
    'sphinx.ext.autosummary',
    'sphinx.ext.intersphinx',
    'm2r2',
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

html_theme_options = {
    # Toc options
    'collapse_navigation': False,
    'sticky_navigation': False,
    'navigation_depth': 4,
    'globaltoc_collapse': False,
    'includehidden': True,
    'titles_only': False
}