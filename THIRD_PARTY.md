# Third-party material

DBMS-Benchmarker is licensed under the GNU Affero General Public License v3.0 or later (see [LICENSE](LICENSE)).
This file lists material from others that is included in the repository or needed to run it, with its licence.

## Included in the repository

| Material | Location | Source | Licence |
|---|---|---|---|
| Dashboard icons | `dbmsbenchmarker/scripts/assets/icons/*.svg` | [Material Icons](https://github.com/google/material-design-icons) by Google | Apache-2.0 |
| TPC-H query texts | `example/tpc-h/queries.config` | Derived from the [TPC-H specification](https://www.tpc.org/tpch/) | See the note on TPC below |
| TPC-DS query texts | `example/tpc-ds/queries.config` | Derived from the [TPC-DS specification](https://www.tpc.org/tpcds/) | See the note on TPC below |

### Note on TPC

TPC-H and TPC-DS are trademarks of the Transaction Processing Performance Council (TPC).
The query texts in `example/` are derived from the TPC specifications and adapted to several SQL dialects.
Results obtained with DBMS-Benchmarker are not TPC benchmark results: they have not been audited, do not follow all rules of the specifications, and must not be compared with published TPC results.

## Not included: JDBC drivers

DBMS-Benchmarker connects to database systems through JDBC drivers.
The drivers are not part of the repository or the Python package; users supply them for the systems they benchmark (see the `JDBC` entries in `connections.config`).
Each driver is subject to the licence of its vendor.

## Python dependencies

The dependencies in [requirements.txt](requirements.txt) are installed by pip and not distributed with DBMS-Benchmarker.
Their licences, as stated in the metadata of the installed versions (checked 2026-11-18):

| Package | Licence |
|---|---|
| JayDeBeApi | LGPL-3.0 |
| JPype1 | Apache-2.0 |
| pandas, numpy, scipy | BSD-3-Clause |
| matplotlib | Matplotlib licence (PSF-based) |
| dash, dash-auth, dash-core-components, dash-daq, dash-html-components, dash-renderer, dash-table, plotly | MIT |
| Flask, Werkzeug, click, itsdangerous, MarkupSafe, jinja2, idna, colour | BSD-3-Clause |
| Flask-Caching, cycler, kiwisolver, Bottleneck | BSD |
| tabulate, Brotli, future, pyparsing, pytz, six, zipp, urllib3, charset_normalizer, setuptools, fonttools | MIT |
| tqdm | MPL-2.0 and MIT |
| certifi | MPL-2.0 |
| requests, retrying | Apache-2.0 |
| python-dateutil | Apache-2.0 or BSD-3-Clause (dual) |
| mistune | BSD-3-Clause |
| pillow | MIT-CMU |
| uuid | Not stated in the package metadata |
