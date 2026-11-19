# FAIR4RS Self-Assessment

This page assesses DBMS-Benchmarker against the [FAIR Principles for Research Software (FAIR4RS)](https://doi.org/10.15497/RDA00068) (Barker et al., *Scientific Data* 9, 622, 2022, [doi:10.1038/s41597-022-01710-x](https://doi.org/10.1038/s41597-022-01710-x)).

Assessed on 2026-11-19 for version 0.15.10.
Each principle is rated **met**, **partly met** or **not met**, with the evidence in the repository or in public services.
Open points are listed at the end.

## Findable

| Principle | Status | Evidence |
|---|---|---|
| F1. Software is assigned a globally unique and persistent identifier. | met | Concept DOI [10.5281/zenodo.6382235](https://doi.org/10.5281/zenodo.6382235), which always resolves to the latest release. |
| F1.1. Components of the software representing levels of granularity are assigned distinct identifiers. | partly met | The package is released and identified as a whole. Its components (benchmarker, evaluation, dashboard, the formats of the configuration and result files) have no identifiers of their own. |
| F1.2. Different versions of the software are assigned distinct identifiers. | met | Every GitHub release (tag `vX.Y.Z`) is archived by Zenodo with its own version DOI, listed under the concept DOI. Versions on [PyPI](https://pypi.org/project/dbmsbenchmarker/) carry the same version number. |
| F2. Software is described with rich metadata. | met | [`CITATION.cff`](https://github.com/Beuth-Erdelt/DBMS-Benchmarker/blob/master/CITATION.cff), [`codemeta.json`](https://github.com/Beuth-Erdelt/DBMS-Benchmarker/blob/master/codemeta.json), [`.zenodo.json`](https://github.com/Beuth-Erdelt/DBMS-Benchmarker/blob/master/.zenodo.json) and `pyproject.toml`: description, authors with ORCID and affiliation, licence, keywords, related publications, repository, documentation, issue tracker, runtime requirements. |
| F3. Metadata clearly and explicitly include the identifier of the software they describe. | met | The concept DOI is in `CITATION.cff`, `codemeta.json` and the README badge; each Zenodo record states its version DOI. |
| F4. Metadata are FAIR, searchable and indexable. | met | Zenodo registers the metadata with DataCite; PyPI and GitHub index the package and the repository. |

## Accessible

| Principle | Status | Evidence |
|---|---|---|
| A1. Software is retrievable by its identifier using a standardised communications protocol. | met | DOIs resolve over HTTPS to Zenodo, which offers the source archive of each release. The package installs with `pip install dbmsbenchmarker`. |
| A1.1. The protocol is open, free, and universally implementable. | met | HTTPS. |
| A1.2. The protocol allows for an authentication and authorization procedure, where necessary. | met | Not necessary: software and metadata are public without authentication. |
| A2. Metadata are accessible, even when the software is no longer available. | met | Zenodo keeps the metadata and the archived source of each version independently of GitHub and PyPI. |

## Interoperable

| Principle | Status | Evidence |
|---|---|---|
| I1. Software reads, writes and exchanges data in a way that meets domain-relevant community standards. | partly met | Systems under test are reached through JDBC and SQL, the common interfaces of relational DBMS. Hardware metrics are read from Prometheus. Measurements are written as CSV (timers, monitoring metrics), the protocol and the evaluation as JSON (`protocol.json`, `evaluation.json`). Result sets are stored as CSV or as pickled pandas DataFrames, as the user chooses (`--store-data csv`, or `'store': ['csv']` per query). However, two kinds of files are always pickled pandas DataFrames, which only Python can read and which depend on the pandas version: the per-query statistics (derived from the timer CSV files) and, when results are written into subfolders, the complete result sets used to compare results across connections. The configuration files are documented ([Query File](Options.html#query-file), [Connection File](Options.html#connection-file)), but they are written as Python literals, which tools outside Python cannot read directly. |
| I2. Software includes qualified references to other objects. | met | `.zenodo.json` and `codemeta.json` refer to the describing publications (JOSS paper and two conference papers, by DOI), to the documentation, and to [Bexhoma](https://github.com/Beuth-Erdelt/Benchmark-Experiment-Host-Manager), which requires DBMS-Benchmarker. |

## Reusable

| Principle | Status | Evidence |
|---|---|---|
| R1. Software is described with a plurality of accurate and relevant attributes. | met | README, [documentation](https://dbmsbenchmarker.readthedocs.io) of concepts, options, evaluations and the dashboard, TPC-H and TPC-DS example workloads in several SQL dialects. |
| R1.1. Software is given a clear and accessible license. | met | AGPL-3.0-or-later in `LICENSE`, stated as SPDX identifier in `pyproject.toml`, `codemeta.json`, `.zenodo.json` and `CITATION.cff`. Material from others and its licences are listed in [`THIRD_PARTY.md`](https://github.com/Beuth-Erdelt/DBMS-Benchmarker/blob/master/THIRD_PARTY.md). |
| R1.2. Software is associated with detailed provenance. | partly met | Software: git history, changes recorded per pull request in the [release notes](https://github.com/Beuth-Erdelt/DBMS-Benchmarker/releases), authors and contributors in [`AUTHORS.md`](https://github.com/Beuth-Erdelt/DBMS-Benchmarker/blob/master/AUTHORS.md) with a written authorship rule in [CONTRIBUTING](CONTRIBUTING.html#authorship). Results: the result folder contains the query and connection configuration, the drawn query parameters and query order (`protocol.json`), errors and warnings, and the DBMS and JDBC driver versions; a random seed makes parameters repeatable. However, the version of DBMS-Benchmarker that produced a result is not recorded in the result folder. |
| R2. Software includes qualified references to other software. | met | Dependencies with version constraints in `requirements.txt` and `pyproject.toml`; their licences in `THIRD_PARTY.md`. JDBC drivers are supplied by the user and named in `connections.config`. |
| R3. Software meets domain-relevant community standards. | partly met | Packaged with `pyproject.toml` and published on PyPI; semantic version numbers with a written [versioning policy and release checklist](CONTRIBUTING.html#versions-and-releases); `master` accepts changes only by pull request with passing tests. Tests run in CI on Python 3.10 to 3.13, including end-to-end runs against SQLite of the benchmark, the merge and evaluation scripts and the dashboard. Results derived from TPC workloads are marked as not comparable with official TPC results. However, test coverage is 34 % (CI fails below 33 %), and code style follows PEP 8 only partly. |

## Open points

* **R1.2:** record the version of DBMS-Benchmarker in the result folder, e.g. in `protocol.json`.
* **I1:** store the per-query statistics and the complete result sets for comparison in a portable format (CSV, Parquet) instead of pickle; document the format of the result folder.
* **I1:** accept `queries.config` and `connections.config` also as JSON, keeping the current format readable.
* **I1:** if other tools are to write these files, publish a JSON Schema of both and check the examples against it in the tests.
* **R3:** raise test coverage, in particular of the inspector, the evaluator and the dashboard, and raise the floor with it.
* **F1.1:** if the result format is used by other tools, release and identify it separately.
