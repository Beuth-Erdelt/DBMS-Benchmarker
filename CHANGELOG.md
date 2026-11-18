# Changelog

All notable changes to DBMS-Benchmarker are recorded here.
Versions follow the rules in [docs/CONTRIBUTING.md](docs/CONTRIBUTING.md#versions-and-releases).
Changes that alter measured or evaluated numbers name the affected metrics, so that results produced with an older version can be interpreted.

Versions before 0.15.8 are described on the [GitHub releases page](https://github.com/Beuth-Erdelt/DBMS-Benchmarker/releases).

## [Unreleased] - 0.15.9

### Added
- Citation metadata: `CITATION.cff`, `.zenodo.json` and `codemeta.json`. Releases are archived on Zenodo and receive a DOI.
- `AUTHORS.md`, `THIRD_PARTY.md`, a pull request template and `CODEOWNERS`.
- Tests run on every pull request (Python 3.10 to 3.13).
- End-to-end test: runs a benchmark through JDBC against a small SQLite database and checks the results with the inspector (`tests/test_benchmark_sqlite.py`).

### Removed
- `OLD_setup.py`, `demo-interactive-inspection-legacy.py`, the 2019 example report `docs/Report-example-tpch.pdf` and the generated API pages in `docs/dbmsbenchmarker/`. `dev/` is no longer part of release archives.

### Changed
- Publishing to PyPI requires the tests to pass.
- Package metadata: licence as SPDX expression `AGPL-3.0-or-later`, `requires-python >=3.10` (the pinned urllib3 needs it), documentation and issue URLs.

## [0.15.8] - 2026-11-10

### Changed
- `requests>=2.34.2` (CVE-2024-47081), as a lower bound so that Bexhoma can pin its own version.

[Unreleased]: https://github.com/Beuth-Erdelt/DBMS-Benchmarker/compare/0.15.8...dev
[0.15.8]: https://github.com/Beuth-Erdelt/DBMS-Benchmarker/releases/tag/0.15.8
