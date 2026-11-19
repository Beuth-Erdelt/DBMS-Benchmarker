# Contributing to DBMSBenchmarker

You would like to contribute? Great!

Some things that you can help on include:
* **New Workloads**: The `example/` folder includes the TPC-H and TPC-DS (reading) queries in various dialects. We are interested in adding other relevant workloads.
* **Evaluation Tools**: The dashboard contains some important prepared charts. However, adding functionality and keeping up to date with Dash could use some help.
* **Documentation**: If a point in the documentation is unclear, we look forward to receiving tips and suggestions for improvement.
* **Testing**: If the behavior is not as expected and you suspect a bug, please report it to our [issue tracker](https://github.com/Beuth-Erdelt/DBMS-Benchmarker/issues).
* **Use Cases**: If you have had any experiences with peculiarities, mistakes, ambiguities or oddities or particularly beautiful cases etc., we are interested in hearing about them and passing them on to others.

## Non-code contributions

Even if you don't feel ready or able to contribute code, you can still help out. There always things that can be improved on the documentation (even just proof reading, or telling us if a section isn't clear enough).


## Code contributions

We welcome pull requests to fix bugs or add new features.

### Licensing

In your git commit and/or pull request, please explicitly state that you agree to your contributions being licensed under the GNU Affero General Public License v3.0 or later (AGPL-3.0-or-later).

If your contribution includes material from others (code, icons, queries, data), add it to [THIRD_PARTY.md](https://github.com/Beuth-Erdelt/DBMS-Benchmarker/blob/master/THIRD_PARTY.md) with its source and licence.


### Git Usage

Development happens on `dev`; `master` holds the releases.
Start each change on a new branch from `dev` with a short but descriptive name, and open a pull request into `dev`.
For a release, `dev` is merged into `master` by a pull request.
Every pull request must pass the automated tests.

Changes are recorded in the notes of the [GitHub releases](https://github.com/Beuth-Erdelt/DBMS-Benchmarker/releases), which are generated from the pull requests.
So give each pull request a title that says what changes for users.
If a change alters measured or evaluated numbers, the pull request names the affected metrics.
This includes any change after which the same experiment or the same raw results give different reported values: units (e.g. ms to s), aggregation (e.g. mean to median, warm-up runs excluded), fixed formulas, rounding, what a timer measures.
Renamed metrics or columns are named as well, since they break comparisons with older results.


### Coding Conventions

DBMSBenchmarker tries to follow the coding conventions laid out in PEP8 and PEP257

- http://www.python.org/dev/peps/pep-0008/
- http://www.python.org/dev/peps/pep-0257/


### Testing

The tests in `tests/` run on every pull request with Python 3.10 to 3.13.
Run them locally from the repository root with `python -m unittest discover tests`.
The end-to-end tests `tests/test_benchmark_sqlite.py` (a benchmark and the inspector) and `tests/test_pipeline_sqlite.py` (the scripts Bexhoma runs: one benchmark per connection in subfolders, `metrics.evaluation.py`, `merge.py`, `benchmark.py read`, `evaluate.py`, the dashboard) need a Java runtime and the [SQLite JDBC driver](https://repo1.maven.org/maven2/org/xerial/sqlite-jdbc/3.47.1.0/sqlite-jdbc-3.47.1.0.jar) in `tests/jars/` (or its path in `DBMSBENCHMARKER_SQLITE_JAR`); without them it is skipped locally, and it always runs in CI.

CI also measures test coverage of the package and the scripts, including the subprocesses the end-to-end tests start.
A pull request fails if coverage drops below the floor `fail_under` in `pyproject.toml`; raise the floor when tests are added.
Locally: `python -m coverage run -m unittest discover tests`, then `python -m coverage combine` and `python -m coverage report`.

New features or functions will not be accepted without testing.
Likewise for any enhancement or bug fix, we require including an additional test.

#### If the feature or functionality concerns benchmarking

We expect it to be testable with the TPC-H workload.
Otherwise please add references to data and queries you used to test the functionality.
They must be publicly accessible.

#### If the feature or functionality concerns evaluation

Please include a zipped result folder, so we can trace the enhancement of evaluations based on the same results you have used.
(And please be sure to not include secret passwords in the connection information).



## Versions and releases

Versions have the form `MAJOR.MINOR.PATCH` and git tags the form `vMAJOR.MINOR.PATCH`.
While the major version is 0, a minor release may contain incompatible changes; they are named in their pull requests.

* **Minor release** (`0.16.0`): changes what an experiment executes or records. This includes how queries are run and timed, connection handling, the format of `queries.config` and `connections.config`, and the format of the result folder.
* **Patch release** (`0.15.10`): everything else, including bug fixes in evaluations, the dashboard and reports. If a fix changes evaluated numbers, its pull request names the affected metrics.

### Release checklist

1. Set the version with `python bump_version.py X.Y.Z`; it writes `dbmsbenchmarker/__version__.py`, `CITATION.cff` and `codemeta.json`, and the tests check that they agree.
2. Merge into `master` and create a GitHub release with the tag `vX.Y.Z`.
   Use *Generate release notes*.
   Publishing runs the tests again, then uploads the package to PyPI; Zenodo archives the release with its notes and assigns a DOI.

The tests, including the end-to-end runs against SQLite, are the release gate for both kinds of release.
[Bexhoma](https://github.com/Beuth-Erdelt/Benchmark-Experiment-Host-Manager) uses DBMS-Benchmarker in its own experiments on Kubernetes and so tests it against many DBMS; a problem found there is reported as an issue here.


## Authorship

DBMS-Benchmarker distinguishes authors from contributors, see [AUTHORS.md](https://github.com/Beuth-Erdelt/DBMS-Benchmarker/blob/master/AUTHORS.md).

* **Authors** made a scientific contribution: the concept or method, the design of benchmarks, measurements or evaluations. They are named in the citation metadata and in the Zenodo record.
* **Contributors** made other contributions, such as implementation, maintenance, bug fixes, documentation or tests. They are acknowledged in `AUTHORS.md`.

For work done within a funded project, authorship and rights are agreed in writing at the start of the project.
