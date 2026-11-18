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

If you are planning to make a pull request, start by creating a new branch with a short but descriptive name (rather than using your master branch).

Every change to `master` goes through a pull request, which is reviewed and must pass the automated tests.
Add an entry to [CHANGELOG.md](https://github.com/Beuth-Erdelt/DBMS-Benchmarker/blob/master/CHANGELOG.md) under *Unreleased* for every change users can notice.


### Coding Conventions

DBMSBenchmarker tries to follow the coding conventions laid out in PEP8 and PEP257

- http://www.python.org/dev/peps/pep-0008/
- http://www.python.org/dev/peps/pep-0257/


### Testing

The tests in `tests/` run on every pull request with Python 3.10 to 3.13.
Run them locally from the repository root with `python -m unittest discover tests`.

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
While the major version is 0, a minor release may contain incompatible changes; they are named in the changelog.

* **Minor release** (`0.16.0`): changes what an experiment executes or records. This includes how queries are run and timed, connection handling, the format of `queries.config` and `connections.config`, and the format of the result folder.
* **Patch release** (`0.15.10`): everything else, including bug fixes in evaluations, the dashboard and reports. If a fix changes evaluated numbers, the changelog entry names the affected metrics.

### Release checklist

1. Set the version in `dbmsbenchmarker/__version__.py`, `CITATION.cff` and `codemeta.json`.
2. Move the *Unreleased* entries of `CHANGELOG.md` to the new version with the release date.
3. For a minor release only: the reference experiments of [Bexhoma](https://github.com/Beuth-Erdelt/Benchmark-Experiment-Host-Manager) have been run with the release candidate and passed all validity checks.
4. Merge into `master` and create a GitHub release with the tag `vX.Y.Z`.
   Publishing runs the tests again, then uploads the package to PyPI; Zenodo archives the release and assigns a DOI.


## Authorship

DBMS-Benchmarker distinguishes authors from contributors, see [AUTHORS.md](https://github.com/Beuth-Erdelt/DBMS-Benchmarker/blob/master/AUTHORS.md).

* **Authors** made a scientific contribution: the concept or method, the design of benchmarks, measurements or evaluations. They are named in the citation metadata and in the Zenodo record.
* **Contributors** made other contributions, such as implementation, maintenance, bug fixes, documentation or tests. They are acknowledged in `AUTHORS.md`.

For work done within a funded project, authorship and rights are agreed in writing at the start of the project.
