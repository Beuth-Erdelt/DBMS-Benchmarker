# Contributing to DBMSBenchmarker

You would like to contribute? Great!

Some things that you can help on include:
* **New Workloads**: The `example/` folder includes the TPC-H and TPC-DS (reading) queries in various dialects. We are interested in adding other relevant workloads.
* **Evaluation Tools**: The dashboard contains some important prepared charts. However, adding functionality and keeping up to date with Dash could use some help.
* **Documentation**: If a point in the documentation is unclear, we look forward to receiving tips and suggestions for improvement.
* **Testing**: If the behavior is not as expected and you suspect a bug, please report it to our [issue tracker](https://github.com/Beuth-Erdelt/DBMS-Benchmarker/issues).
* **Use Cases**: If you have had any experiences with peculiarities, mistakes, ambiguities or oddities or particularly beautiful cases etc., we are interested in hearing about them and passing them on to others.

## Non-code contributions

Even if you don’t feel ready or able to contribute code, you can still help out. There always things that can be improved on the documentation (even just proof reading, or telling us if a section isn’t clear enough).


## Code contributions

We welcome pull requests to fix bugs or add new features.

### Licensing

In your git commit and/or pull request, please explicitly state that you agree to your contributions being licensed under "GNU Affero General Public License v3".


### Git Usage

If you are planning to make a pull request, start by creating a new branch with a short but descriptive name (rather than using your master branch).


### Coding Conventions

DBMSBenchmarker tries to follow the coding conventions laid out in PEP8 and PEP257

- http://www.python.org/dev/peps/pep-0008/
- http://www.python.org/dev/peps/pep-0257/


### Testing

New features or functions will not be accepted without testing.
Likewise for any enhancement or bug fix, we require including an additional test.

**If the feature or functionality concerns benchmarking**:
We expect it to be testable with the TPC-H workload.
Otherwise please add references to data and queries you used to test the functionality.
They must be publicly accessible.

**If the feature or functionality concerns evaluation**:
Please include a zipped result folder, so we can trace the enhancement of evaluations based on the same results you have used.

