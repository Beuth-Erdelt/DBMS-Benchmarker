# Example: TPC-H

This example shows how to benchmark 22 reading queries Q1-Q22 derived from TPC-H in MySQL

> The query file is derived from the TPC-H and as such is not comparable to published TPC-H results, as the query file results do not comply with the TPC-H Specification.

Official TPC-H benchmark - http://www.tpc.org/tpch

**Content**:
* [Prerequisites](#prerequisites)
* [Perform Benchmark](#perform-benchmark)
* [Evaluate Results in Dashboard](#evaluate-results-in-dashboard)
* [Where to go](#where-to-go)

## Prerequisites

We need
* a local instance of MySQL
  * having a database `database` containing the TPC-H data of SF=1
  * access rights for user / password: `username/password`
* a suitable MySQL JDBC driver jar file
* JDK 8 installed

If necessary, adjust the settings in the file `example/connections.py`:

```
[
  {
    'name': "MySQL",
    'alias': "Some DBMS",
    'version': "CE 8.0.13",
    'docker': 'MySQL',
    'docker_alias': "DBMS A",
    'dialect': "MySQL",
    'hostsystem': {'node': 'localhost'},
    'info': "This is an example: MySQL on localhost",
    'active': True,
    'JDBC': {
      'driver': "com.mysql.cj.jdbc.Driver",
      'url': "jdbc:mysql://localhost:3306/database",
      'auth': ["username", "password"],
      'jar': "mysql-connector-java-8.0.13.jar"
    },
  },
]
```

## Perform Benchmark

Run the command:

`python benchmark.py run -e yes -b -f example/tpch`

* `-e yes`: This will precompile some evaluations and generate the timer cube.
* `-b`: This will suppress some output
* `-f`: This points to a folder having the configuration files.

For more options, see the [documentation](Options.md#command-line-options-and-configuration)

After benchmarking has been finished will see a message like
```
Experiment <code> has been finished
```

The script has created a result folder in the current directory containing the results. `<code>` is the name of the folder.


### Evaluate Results in Dashboard

Run the command:

`python dashboard.py`

This will start the evaluation dashboard at `localhost:8050`.
Visit the address in a browser and select the experiment `<code>`.

## Where to go

* Different DBMS
* Add metadata
* Use Bexhoma
* SF
