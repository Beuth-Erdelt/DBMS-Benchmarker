# Example: TPC-H

This example shows how to benchmark 22 reading queries Q1-Q22 derived from TPC-H.

> The query file is derived from the TPC-H and as such is not comparable to published TPC-H results, as the query file results do not comply with the TPC-H Specification.

Official TPC-H benchmark - http://www.tpc.org/tpch

## Prerequisits

We need
* a local instance of MySQL
  * having a database `database` containing the TPC-H data for SF=1
  * access rights for user / password `username/password`
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

`python benchmark.py run -qf example/query-tpch.py -cf example/connections.py`

After benchmarking has been finished will see a message like
```
Experiment <code> has been finished
```

The script has created a result folder in the current directory containing the results. `<code>` is the name of the folder.

### Prepare Evaluation

Run the command:

`python benchmark.py read -e yes -r <code>`

This will precompile some evaluations and generate the timer cube.

### Evaluate Results

Run the command:

`python dashboard.py`

This will start the evaluation dashboard at `localhost:8050`.
Visit the address in a browser and select the experiment `<code>`.

## Improvements

* Different DBMS
* Add metadata
* Use Bexhoma
* SF
