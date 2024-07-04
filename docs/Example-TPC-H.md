# Example: TPC-H

This example shows how to benchmark 22 reading queries Q1-Q22 derived from TPC-H in PostgreSQL

> The query file is derived from the TPC-H and as such is not comparable to published TPC-H results, as the query file results do not comply with the TPC-H Specification.

Official TPC-H benchmark - http://www.tpc.org/tpch

**Content**:
* [Prerequisites](#prerequisites)
* [Perform Benchmark](#perform-benchmark)
* [Evaluate Results](#evaluate-results-in-dashboard)
* [Where to go](#where-to-go)

## Prerequisites

We need
* a local instance of PostgreSQL
  * having a database `database` containing the TPC-H data of SF=1
  * access rights for user / password: `username/password`
* a suitable PostgreSQL JDBC driver jar file
* JDK 8 installed

If necessary, adjust the settings in the file `example/tpc-h/connections.py`:

```bash
[
    {
        'name': 'PostgreSQL',
        'info': 'This is a demo of PostgreSQL',
        'active': True,
        'dialect': 'PostgreSQL',
        'hostsystem': {'node': 'localhost'},
        'JDBC': {
            'driver': 'org.postgresql.Driver',
            'url': 'jdbc:postgresql://localhost:5432/tpch',
            'auth': ['postgres', 'postgres'],
            'jar': 'jars/postgresql-42.2.5.jar'
        }
    },
]
```

## Perform Benchmark

Run the command:

`dbmsbenchmarker run -e yes -b -f example/tpch`

* `-e yes`: This will precompile some evaluations and generate the timer cube.
* `-b`: This will suppress some output
* `-f`: This points to a folder having the configuration files.

For more options, see the [documentation](Options.html#command-line-options-and-configuration)

After benchmarking has been finished will see a message like
```
Experiment <code> has been finished
```

The script has created a result folder in the current directory containing the results. `code` is the name of the folder.

## Evaluate Results

If the `-e yes` option is used, you will see something like

```bash
First successful query: 1
Limited to: ['PostgreSQL']
Number of runs per query: 1
Number of successful queries: 22
Number of max. parallel clients: 1

### Errors (failed queries)
     PostgreSQL
Q1        False
Q2        False
Q3        False
Q4        False
Q5        False
Q6        False
Q7        False
Q8        False
Q9        False
Q10       False
Q11       False
Q12       False
Q13       False
Q14       False
Q15       False
Q16       False
Q17       False
Q18       False
Q19       False
Q20       False
Q21       False
Q22       False

### Warnings (result mismatch)
     PostgreSQL
Q1        False
Q2        False
Q3        False
Q4        False
Q5        False
Q6        False
Q7        False
Q8        False
Q9        False
Q10       False
Q11       False
Q12       False
Q13       False
Q14       False
Q15       False
Q16       False
Q17       False
Q18       False
Q19       False
Q20       False
Q21       False
Q22       False
### Geometric Mean of Medians of Run Times (only successful) [s]
            average run time [s]
DBMS
PostgreSQL                  0.19
### Sum of Maximum Run Times per Query (only successful) [s]
            sum of max run times [s]
DBMS
PostgreSQL                       6.3
### Queries per Hour (only successful) [QpH] - 1*22*3600/(sum of max run times)
            queries per hour [Qph]
DBMS
PostgreSQL                12566.32
### Queries per Hour (only successful) [QpH] - Sum per DBMS
            queries per hour [Qph]
DBMS
PostgreSQL                12566.32
### Queries per Hour (only successful) [QpH] - (max end - min start)
            queries per hour [Qph]        formula
DBMS
PostgreSQL                  9900.0  1*1*22*3600/8
```

### Evaluate Results in Dashboard

Run the command:

`dbmsdashboard`

This will start the evaluation dashboard at `localhost:8050`.
Visit the address in a browser and select the experiment `code`.


## Where to go

### Set a Folder for Collecting Results

`dbmsbenchmarker run -e yes -b -f example/tpc-h -r ~/benchmarks`

All results will be stored in subfolders of `~/benchmarks`.

### Show Results Again

`dbmsbenchmarker read -e yes -r ~/benchmarks/12345`

This shows the evaluation of experiment 12345 in folder `~/benchmarks` again.

### Limit to Single DBMS

`dbmsbenchmarker run -e yes -b -f example/tpc-h -c PostgreSQL`

Handy, if the connection file contains several DBMS.

### Repeat for Statistical Confidence

`dbmsbenchmarker run -e yes -b -f example/tpc-h -pn 2`

This causes each query to be executed twice, one after the other.

### Simulate Parallel Execution

`dbmsbenchmarker run -e yes -b -f example/tpc-h -pn 2 -p 2`

This causes each query to be executed twice in two separate processes, one execution per process, one query at a time.

### Simulate Parallel Streams

`dbmsbenchmarker run -e yes -b -f example/tpc-h -pn 1 -p 2 -pp -s 1234`

This will start two separate, independent processes, each running TPC-H.
Random seed is set (to 1234) to make sure, all processes receive the same randomization.

### Compare Result Sets

`dbmsbenchmarker run -e yes -b -f example/tpc-h -pn 2 -p 2 -sd csv`

This stores the result sets of the 22 queries into CSV files.
If there is more than 1 execution, result sets are compared.


### Moreover

* Use different [DBMS](DBMS.html)
* Add metadata: `-wli 'This is my first test'`
* Shuffle parameters: `-sid 2 -ssh True`: Sets the id of the stream to 2 and shuffles the ordering of query execution according to TPC-H
* Use Bexhoma for management inside a kubernetes cluster: https://github.com/Beuth-Erdelt/Benchmark-Experiment-Host-Manager
