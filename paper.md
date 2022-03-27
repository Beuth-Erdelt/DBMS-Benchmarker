---
title: 'DBMS-Benchmarker: Benchmark and Evaluate DBMS in Python'
tags:
  - Python
  - DBMS
  - JDBC
authors:
  - name: Patrick K. Erdelt
    orcid: 0000-0002-3359-2386
    affiliation: 1
affiliations:
 - name: Berliner Hochschule für Technik (BHT)
   index: 1
date: 21 March 2022
bibliography: paper.bib

---

# DBMS-Benchmarker

DBMS-Benchmarker is a Python-based application-level blackbox benchmark tool for Database Management Systems (DBMS).
It aims at reproducible measuring and easy evaluation of the performance the user receives even in complex benchmark situations.
It connects to a given list of DBMS (via JDBC) and runs a given list of (SQL) benchmark queries.
Queries can be parametrized and randomized.
Results and evaluations are available via a Python interface and can be inspected with standard Python tools like pandas DataFrames.
An interactive visual dashboard assists in multi-dimensional analysis of the results.

See the [homepage](https://github.com/Beuth-Erdelt/DBMS-Benchmarker) and the [documentation](https://dbmsbenchmarker.readthedocs.io/en/latest/Docs.html).

# Statement of Need

There are a variety of (relational) database management systems (DBMS) and a lot of products.
The types thereof can be divided into for example row-wise, column-wise, in-memory, distributed and GPU-enhanced. 
All of these products have unique characteristics, special use cases, advantages and disadvantages and their justification.
In order to be able to verify and ensure the performance measurement, we want to be able to create and repeat scenarios.
Repetition is crucial @Raasveldt2018FBC32099503209955, in particular in the age of Cloud-based systems with it's diversity of hardware configurations.

We are looking for a tool to support the repetition and reproducibility of benchmarking situations, and that is capable of connecting to all these systems.
We are also looking for a tool that will help with the statistical and interactive analysis of the results.
We want to use Python as the common Data Science language.

To our knowledge there is no other such tool, c.f. @10.1007/978-3-319-67162-8_12, @10.1007/978-3-030-12079-5_4.
There are other tools like Apache JMeter, HammerDB, Sysbench, OLTPBench, that provide nice features, but none fitting these needs.
The design decisions of this tool have been elaborated in more detail in @10.1007/978-3-030-84924-5_6.
DBMS-Benchmarker has been used as a support for recieving scientific results about benchmarking DBMS performance in Cloud environments as in @10.1007/978-3-030-84924-5_6 and @10.1007/978-3-030-94437-7_6.
This module has been tested with Clickhouse, DB2, Exasol, Citus Data (Hyperscale), Kinetica, MariaDB, MariaDB Columnstore, MemSQL (SingleStore), MariaDB, MariaDB Columnstore, MonetDB, MySQL, OmniSci (HEAVY.AI) , Oracle DB, PostgreSQL, SQL Server, SAP HANA and Vertica.

## Summary of Solution

DBMS-Benchmarker is Python3-based and helps to **benchmark DBMS**. It

* connects to all DBMS having a JDBC interface - including GPU-enhanced DBMS
* requires *only* JDBC - no vendor specific supplements are used
* benchmarks arbitrary SQL queries - in all dialects
* allows planning of complex test scenarios - to simulate realistic or revealing use cases
* allows easy repetition of benchmarks in varying settings - different hardware, DBMS, DBMS configurations, DB settings etc
* allows randomized queries (optionally with specified seeds for reproducible results) to avoid caching side effects
* investigates a number of timing aspects - connection, execution, data transfer, in total, per session etc
* investigates a number of other aspects - received result sets, precision, number of clients
* collects hardware metrics from a Prometheus server - hardware utilization, energy consumption etc
* compares result sets: *Do I always receive the same data?*

DBMS-Benchmarker helps to **evaluate results** - by providing    

* metrics that can be analyzed by aggregation in multi-dimensions, like maximum throughput per DBMS, average CPU utilization per query or geometric mean of run latency per workload
* predefined evaluations like statistics
* in standard Python data structures
* in Jupyter notebooks - see [rendered example](https://beuth-erdelt.github.io/DBMS-Benchmarker/Evaluation-Demo.html)
* in an interactive dashboard

Some features are inspired by [TPC-H](http://www.tpc.org/tpch/) and [TPC-DS](http://www.tpc.org/tpcds/) - Decision Support Benchmarks, which are provided in parts as predefined configs.


# Basic Example

The following very simple use case runs the query `SELECT COUNT(*) FROM test` 10 times against one local (existing) MySQL installation.
We assume here we have downloaded the required JDBC driver, e.g. `mysql-connector-java-8.0.13.jar`.

Run `pip install dbmsbenchmarker` for installation.

## Configuration

### DBMS configuration file, e.g. in `./config/connections.config`  

```
[
  {
    'name': "MySQL",
    'active': True,
    'JDBC': {
      'driver': "com.mysql.cj.jdbc.Driver",
      'url': "jdbc:mysql://localhost:3306/database",
      'auth': ["username", "password"],
      'jar': "mysql-connector-java-8.0.13.jar"
    }
  }
]
```

### Queries configuration file, e.g. in `./config/queries.config`  

```
{
  'name': 'Some simple queries',
  'connectionmanagement': {
        'timeout': 5 # in seconds
    },
  'queries':
  [
    {
      'title': "Count all rows in test",
      'query': "SELECT COUNT(*) FROM test",
      'numRun': 10
    }
  ]
}
```


## Perform Benchmark and Evaluate Results

Run the CLI command: `dbmsbenchmarker run -e yes -b -f ./config`

After benchmarking has been finished we will see a message like `Experiment <code> has been finished`.
The script has created a result folder in the current directory containing the results. `<code>` is the name of the folder.

Run the CLI command: `dbmsdashboard`

This will start the evaluation dashboard at `localhost:8050`.
Visit the address in a browser and select the experiment `<code>`.
Alternatively you may use a Jupyter notebooks or standard Python.


# Concepts

## Experiment

An **experiment** is organized in *queries*.
A **query** is a statement, that is understood by a Database Management System (DBMS).

## Single Query

A **benchmark** of a query consists of these steps:

![Caption for example figure.\label{fig:Concept-Query}](docs/Concept-Query.png){ width=960}


1. Establish a **connection** between client and server  
This uses `jaydebeapi.connect()` (and also creates a cursor - time not measured)
1. Send the query from client to server and
1. **Execute** the query on server  
These two steps use `execute()` on a cursor of the JDBC connection
1. **Transfer** the result back to client  
This uses `fetchall()` on a cursor of the JDBC connection
1. Close the connection  
This uses `close()` on the cursor and the connection

The times needed for steps connection (1.), execution (2. and 3.) and transfer (4.) are measured on the client side.
A unit of connect, send, execute and transfer is called a **run**. Connection time will be zero if an existing connection is reused.
A sequence of runs between establishing and discarding a connection is called a **session**.

A basic parameter of a query is the **number of runs** (units of send, execute, transfer).
To configure sessions it is also possible to adjust

* the **number of runs per connection** (session length, to have several sequential connections) and
* the **number of parallel connections** (to simulate several simultanious clients)
* a **timeout** (maximum lifespan of a connection)
* a **delay** for throttling (waiting time before each connection or execution)

for the same query.
Parallel clients are simulated using the `pool.apply_async()` method of a `Pool` object of the module multiprocessing.
Runs and their benchmark times are ordered by numbering.
Moreover we can **randomize** a query, such that each run will look slightly different.
This means we exchange a part of the query for a random value.


## Basic Metrics

We have several **timers** to collect timing information in ms and per run, corresponding to the parts of query processing: **timerConnection**, **timerExecution** and **timerTransfer**.
The tool also computes **timerRun** (the sum of *timerConnection*, *timerExecution* and *timerTransfer*) and **timerSession**:
This timer gives the time in ms and per session.
It aggregates all runs of a session and sums up their *timerRun*s.
A session starts with establishing a connection and ends when the connection is disconnected.

We also measure and store the **total time** of the benchmark of the query, since for parallel execution this differs from the **sum of times** based on *timerRun*. Total time means measurement starts before first benchmark run and stops after the last benchmark run has been finished. Thus total time also includes some overhead (for spawning a pool of subprocesses, compute size of result sets and joining results of subprocesses).
Thus the sum of times is more of an indicator for performance of the server system, the total time is more of an indicator for the performance the client user receives.
We also compute for each query and DBMS **latency** (measured time) and **throughput** (number of parallel clients per mean time).
Additionally error messages and timestamps of begin and end of benchmarking a query are stored.


## Comparison

We can specify a dict of DBMS.
Each query will be sent to every DBMS in the same number of runs.
This also respects randomization, i.e. every DBMS receives exactly the same versions of the query in the same order.
We assume all DBMS will give us the same result sets.
Without randomization, each run should yield the same result set.
This tool automatically can check these assumptions by **comparison**.
The resulting data table is handled as a list of lists and treated by this:
Result sets of different runs (not randomized) and different DBMS can be compared by their sorted table (small data sets) or their hash value or size (bigger data sets).
In order to do so, result sets (or their hash value or size) are stored as lists of lists and additionally can be saved as csv files or pickled pandas dataframes.

## Monitoring Hardware Metrics

To make hardware metrics available, we must provide an API URL for a Prometheus Server.
The tool collects metrics from the Prometheus server with a step size of 1 second.
We may define the metrics in terms of **promql**.
Metrics can be defined per connection.

## Results

As a result we obtain measured times in milliseconds for the query processing parts: connection, execution, data transfer.

![Caption for example figure.\label{fig:Evaluation-Cubes}](docs/Evaluation-Cubes.png){ width=1440}

These are described in three dimensions:
number of run, number of query and configuration.
The configuration dimension can consist of various nominal attributes like DBMS, selected processor, assigned cluster node, number of clients and execution order.
We also can have various hardware metrics like CPU and GPU utilization, CPU throttling, memory caching and working set.
These are also described in three dimensions:
Second of query execution time, number of query and number of configuration.

All these metrics can be sliced or diced, rolled-up or drilled-down into the various dimensions using several aggregation functions for evaluation.

# Evaluation

Results can be evaluated via a Python interface.
Measurements are available as pandas DataFrames.
Moreover the package includes a browser-based visual analysis tool.

## Python - Pandas

There are DataFrames covering errors and warnings, that have occured, and timing and hardware metrics that have been collected or derived.

For example the latency of execution, aggregated in the query dimension by computing the mean value, can be obtained as:

```
df = evaluate.get_aggregated_query_statistics(
    type='latency', name='execution', query_aggregate='Mean')
```

![Caption for example figure.\label{fig:dashboard}](docs/latency-table-example.png){ width=1440}

## GUI - Dashboard

The dashboard helps in interactive evaluation of experiment results.
It show plots of various types, that can be customized and filtered by DBMS configuration and query.

![Caption for example figure.\label{fig:dashboard}](docs/dashboard.png){ width=1440}


# References