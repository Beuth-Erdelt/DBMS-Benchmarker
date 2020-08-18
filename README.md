# DBMS-Benchmarker

DBMS-Benchmarker is a Python-based application-level blackbox benchmark tool for Database Management Systems (DBMS).
It aims at reproducible measuring and easy evaluation of the performance the user receives even in complex benchmark situations.
It connects to a given list of DBMS (via JDBC) and runs a given list of (SQL) benchmark queries.
Queries can be parametrized and randomized.
Results and evaluations are available via a Python interface.
Optionally some reports are generated.
An interactive dashboard assists in multi-dimensional analysis of the results.

## Overview

This documentation contains
* an example of how to perform a [TPC-H-like Benchmark](docs/Example-TPC-H.md) from a command line
* a list of the [key features](#key-features)
* an example of the [basic usage](#basic-usage) in Python
* an illustration of the [concepts](docs/Concept.md)
* an illustration of the [evaluations](docs/Evaluations.md)
* a description of the [options and configurations](docs/Options.md)
* more extensive [examples](docs/Usage.md) of using the [cli tool](docs/Options.md#command-line-options-and-configuration)
* some [use-cases](docs/Usecases.md#use-cases) and [test scenarios](docs/Usecases.md#scenarios)
* examples of how to use the interactive [inspector](docs/Inspection.md)
* examples of how to use the interactive [dashboard](docs/Dashboard.md)

## Key Features

DBMS-Benchmarker
* is Python3-based
* connects to all [DBMS](docs/Options.md#connection-file) having a JDBC interface - including GPU-enhanced DBMS
* requires *only* JDBC - no vendor specific supplements are used
* benchmarks arbitrary SQL queries - in all dialects
* allows [planning](docs/Options.md#query-file) of complex test scenarios - to simulate realistic or revealing use cases
* allows easy repetition of benchmarks in varying settings - different hardware, DBMS, DBMS configurations, DB settings etc
* investigates a number of timing aspects - connection, execution, data transfer, in total, per session etc
* investigates a number of other aspects - received result sets, precision, number of clients
* collects hardware metrics from a Grafana server - hardware utilization, energy consumption etc
* helps to [evaluate](docs/Evaluations.md) results - by providing  
  * standard Python data structures
  * predefined evaluations like statistics, plots, Latex reporting
  * an [inspection tool](docs/Inspection.md)
  * an [interactive dashboard](docs/Dashboard.md)

In the end this tool provides metrics that can be analyzed by [aggregation](docs/Concept.md#aggregation-functions) in [multi-dimensions](docs/Concept.md#evaluation), like maximum throughput per DBMS, average CPU utilization per query or geometric mean of run latency per workload.

For more informations, see a [basic example](#basic-usage), take a look at help for a full list of [options](docs/Options.md#command-line-options-and-configuration) or take a look at a [demo report](docs/Report-example-tpch.pdf).

The code uses several Python modules, in particular <a href="https://github.com/baztian/jaydebeapi" target="_blank">jaydebeapi</a> for handling DBMS.
This module has been tested with Brytlyt, Exasol, Kinetica, MariaDB, MemSQL, Mariadb, MonetDB, OmniSci and PostgreSQL.


## Basic Usage

The following very simple use case runs the query `SELECT COUNT(*) FROM test` 10 times against one local MySQL installation.
As a result we obtain the execution times as a csv file, as a series plot and as a bloxplot.

We need to provide
* a [DBMS configuration file](docs/Options.md#connection-file), e.g. in `./config/connections.config`  
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
* a [Queries configuration file](docs/Options.md#query-file), e.g. in `./config/queries.config`  
```
{
  'name': 'Some simple queries',
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

In Python we basically use the benchmarker as follows:
```
from dbmsbenchmarker import *

# tell the benchmarker where to find the config files
configfolder = "./config"
# tell the benchmarker where to put results
resultfolder = "/results"

# get a benchmarker object
dbms = benchmarker.benchmarker(result_path=resultfolder)
dbms.getConfig(configfolder)

# tell the benchmarker which fixed evaluations we want to have (line plot and box plot per query)
dbms.reporter.append(benchmarker.reporter.ploter(dbms))
dbms.reporter.append(benchmarker.reporter.boxploter(dbms))

# start benchmarking
dbms.runBenchmarks()

# print collected errors
dbms.printErrors()

# get unique code of this experiment
code = dbms.code

# generate inspection object
evaluate = inspector.inspector(resultfolder)

# load this experiment into inspector
evaluate.load_experiment(code)

# get latency of run (measures and statistics) of first query
df_measure, df_statistics = evaluate.get_measures_and_statistics(1, type='latency', name='run')
```
There also is a [command line interface](docs/Options.md#command-line-options-and-configuration) for running benchmarks and generation of reports.


## Limitations

Limitations are:
* strict black box perspective - may not use all tricks available for a DBMS
* strict JDBC perspective - depends on a JVM and provided drivers
* strict user perspective - client system, network connection and other host workloads may affect performance
* not officially applicable for well known benchmark standards - partially, but not fully complying with TPC-H and TPC-DS
* hardware metrics are collected from a monitoring system - not as precise as profiling
* no GUI for configuration
* strictly Python - a very good and widely used language, but maybe not your choice

Other comparable products you might like
* [Apache JMeter](https://jmeter.apache.org/index.html) - Java-based performance measure tool, including a configuration GUI and reporting to HTML
* [HammerDB](https://www.hammerdb.com/) - industry accepted benchmark tool, but limited to some DBMS
* [Sysbench](https://github.com/akopytov/sysbench) - a scriptable multi-threaded benchmark tool based on LuaJIT
* [OLTPBench](https://github.com/oltpbenchmark/oltpbench) -Java-based performance measure tool, using JDBC and including a lot of predefined benchmarks 






