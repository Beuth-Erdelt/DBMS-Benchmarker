# Options

## Featured Parameters

The lists of [DBMS](#connection-file) and [queries](#query-file) are given in config files in dict format.

Benchmarks can be [parametrized](#query-file) by
* number of benchmark runs: *Is performance stable across time?*
* number of benchmark runs per connection: *How does reusing a connection affect performance?*
* number of warmup and cooldown runs, if any: *How does (re)establishing a connection affect performance?*
* number of parallel clients: *How do multiple user scenarios affect performance?*
* optional list of timers (currently: connection, execution, data transfer, run and session): *Where does my time go?*
* [sequences](#query-list) of queries: *How does sequencing influence performance?*
* optional [comparison](#results-and-comparison) of result sets: *Do I always receive the same results sets?*

Benchmarks can be [randomized](#randomized-query-file) (optionally with specified [seeds](#random-seed) for reproducible results) to avoid caching side effects and to increase variety of queries by taking samples of arbitrary size from a
* list of elements
* dict of elements (one-to-many relations)
* range of integers
* range of floats
* range of days
* range of (first of) months
* range of years

This is inspired by [TPC-H](http://www.tpc.org/tpch/) and [TPC-DS](http://www.tpc.org/tpcds/) - Decision Support Benchmarks.

## Command Line Options and Configuration

How to configure the benchmarker can be illustrated best by looking at the source code of the [command line tool](../benchmark.py), which will be described in the following.

`python3 benchmark.py -h`

```
usage: benchmark.py [-h] [-d] [-b] [-qf QUERY_FILE] [-cf CONNECTION_FILE]
                    [-q QUERY] [-c CONNECTION] [-l LATEX_TEMPLATE]
                    [-f CONFIG_FOLDER] [-r RESULT_FOLDER] [-g {no,yes}]
                    [-e {no,yes}] [-w {query,connection}] [-a]
                    [-u [UNANONYMIZE [UNANONYMIZE ...]]] [-p NUMPROCESSES]
                    [-s SEED] [-vq] [-vs] [-pn NUM_RUN]
                    {run,read,continue}

A benchmark tool for RDBMS. It connects to a given list of RDBMS via JDBC and
runs a given list benchmark queries. Optionally some reports are generated.

positional arguments:
  {run,read,continue}   run benchmarks and save results, or just read
                        benchmark results from folder, or continue with
                        missing benchmarks only

optional arguments:
  -h, --help            show this help message and exit
  -d, --debug           dump debug informations
  -b, --batch           batch mode (more protocol-like output), automatically
                        on for debug mode
  -qf QUERY_FILE, --query-file QUERY_FILE
                        name of query config file
  -cf CONNECTION_FILE, --connection-file CONNECTION_FILE
                        name of connection config file
  -q QUERY, --query QUERY
                        number of query to benchmark
  -c CONNECTION, --connection CONNECTION
                        name of connection to benchmark
  -l LATEX_TEMPLATE, --latex-template LATEX_TEMPLATE
                        name of latex template for reporting
  -f CONFIG_FOLDER, --config-folder CONFIG_FOLDER
                        folder containing query and connection config files.
                        If set, the names connections.config and
                        queries.config are assumed automatically.
  -r RESULT_FOLDER, --result-folder RESULT_FOLDER
                        folder for storing benchmark result files, default is
                        given by timestamp
  -g {no,yes}, --generate-output {no,yes}
                        generate new report files
  -e {no,yes}, --generate-evaluation {no,yes}
                        generate new evaluation file
  -w {query,connection}, --working {query,connection}
                        working per query or connection
  -a, --anonymize       anonymize all dbms
  -u [UNANONYMIZE [UNANONYMIZE ...]], --unanonymize [UNANONYMIZE [UNANONYMIZE ...]]
                        unanonymize some dbms, only sensible in combination
                        with anonymize
  -p NUMPROCESSES, --numProcesses NUMPROCESSES
                        Number of parallel client processes. Global setting,
                        can be overwritten by connection. If None given, half
                        of all available processes is taken
  -s SEED, --seed SEED  random seed
  -vq, --verbose-queries
                        print every query that is sent
  -vs, --verbose-statistics
                        print statistics about query that have been sent
  -pn NUM_RUN, --num-run NUM_RUN
                        Parameter: Number of executions per query
```

### Result folder

This optional argument is the name of a folder.

If this folder contains results, results saved inside can be read or benchmarks saved there can be continued.  
Example: `-r /tmp/dbmsresults/1234/` contains benchmarks of code `1234`.

If this folder does not contain results, a new subfolder is generated.
It's name is set automatically to some number derived from current timestamp.
Results and reports are stored there.
Input files for connections and queries are copied to this folder.
Example: `-r /tmp/dbmsresults/`, and a subfolder, say `1234`, will be generated containing results.

### Config folder

Name of folder containing query and connection config files.
If set, the names `connections.config` and `queries.config` are assumed automatically.

### Connection File

Contains infos about JDBC connections.

Example for `CONNECTION_FILE`:
```
[
  {
    'name': "MySQL",
    'version': "CE 8.0.13",
    'info': "This uses engine innodb",
    'active': True,
    'alias': "DBMS A",
    'docker': "MySQL",
    'docker_alias': "DBMS A",
    'dialect': "MySQL",
    'timeload': 100,
    'priceperhourdollar': 1.0,
    `monitoring`: {
      'grafanatoken': 'Bearer 46363756756756476754756745',
      'grafanaurl': 'http://127.0.0.1:3000/api/datasources/proxy/1/api/v1/',
      `grafanaextend`: 5
    },
    'JDBC': {
      'driver': "com.mysql.cj.jdbc.Driver",
      'url': "jdbc:mysql://localhost:3306/database",
      'auth': ["username", "password"],
      'jar': "mysql-connector-java-8.0.13.jar"
    },
    'connectionmanagement': {
      'timeout': 600,
      'numProcesses': 4,
      'runsPerConnection': 5
    },
    'hostsystem': {
      'RAM': 61.0,
      'CPU': 'Intel(R) Xeon(R) CPU E5-2686 v4 @ 2.30GHz\n',
      'Cores': '8\n',
      'host': '4.4.0-1075-aws\n',
      'disk': '82G\n',
      'CUDA': ' NVIDIA-SMI 410.79       Driver Version: 410.79       CUDA Version: 10.0',
      'instance': 'p3.2xlarge'
    }
},
]
```

* `name`: References the connection
* `version` and `info`: Just info texts for implementation in reports
* `active`: Use this connection in benchmarking and reporting (optional, default True)
* `alias`: Alias for anonymized reports (optional, default is a random name)
* `docker`: Name of the docker image. This helps aggregating connections using the same docker image.
* `docker_alias`: Anonymized name of the docker image. This helps aggregating connections using the same docker image in anonymized reports.
* `alias`: Alias for anonymized reports (optional default is a random name)
* `dialect`: Key for (optional) alternative SQL statements in the query file
* `driver`, `url`, `auth`, `jar`: JDBC data
* Additional information useful for reporting and also used for computations
  * `timeload`: Time for ingest (in milliseconds), because not part of the benchmark
  * `priceperhourdollar`: Used to compute total cost based on total time (optional)
  * `grafanatoken`, `grafanaurl`, `grafanaextend`: To fetch hardware metrics from Grafana API. `grafanaextend` extends the fetched interval by `n` seconds at both ends.
* `connectionmanagement`: Parameter for connection management. This overwrites general settings made in the [query config](#extended-query-file) and can be overwritten by query-wise settings made there.
  * `timeout`: Maximum lifespan of a connection. Default is None, i.e. no limit.
  * `numProcesses`: Number of parallel client processes. Default is 1.
  * `runsPerConnection`: Number of runs performed before connection is closed. Default is None, i.e. no limit.
* `hostsystem`: Describing information for report in particular about the host system.
  This can be written automatically by https://github.com/Beuth-Erdelt/Benchmark-Experiment-Host-Manager

### Query File

Contains the queries to benchmark.

Example for `QUERY_FILE`:
```
{
  'name': 'Some simple queries',
  'intro': 'Some describing text about this benchmark test setup',
  'factor': 'mean',
  'queries':
  [
    {
      'title': "Count all rows in test",
      'query': "SELECT COUNT(*) FROM test",
      'numWarmup': 5,
      'numCooldown': 2,
      'delay': 0,
      'numRun': 10,
    },
  ]
}
```

* `name`: Name of the list of queries
* `intro`: Introductional text for reports
* `factor`: Determines the measure for comparing performances (optional). Can be set to `mean` or `median` or `relative`. Default is `mean`.
* `query`: SQL query string
* `title`: Title of the query
* `numRun`: Number of runs of this query for benchmarking
* `numWarmup`: Number of runs of this query for warmup (first `n` queries not counting into statistics), between 0 and `numRun`. This makes sure data is hot and caching is in effect.
* `numCooldown`: Number of runs of this query for cooldown (last `n` queries not counting into statistics), between 0 and `numRun`. This helps sorting out faster executions when the number of parallel clients decreases near the end of a batch.
* `delay`: Number of seconds to wait before each execution statement. This is for throtteling. Default is 0.

Such a query will be executed 10 times, the time of execution will be measured each time, and statistics will be computed for the runs 6,7 and 8.

### Extended Query File

Extended example for `QUERY_FILE`:
```
{
  'name': 'Some simple queries',
  'intro': 'This is an example workload',
  'info': 'It runs on a P100 GPU',
  'reporting':
  {
    'resultsetPerQuery': 10,
    'resultsetPerQueryConnection': 10,
    'queryparameter': 10,
    'rowsPerResultset': 20,
  },
  'connectionmanagement': {
    'timeout': 600,
    'numProcesses': 4,
    'runsPerConnection': 5
  },
  'queries':
  [
    {
      'title': "Count all rows in test",
      'query': "SELECT COUNT(*) c FROM test",
      'DBMS': {
        'MySQL': "SELECT COUNT(*) AS c FROM test"
      }
      'numWarmup': 5,
      'numCooldown': 0,
      'delay': 1,
      'numRun': 10,
      'connectionmanagement': {
        'timeout': 100,
        'numProcesses': 1,
        'runsPerConnection': None
      },
      'timer':
      {
        'connection':
        {
          'active': True,
          'delay': 0
        },
        'datatransfer':
        {
          'active': True,
          'sorted': True,
          'compare': 'result',
          'store': 'dataframe',
          'precision': 4,
        }
      }
    },
  ]
}
```

#### Reporting

Some options are used to configure reporting:
* `intro`: Intro text for report
* `info`: Short info about the current experiment
* `reporting`: Optional settings for latex report
  * `resultsetPerQuery`: Show result sets for each query (and run, in case of randomized)  
  `None`: Don't show
  `False`: No limit
  `n`: Maximum number of runs
  * `resultsetPerQueryConnection`: Show result sets for each query and dbms if differing (and run, in case of randomized)  
  `None`: Don't show
  `False`: No limit
  `n`: Maximum number of runs
  * `queryparameter`: Show set of parameters (in case of randomized query)  
  `None`: Don't show
  `False`: No limit
  `n`: Maximum number of runs
  * `rowsPerResultset`: Show rows per result sets  
  `False`: No limit
  `n`: Maximum number of rows

#### SQL Dialects

The `DBMS` key allows to specify SQL dialects. All connections starting with the key in this dict with use the specified alternative query. In the example above, for instance a connection 'MySQL-InnoDB' will use the alternative.
Optionally at the definition of the connections an attribute `dialect` can be used. For example MemSQL may use the dialect `MySQL`.

#### Connection Management

The first `connectionmanagement` options set global values valid for all DBMS. This can be overwritten by the settings in the [connection config](#connection-file). The second `connectionmanagement` is fixed valid for this particular query and cannot be overwritten.
  * `timeout`: Maximum lifespan of a connection. Default is None, i.e. no limit.
  * `numProcesses`: Number of parallel client processes. Default is 1.
  * `runsPerConnection`: Number of runs performed before connection is closed. Default is None, i.e. no limit.


#### Connection Latency
The `connection` timer will also measure the time for establishing a connection.
It is possible to force sleeping before each establishment by using `delay` (in seconds).
Default is 0.

#### Results and Comparison

The `datatransfer` timer will also measure the time for data transfer.
The tool can store retrieved data to compare different queries and dbms.
This helps to be sure different approaches yield the same results.
For example the query above should always return the same number of rows in table `test`.

`compare` can be used to compare result sets obtained from different runs and dbms.
`compare` is optional and can be 
* `result`: Compare complete result set. Every cell is trimmed. Floats can be rounded to a given `precision` (decimal places). This is important for example for comparing CPU and GPU based DBMS.
* `hash`: Compare hash value of result set.
* `size`: Compare size of result set.

If comparison detects any difference in result sets, a warning is generated.

The result set can optionally be sorted by each column before comparison by using `sorted`.
This helps avoid mismatch due to different orderings in the received sets.

Note that comparing result sets necessarily means they have to be stored, so `result` should only be used for small data sets. The parameter `store` commands the tool to keep the result set and is automatically set to `True` if any of the above is used. It can be set to `False` to command the tool to fetch the result set and immediately forget it. This helps measuring the time for data transfer without having to store all result sets, which in particular for large result sets and numbers of runs can exhauste the RAM.
Setting `store` can also yield the result sets to be stored in extra files. Possible values are: `'store': ['dataframe', 'csv']`



### Randomized Query File

Example for `QUERY_FILE` with randomized parameters:
```
{
  'name': 'Some simple queries',
  'queries':
  [
    {
      'title': "Count rows in test",
      'query': "SELECT COUNT(*) FROM test WHERE name = {NAME}",
      'parameter': {
        'NAME': {
          'type': "list",
          'size': 1,
          'range': ["AUTOMOBILE","BUILDING","FURNITURE","MACHINERY","HOUSEHOLD"]
        }
      },
      'numWarmup': 5,
      'numRun': 10,
    },
  ]
}
```
A `parameter` contain of a name `NAME`, a `range` (list), a `size`(optional, default 1) and a `type`, which can be
* `list`: list of values - random element
* `integer`: 2 integers - random value in between
* `float`: 2 floats - random value in between
* `date`: 2 dates in format 'YYYY-mm-dd' - random date in between
* `firstofmonth`: 2 dates in format 'YYYY-mm-dd' - first of random month in between
* `year`: 2 years as integers - random year in between
* `hexcode`: 2 integers - random value in between as hexcode

For each benchmark run, `{NAME}` is replaced by a (uniformly) randomly chosen value in the range and type given above.
By `size` we can specify the size of the sample (without replacement).
If set, each generated value will receive a `{NAME}` concatenated with the number of the sample.
Python3's `format()` is used for replacement.
The values are generated once per query.
This means if a query is rerun or run for different dbms, the same list of values is used.

Example:
```
'NAME': {
  'type': "integer",
  'range': [1,100]
},
```
in a query with `numWarmup=5` and `numRun=10` will generate a random list of 10 integers between 1 and 100.
Each time the benchmark for this query is done, the same 10 numbers are used.

```
'NAME': {
  'type': "integer",
  'size': 2,
  'range': [1,100]
},
```
in a query with `numWarmup=5` and `numRun=10` will generate a random list of 10 pairs of integers between 1 and 100.
These pairs will replace `{NAME1}` and `{NAME2}` in the query.
Both elements of each pair will be different from eachother.
Each time the benchmark for this query is done, the same 10 pairs are used.

### Query List

Example for `QUERY_FILE` with a query that is a sequence:
```
{
  'name': 'Some simple queries',
  'queries':
  [
    {
      'title': "Sequence",
      'queryList': [2,3,4,5],
      'connectionmanagement': {
        'timeout': 600,
        'numProcesses': 1,
        'runsPerConnection': 4
      },
      'numRun': 12,
    },
  ]
}
```
This query does not have a `query` attribute, but an attribute `queryList`.
It is a list of other queries, here number `2`, `3`, `4` and `5`.
The 12 benchmark runs are done by running these four queries one after the other, three times in total.
Here, we reconnect each time the sequence is through (`runsPerConnection` = 4) and we simulate one parallel client (`numProcesses` = 1).

This also respects randomization, i.e. every DBMS receives exactly the same versions of the queries in the same order.

### Query

This parameter sets reading or running benchmarks to one fixed query.
For `mode=run` this means the fixed query is benchmarked (again), no matter if benchmarks already exist for this query.
For `mode=continue` this means missing benchmarks are performed for this fixed query only.
If reports are about to be generated, only the report for this fixed query is generated.
This does not apply to the latex reporter, which always generates a complete report due to technical reasons.
Queries are numbered starting at 1.

### Connection

This parameter sets running benchmarks to one fixed DBMS (connection).
For `mode=run` this means the fixed DBMS is benchmarked (again), no matter if benchmarks already exist for it.
For `mode=continue` this means missing benchmarks are performed for this fixed DBMS only.
If reports are about to be generated, all reports involving this fixed DBMS are generated.
Connections are called by name.

### Generate reports

If set to yes, some reports are generated each time a benchmark of a single connection and query is finished.
Currently this means
* [bar charts](Evaluations.md#timers-per-query) as png files
* [plots](Evaluations.md#plot-of-values) as png files
* [boxplots](Evaluations.md#boxplot-of-values) as png files
* [dataframer](Evaluations.md#all-benchmark-times) - benchmark times as pickled dataframe files
* [pickler](Evaluations.md#statistics-table) - statistics as pickled dataframe files
* [metricer](Evaluations.md#hardware-metrics-per-query) - hardware metrics as png and csv files
* latexer, see an [example report](Report-example-tpch.pdf), also containing all plots and charts, and possibly error messages and fetched result tables. The latex reporter demands all other reporters to be active.

Reports are generated per query, that is one for each entry in the list in the `QUERY_FILE`.
The latex survey file contains all latex reports, that is all [evaluations](Evaluations.md) for all queries.

### Generate evaluation

If set to yes, an evaluation file is generated. This is a JSON file containing most of the [evaluations](Evaluations.md).
It can be accessed most easily using the inspection class or the interactive dashboard.

### Debug

This flag activates output of debug infos.

### Batch

This flag changes the output slightly and should be used for logging if script runs in background.
This also means reports are generated only at the end of processing.
Batch mode is automatically turned on if debug mode is used.

### Verbosity Level

Using the flags `-vq` means each query that is sent is dumped to stdout.
Using the flags `-vs` means after each query that has been finished, some statistics are dumped to stdout.

### Working querywise or connectionswise

This options sets if benchmarks are performed per query (one after the other is completed) or per connection (one after the other is completed).

This means processing `-w query` is
* loop over queries q
  * loop over connections c
    * making n benchmarks for q and c
    * compute statistics
    * save results
    * generate reports  

and processing `-w connection` is
* loop over connections c
  * loop over queries q
    * making n benchmarks for q and c
    * compute statistics
    * save results
    * generate reports

### Anonymize

Setting `-a` anonymizes all dbms.
This hides the name of the connections consistently in all reports.
You may unanonymize one or more dbms by using `-u` followed by a list of names of connections.

Example: `python3 benchmark.py read -r 1234 -a -u MySQL` would hide the name of all connections except for MySQL.

### Latex reports

The option `-l` can be used to change the templates for the generation of latex reports. The default is `pagePerQuery`.

Example: `python3 benchmark.py read -r 1234 -g yes -l simple` would use the templates located in `latex/simple`.

### Client processes

This tool simulates parallel queries from several clients.
The option `-p` can be used to change the global setting for the number of parallel processes.
Moreover each connection can have a local values for this parameter.
If nothing is specified, the default value is used, which is half of the number of processors.

### Random Seed
The option `-s` can be used to specify a random seed.
This should guarantee reproducible results for randomized queries.

