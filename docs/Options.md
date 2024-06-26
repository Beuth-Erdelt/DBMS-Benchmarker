# Parameter

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

# Options

## Command Line Options and Configuration

How to configure the benchmarker can be illustrated best by looking at the source code of the command line tool `benchmark.py`, which will be described in the following.

`python benchmark.py -h`

```
usage: dbmsbenchmarker [-h] [-d] [-b] [-qf QUERY_FILE] [-cf CONNECTION_FILE] [-q QUERY] [-c CONNECTION] [-ca CONNECTION_ALIAS] [-f CONFIG_FOLDER] [-r RESULT_FOLDER] [-e {no,yes}] [-w {query,connection}] [-p NUMPROCESSES] [-s SEED] [-cs] [-ms MAX_SUBFOLDERS]
                       [-sl SLEEP] [-st START_TIME] [-sf SUBFOLDER] [-sd {None,csv,pandas}] [-vq] [-vs] [-vr] [-vp] [-pn NUM_RUN] [-m] [-mps] [-sid STREAM_ID] [-ssh STREAM_SHUFFLE] [-wli WORKLOAD_INTRO] [-wln WORKLOAD_NAME]
                       {run,read,continue}

A benchmark tool for RDBMS. It connects to a given list of RDBMS via JDBC and runs a given list benchmark queries. Optionally some reports are generated.

positional arguments:
  {run,read,continue}   run benchmarks and save results, or just read benchmark results from folder, or continue with missing benchmarks only

optional arguments:
  -h, --help            show this help message and exit
  -d, --debug           dump debug informations
  -b, --batch           batch mode (more protocol-like output), automatically on for debug mode
  -qf QUERY_FILE, --query-file QUERY_FILE
                        name of query config file
  -cf CONNECTION_FILE, --connection-file CONNECTION_FILE
                        name of connection config file
  -q QUERY, --query QUERY
                        number of query to benchmark
  -c CONNECTION, --connection CONNECTION
                        name of connection to benchmark
  -ca CONNECTION_ALIAS, --connection-alias CONNECTION_ALIAS
                        alias of connection to benchmark
  -f CONFIG_FOLDER, --config-folder CONFIG_FOLDER
                        folder containing query and connection config files. If set, the names connections.config and queries.config are assumed automatically.
  -r RESULT_FOLDER, --result-folder RESULT_FOLDER
                        folder for storing benchmark result files, default is given by timestamp
  -e {no,yes}, --generate-evaluation {no,yes}
                        generate new evaluation file
  -w {query,connection}, --working {query,connection}
                        working per query or connection
  -p NUMPROCESSES, --numProcesses NUMPROCESSES
                        Number of parallel client processes. Global setting, can be overwritten by connection. Default is 1.
  -s SEED, --seed SEED  random seed
  -cs, --copy-subfolder
                        copy subfolder of result folder
  -ms MAX_SUBFOLDERS, --max-subfolders MAX_SUBFOLDERS
                        maximum number of subfolders of result folder
  -sl SLEEP, --sleep SLEEP
                        sleep SLEEP seconds before going to work
  -st START_TIME, --start-time START_TIME
                        sleep until START-TIME before beginning benchmarking
  -sf SUBFOLDER, --subfolder SUBFOLDER
                        stores results in a SUBFOLDER of the result folder
  -sd {None,csv,pandas}, --store-data {None,csv,pandas}
                        store result of first execution of each query
  -vq, --verbose-queries
                        print every query that is sent
  -vs, --verbose-statistics
                        print statistics about queries that have been sent
  -vr, --verbose-results
                        print result sets of every query that has been sent
  -vp, --verbose-process
                        print result sets of every query that has been sent
  -pn NUM_RUN, --num-run NUM_RUN
                        Parameter: Number of executions per query
  -m, --metrics         collect hardware metrics per query
  -mps, --metrics-per-stream
                        collect hardware metrics per stream
  -sid STREAM_ID, --stream-id STREAM_ID
                        id of a stream in parallel execution of streams
  -ssh STREAM_SHUFFLE, --stream-shuffle STREAM_SHUFFLE
                        shuffle query execution based on id of stream
  -wli WORKLOAD_INTRO, --workload-intro WORKLOAD_INTRO
                        meta data: intro text for workload description
  -wln WORKLOAD_NAME, --workload-name WORKLOAD_NAME
                        meta data: name of workload
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

### Monitoring

The parameter `--metrics` can be used to activate fetching metrics from a Prometheus server.
In the `connection.config` we may insert a section per connection about where to fetch these metrics from and which metrics we want to obtain.  

More information about monitoring and metrics can be found here: https://github.com/Beuth-Erdelt/Benchmark-Experiment-Host-Manager/blob/master/docs/Monitoring.html

The parameter `--metrics-per-stream` does the same, but collects the metrics per stream - not per query.
This is useful when queries are very fast.

### Query

This parameter sets reading or running benchmarks to one fixed query.
For `mode=run` this means the fixed query is benchmarked (again), no matter if benchmarks already exist for this query.
For `mode=continue` this means missing benchmarks are performed for this fixed query only.
Queries are numbered starting at 1.

### Connection

This parameter sets running benchmarks to one fixed DBMS (connection).
For `mode=run` this means the fixed DBMS is benchmarked (again), no matter if benchmarks already exist for it.
For `mode=continue` this means missing benchmarks are performed for this fixed DBMS only.
Connections are called by name.

### Generate evaluation

If set to yes, an evaluation file is generated. This is a JSON file containing most of the [evaluations](Evaluations.html).
It can be accessed most easily using the inspection class or the interactive dashboard.

### Debug

This flag activates output of debug infos.

### Sleep

Time in seconds to wait before starting to operate.
This is handy when we want to wait for other systems (e.g. a DBMS) to startup completely.

### Batch

This flag changes the output slightly and should be used for logging if script runs in background.
This also means reports are generated only at the end of processing.
Batch mode is automatically turned on if debug mode is used.

### Verbosity Level

Using the flags `-vq` means each query that is sent is dumped to stdout.
Using the flags `-vr` means each result set that is received is dumped to stdout.
Using the flags `-vp` means more information about the process and connections are dumped to stdout.
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

Default is `connection`-wise.

### Client processes

This tool simulates parallel queries from several clients.
The option `-p` can be used to change the global setting for the number of parallel processes.
Moreover each connection can have a local values for this parameter.
If nothing is specified, the default value 1 is used.
To circumvent Python's GIL, the module `multiprocessing` is used.
For each combination connection/query, a pool of asynchronous subprocesses is spawned.
The subprocesses connect to the DBMS and send the query.
Note this implies a reconnection and the creation of a JVM.
When all subprocesses are finished, results are joined and dbmsbenchmarker may proceed to the next query.
This helps in evaluating concurrency on a query level.
You can for example compare performance of 15 clients running TPC-H Q8 at the same time.
If you want to evaluate concurrency on stream level with a single connection per client, you should start several dbmsbenchmarker. 

This should be changed in align with the number of runs per query (`-pn`), that is, the number of runs must be higher than the number of clients.
Ideally, the number of runs should be a multiple of the number of parallel clients.

### Random Seed
The option `-s` can be used to specify a random seed.
This should guarantee reproducible results for randomized queries.

### Subfolders

If the flag `--copy-subfolder` is set, connection and query configuration will be copied from an existing result folder to a subfolder.
The name of the subfolder can be set via `--subfolder`.
These flags can be used to allow parallel quering of independent dbmsbenchmarker:
Each will write in an own subfolder.
These partial results can be merged using the `merge.py` command line tool.
The normal behaviour is: If we run the same connection twice, the results of the first run will be overwritten.
Since we might query the same connection in these instances, the subfolders will be numbered automatically.
Using `MAX_SUBFOLDERS` we can limit the number of subfolders that are allowed.  
Example: `-r /tmp/dbmsresults/1234/ -cs -sf MySQL` will continue the benchmarks of folder `/tmp/dbmsresults/1234/` by creating a folder `/tmp/dbmsresults/1234/MySQL-1`.
If that folder already exists, `/tmp/dbmsresults/1234/MySQL-2` will be used etc.

This is in particular used by https://github.com/Beuth-Erdelt/Benchmark-Experiment-Host-Manager for jobs of parallel benchmarker.

### Delay start

The parameter `--sleep` can be used to set a start time.
DBMSBenchmarker will wait until the given time is reached.

This is in particular used by https://github.com/Beuth-Erdelt/Benchmark-Experiment-Host-Manager for synching jobs of parallel benchmarker.

## Query File

Contains the queries to benchmark.

Example for `QUERY_FILE`:
```
{
  'name': 'Some simple queries',
  'intro': 'Some describing text about this benchmark test setup',
  'info': 'It runs on a P100 GPU',
  'factor': 'mean',
  'queries':
  [
    {
      'title': "Count all rows in test",
      'query': "SELECT COUNT(*) FROM test",
      'delay': 0,
      'numRun': 10,
    },
  ]
}
```

* `name`: Name of the list of queries
* `intro`: Introductional text for reports
* `info`: Short info about the current experiment
* `factor`: Determines the measure for comparing performances (optional). Can be set to `mean` or `median` or `relative`. Default is `mean`.
* `query`: SQL query string
* `title`: Title of the query
* `delay`: Number of seconds to wait before each execution statement. This is for throtteling. Default is 0.
* `numRun`: Number of runs of this query for benchmarking

Such a query will be executed 10 times and the time of execution will be measured each time.

### Extended Query File

Extended example for `QUERY_FILE`:
```
{
  'name': 'Some simple queries',
  'intro': 'This is an example workload',
  'info': 'It runs on a P100 GPU',
  'connectionmanagement': {
    'timeout': 600,             # in seconds
    'numProcesses': 4,          # number of parallel client processes
    'runsPerConnection': 5,     # number of runs performed before connection is closed
    'singleConnection': False   # if connection should be used for the complete stream
  },
  'queries':
  [
    {
      'title': "Count all rows in test",
      'query': "SELECT COUNT(*) c FROM test",
      'DBMS': {
        'MySQL': "SELECT COUNT(*) AS c FROM test"
      }
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

#### SQL Dialects

The `DBMS` key allows to specify SQL dialects. All connections starting with the key in this dict with use the specified alternative query. In the example above, for instance a connection 'MySQL-InnoDB' will use the alternative.
Optionally at the definition of the connections an attribute `dialect` can be used. For example MemSQL may use the dialect `MySQL`.

#### Connection Management

The first `connectionmanagement` options set global values valid for all DBMS. This can be overwritten by the settings in the [connection config](#connection-file). The second `connectionmanagement` is fixed valid for this particular query and cannot be overwritten.
  * `timeout`: Maximum lifespan of a connection. Default is None, i.e. no limit.
  * `numProcesses`: Number of parallel client processes. Default is 1.
  * `runsPerConnection`: Number of runs performed before connection is closed. Default is None, i.e. no limit.
  * `singleConnection`: This indicates if the connection should be used for the complete stream of queries. Default is True. Switch this off, if you want to have reconnects during the stream, for example to inspect the effect of reconnection of execution times.


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
  'defaultParameters': {'SF': '10'},
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

`defaultParameters` can be used to set parameters that hold for the complete workload.

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


## Connection File

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
    'JDBC': {
      'driver': "com.mysql.cj.jdbc.Driver",
      'url': "jdbc:mysql://localhost:3306/database",
      'auth': ["username", "password"],
      'jar': "mysql-connector-java-8.0.13.jar"
    },
    'init_SQL': "USE tpch",
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
    },
    'monitoring': {
      'shift': 0,
      'extend': 20,
      'prometheus_url': 'http://127.0.0.1:9090/api/v1/',
      'metrics': {
        'total_cpu_memory': {
          'query': 'container_memory_working_set_bytes{job="monitor-node"}/1024/1024',
          'title': 'CPU Memory [MiB]'
        }
      }
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
* `init_SQL`: Optional command, that is sent once, when the connection has been established
* Additional information useful for reporting and also used for computations
  * `timeload`: Time for ingest (in milliseconds), because not part of the benchmark
  * `priceperhourdollar`: Used to compute total cost based on total time (optional)
* `connectionmanagement`: Parameter for connection management. This overwrites general settings made in the [query config](#extended-query-file) and can be overwritten by query-wise settings made there.
  * `timeout`: Maximum lifespan of a connection. Default is None, i.e. no limit.
  * `numProcesses`: Number of parallel client processes. Default is 1.
  * `runsPerConnection`: Number of runs performed before connection is closed. Default is None, i.e. no limit.
  * `singleConnection`: This indicates if the connection should be used for the complete stream of queries. Default is True. Switch this off, if you want to have reconnects during the stream, for example to inspect the effect of reconnection of execution times.
* `hostsystem`: Describing information for report in particular about the host system.
  This can be written automatically by https://github.com/Beuth-Erdelt/Benchmark-Experiment-Host-Manager
* `monitoring`: We might also add information about fetching [monitoring](#monitoring) metrics.
  * `prometheus_url`: URL to API of Prometheus instance monitoring the system under test
  * `shift`: Shifts the fetched interval by `n` seconds to the future.
  * `extend`: Extends the fetched interval by `n` seconds at both ends.

