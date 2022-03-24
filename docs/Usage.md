# OLD: Usage

## Featured Usage for Benchmarking

This tool can be used to
* [run](#run-benchmarks) benchmarks
* [continue](#continue-benchmarks) aborted benchmarks
* [rerun](#rerun-benchmarks) benchmarks for one fixed [query](#rerun-benchmarks-for-one-query) and/or one fixed [DBMS](#rerun-benchmarks-for-one-connection)
* add benchmarks for more [queries](#continue-benchmarks-for-more-queries) or for more [DBMS](#continue-benchmarks-for-more-connections)
* [read](#read-stored-benchmarks) finished benchmarks

Basically this can be done running `dbmsbenchmarker run` or `dbmsbenchmarker continue` with additional parameters.

## Run benchmarks

`python3 benchmark.py run -f test` generates a folder containing result files: csv of benchmarks per query.
The example uses `test/connections.config` and `test/queries.config` as config files.

Example: This produces a folder containing
```
connections.config
queries.config
protocol.json
query_1_connection.csv
query_1_execution.csv
query_1_transfer.csv
query_2_connection.csv
query_2_execution.csv
query_2_transfer.csv
query_3_connection.csv
query_3_execution.csv
query_3_transfer.csv
```
where
- `connections.config` is a copy of the input file
- `queries.config` is a copy of the input file
- `protocol.json`: JSON file containing error messages (up to one per query and connection), durations (per query) and retried data (per query)
- `query_n_connection.csv`: CSV containing times (columns) for each dbms (rows) for query n - duration of establishing JDBC connection
- `query_n_execution.csv`: CSV containing times (columns) for each dbms (rows) for query n - duration of execution
- `query_n_transfer.csv`: CSV containing times (columns) for each dbms (rows) for query n - duration of data transfer

## Read stored benchmarks

`python3 benchmark.py read  -r 12345` reads files from folder `12345`containing result files and shows summaries of the results.       


## Continue benchmarks

`python3 benchmark.py continue -r 12345 -g yes` reads files from folder `12345` containing result files and continues to perform possibly missing benchmarks.
This is useful if a run had to be stopped. It continues automatically at the first missing query.
It can be restricted to specific queries or connections using `-q` and `c` resp.
The example uses `12345/connections.config` and `12345/queries.config` as config files.

### Continue benchmarks for more queries
You would go to a result folder, say `12345`, and add queries to the query file.
`python3 benchmark.py continue -r 12345 -g yes` then reads files from folder `12345` and continue benchmarking the new (missing) queries.

**Do not remove existing queries, since results are mapped to queries via their number (position). Use `active` instead.**

### Continue benchmarks for more connections
You would go to a result folder, say `12345`, and add connections to the connection file.
`python3 benchmark.py continue -r 12345 -g yes` then reads files from folder `12345` and continue benchmarking the new (missing) connections.

**Do not remove existing connections, since their results would not make any sense anymore. Use `active` instead.**

## Rerun benchmarks

`python3 benchmark.py run -r 12345 -g yes` reads files from folder `12345` containing result files and performs benchmarks again.
It also performs benchmarks of missing queries.
It can be restricted to specific queries or connections using `-q` and `c` resp.
The example uses `12345/connections.config` and `12345/queries.config` as config files.

### Rerun benchmarks for one query

`python3 benchmark.py run -r 12345 -g yes -q 5` reads files from folder `12345`containing result files and performs benchmarks again.
The example uses `12345/connections.config` and `12345/queries.config` as config files.
In this example, query number 5 is benchmarked (again) in any case.

### Rerun benchmarks for one connection

`python3 benchmark.py run -r 12345 -g yes -c MySQL` reads files from folder `12345`containing result files and performs benchmarks.
The example uses `12345/connections.config` and `12345/queries.config` as config files.
In this example, the connection named MySQL is benchmarked (again) in any case.

