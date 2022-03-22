# Inspector

Start the inspector:
```
result_path = 'tmp/results'
code = '1234512345'
benchmarks = benchmarker.inspector(result_path, code)
```

## Get General Informations and Evaluations

```
# list of successful queries
qs = benchmarks.listQueries()
# list of connections
cs = benchmarks.listConnections()
# print all errors
benchmarks.printErrors()
# get survey evaluation
dftt, title = benchmarks.getTotalTime()
dfts, title = benchmarks.getSumPerTimer()
dftp, title = benchmarks.getProdPerTimer()
dftr, title = benchmarks.generateSortedTotalRanking()
# get evaluation dict
e = evaluator.evaluator(benchmarks)
# show it
e.pretty()
# show part about query 1
e.pretty(e.evaluation['query'][1])
# get dataframe of benchmarks for query 1 and timerRun
dfb1 = benchmarks.benchmarksToDataFrame(1,benchmarks.timerRun)
# get dataframe of statistics for query 1 and timerRun
dfs1 = benchmarks.statsToDataFrame(1,benchmarks.timerRun)
```

## Get Informations and Evaluations for a Specific DBMS and Query:
```
# pick first connection (dbms)
connectionname = cs[0]

# pick a query
numQuery = 10

# get infos about query
q = benchmarks.getQueryObject(numQuery)
print(q.title)

# get benchmarks and statistics for specific query
dfb1 = benchmarks.getBenchmarks(numQuery)
dfb1b = benchmarks.getBenchmarksCSV(numQuery)
dfs1 = benchmarks.getStatistics(numQuery)
dfr1 = benchmarks.getResultSetDF(numQuery, connectionname)

# get error of connection at specific query
benchmarks.getError(numQuery, connectionname)

# get all errors of connection at specific query
benchmarks.getError(numQuery)

# get data storage (for comparison) for specific query and benchmark run
numRun = 0
df1=benchmarks.readDataStorage(numQuery,numRun)
df2=benchmarks.readResultSet(numQuery, cs[1],numRun)
inspector.getDifference12(df1, df2)

# get query String for specific query
queryString = benchmarks.getQueryString(numQuery)
print(queryString)

# get query String for specific query and and dbms
queryString = benchmarks.getQueryString(numQuery, connectionname=connectionname)
print(queryString)

# get query String for specific query and and dbms and benchmark run
queryString = benchmarks.getQueryString(numQuery, connectionname=connectionname, numRun=1)
print(queryString)
```

## Run some Isolated Queries

```
# run single benchmark run for specific query and connection
# this is for an arbitrary query string - not contained in the query config
# result is not stored and does not go into any reporting
queryString = "SELECT COUNT(*) c FROM test"
output = benchmarks.runIsolatedQuery(connectionname, queryString)
print(output.durationConnect)
print(output.durationExecute)
print(output.durationTransfer)
df = tools.dataframehelper.resultsetToDataFrame(output.data)
print(df)


# run single benchmark run multiple times for specific query and connection
# this is for an arbitrary query string - not contained in the query config
# result is not stored and does not go into any reporting
queryString = "SELECT COUNT(*) c FROM test"
output = benchmarks.runIsolatedQueryMultiple(connectionname, queryString, times=10)
print(output.durationConnect)
print(output.durationExecute)
print(output.durationTransfer)
# compute statistics for execution
df = tools.dataframehelper.timesToStatsDataFrame(output.durationExecute)
print(df)



# the following is handy when comparing result sets of different dbms

# run an arbitrary query
# this saves the result set data frame to
#   "query_resultset_"+connectionname+"_"+queryName+".pickle"
queryName = "test"
queryString = "SELECT COUNT(*) c FROM test"
benchmarks.runAndStoreIsolatedQuery(connectionname, queryString, queryName)

# we can also easily load this data frame
df = benchmarks.getIsolatedResultset(connectionname, queryName)
```

# Debug Tool

There is a debug tool, that helps to analyze result folders: `python evaluate.py -h`

```
usage: evaluate.py [-h] [-r RESULT_FOLDER] [-e EXPERIMENT] [-q QUERY] [-c CONNECTION] [-n NUM_RUN] [-d] [-rt] {resultsets,errors,warnings,query}

A debug tool for DBMSBenchmarker. It helps to analyze a result folder. It depends on the evaluation cube, so that cube must have been created before.

positional arguments:
  {resultsets,errors,warnings,query}
                        show debug infos about which part of the outcome

optional arguments:
  -h, --help            show this help message and exit
  -r RESULT_FOLDER, --result-folder RESULT_FOLDER
                        folder for storing benchmark result files, default is given by timestamp
  -e EXPERIMENT, --experiment EXPERIMENT
                        code of experiment
  -q QUERY, --query QUERY
                        number of query to inspect
  -c CONNECTION, --connection CONNECTION
                        name of DBMS to inspect
  -n NUM_RUN, --num-run NUM_RUN
                        number of run to inspect
  -d, --diff            show differences in result sets
  -rt, --remove-titles  remove titles when comparing result sets
 ```

It depends on the evaluation cube. In case, it can be generated by `dbmsbenchmarker -e yes -r 1647993954 read` for example for experiment `1647993954`.

## Show Queries

We can take a look at the actual queries that have been sent: `python evaluate.py -e 1647993954 -q 1 -n 0 query`

This shows the query string for query number 2, first run.

## Show Result Sets

We can take a look at the actual queries that have been sent: `python evaluate.py -e 1647993954 -q 2 resultsets`

This shows the query string for query number 2.

## Show Errors

We can take a look at the actual queries that have been sent: `python evaluate.py -e 1647993954 errors`

## Show Warnings

We can take a look at the actual queries that have been sent: `python evaluate.py -e 1647993954 warnings`

