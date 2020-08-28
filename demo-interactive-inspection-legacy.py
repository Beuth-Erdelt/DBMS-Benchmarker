"""
    Small demo for inspection of finished benchmarks and manual sending of queries in an interactive python shell.
    This uses the old interface.
    Copyright (C) 2020  Patrick Erdelt

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <https://www.gnu.org/licenses/>.
"""
from dbmsbenchmarker import *

resultfolder = "tmp/results"
code = '1234567890'
benchmarks = benchmarker.inspector(resultfolder, code)

# list of successful queries
qs = benchmarks.listQueries()
# list of connections
cs = benchmarks.listConnections()
# print all errors
benchmarks.printErrors()
# get survey evaluation as dataframes
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




# pick first connection (dbms)
connectionname = cs[0]

# pick a query
numQuery = 10

# get infos about query
q = benchmarks.getQueryObject(numQuery)
print(q.title)

# read benchmarks and statistics for specific query from disk
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
df1 = benchmarks.readDataStorage(numQuery,numRun)
df2 = benchmarks.readResultSet(numQuery, cs[1],numRun)
inspector.getDifference12(df1, df2)

# get query String for specific query
queryString = benchmarks.getQueryString(numQuery)
print(queryString)

# get query String for specific query and dbms
queryString = benchmarks.getQueryString(numQuery, connectionname=connectionname)
print(queryString)

# get query String for specific query and dbms and benchmark run
queryString = benchmarks.getQueryString(numQuery, connectionname=connectionname, numRun=1)
print(queryString)




# run single benchmark run for specific query and connection
# also set numRun for parametrized queries
# this is for a query contained in the query config
# result is not stored and does not go into any reporting
output = benchmarks.runSingleBenchmarkRun(numQuery, connectionname=connectionname, numRun=1)
print(output.durationConnect)
print(output.durationExecute)
print(output.durationTransfer)
df = tools.dataframehelper.resultsetToDataFrame(output.data)
print(df)

# run single benchmark run multiple times for specific query and connection
# also set numRun for parametrized queries
# this is for a query contained in the query config
# result is not stored and does not go into any reporting
output = benchmarks.runSingleBenchmarkRunMultiple(numQuery, connectionname=connectionname, numRun=1, times=20)
print(output.durationConnect)
print(output.durationExecute)
print(output.durationTransfer)
print(output.data)
# compute statistics for execution
df = tools.dataframehelper.timesToStatsDataFrame(output.durationExecute)
print(df)



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

