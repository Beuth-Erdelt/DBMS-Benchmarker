"""
    Small demo for inspection of finished benchmarks.
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
import pandas as pd

#import logging
#logging.basicConfig(level=logging.DEBUG)

###################
##### general properties
###################

# path of folder containing experiment results
resultfolder = "./"

# create evaluation object for result folder
evaluate = inspector.inspector(resultfolder)

# list of all experiments in folder
evaluate.list_experiments

# dataframe of experiments
evaluate.get_experiments_preview()

# pick last experiment
code = evaluate.list_experiments[len(evaluate.list_experiments)-1]

# load it
evaluate.load_experiment(code)


###################
##### general experiment properties
###################

# hardware metrics
# list all available metrics
monitor.metrics.metrics

# get experiment workflow
df = evaluate.get_experiment_workflow()

# get workload properties
workload_properties = evaluate.get_experiment_workload_properties()
workload_properties['code']
workload_properties['path']
workload_properties['name']
workload_properties['info']
workload_properties['intro']
workload_properties['connectionmanagement']
workload_properties['reporting']

# list queries
list_queries = evaluate.get_experiment_list_queries()

# list connections
list_nodes = evaluate.get_experiment_list_nodes()
list_dbms = evaluate.get_experiment_list_dbms()
list_connections = evaluate.get_experiment_list_connections()
list_connections_node = evaluate.get_experiment_list_connections_by_node()
list_connections_dbms = evaluate.get_experiment_list_connections_by_dbms()
list_connections_clients = evaluate.get_experiment_list_connections_by_connectionmanagement('numProcesses')
list_connections_gpu = evaluate.get_experiment_list_connections_by_hostsystem('GPU')

# fix some examples:
# first connection, first query, first run
connection = list_connections[0]
numQuery = 1
numRun = 0

# show pretty some values
evaluator.pretty(evaluate.get_experiment_list_connections_by_dbms())
evaluator.pretty(evaluate.get_experiment_connection_properties(connection))

# get all dbms properties
# address: connection_properties[connection]['name']
connection_properties = evaluate.get_experiment_connection_properties()

# get single connection properties
connection_properties = evaluate.get_experiment_connection_properties(connection)
connection_properties['name']
connection_properties['hostsystem']
connection_properties['connectionmanagement']
connection_properties['connectionmanagement']['numProcesses']

# get single connection initialization script
evaluate.get_experiment_initscript(connection)

# get all queries properties
# address: query_properties[str(numQuery)]['title']
query_properties = evaluate.get_experiment_query_properties()
query_properties[str(numQuery)]['title']

# get single query properties
query_properties = evaluate.get_experiment_query_properties(numQuery)
query_properties['title']
query_properties['numRun']
query_properties['numWarmup']
query_properties['numCooldown']
query_properties['timer']
query_properties['query']
query_properties['parameter']
query_properties['active']




###################
##### general survey
###################

# some dataframes
# showing per DBMS and query overview
#
# measures, show as table
evaluate.get_aggregated_query_statistics(type='timer', name='run', query_aggregate='Mean')
evaluate.get_aggregated_query_statistics(type='throughput', name='run', query_aggregate='Mean')
evaluate.get_aggregated_query_statistics(type='latency', name='run', query_aggregate='Mean')
evaluate.get_aggregated_query_statistics(type='timer', name='connection', query_aggregate='Mean')
evaluate.get_aggregated_query_statistics(type='timer', name='execution', query_aggregate='Mean')
evaluate.get_aggregated_query_statistics(type='timer', name='datatransfer', query_aggregate='Mean')
evaluate.get_aggregated_query_statistics(type='monitoring', name='total_gpu_util', query_aggregate='Mean')
evaluate.get_aggregated_query_statistics(type='monitoring', name='total_gpu_memory', query_aggregate='Max')
# measures, show as heatmap (normalized)
evaluate.get_aggregated_query_statistics(type='timer', name='run', query_aggregate='factor')
# example: filter DBMS
list_omnisci = list_connections_dbms['OmniSci']
evaluate.get_aggregated_query_statistics(type='monitoring', name='total_gpu_memory', query_aggregate='factor', factor_base='Max', dbms_filter=list_omnisci)
# size of result sets
evaluate.get_total_resultsize()
# relative size of result sets (minimum=100%), show as heatmap
evaluate.get_total_resultsize_normalized()
# if there was an error, show as heatmap
evaluate.get_total_errors()
# if there was a warning, show as heatmap
evaluate.get_total_warnings()
# total times benchmarking a query took, show as table
evaluate.get_total_times()
# same, normalized to 100% per query, show as stacked area plot
evaluate.get_total_times_normalized()
# same, normed to relative to best per query, show as heatmap
evaluate.get_total_times_relative()
# get hardware metrics for loading test
evaluate.get_loading_metrics('total_cpu_memory')


# some dataframes
# showing metrics per dbms by aggregation
#
# from measures, show as bar plot
evaluate.get_aggregated_experiment_statistics(type='timer', name='run', query_aggregate='Mean', total_aggregate='Mean')
evaluate.get_aggregated_experiment_statistics(type='throughput', name='run', query_aggregate='Mean', total_aggregate='Geo')
evaluate.get_aggregated_experiment_statistics(type='timer', name='run', query_aggregate='factor', total_aggregate='Geo')
evaluate.get_aggregated_experiment_statistics(type='monitoring', name='total_gpu_memory', query_aggregate='Mean', total_aggregate='Mean')
# average data obtained from monitoring
# average per second - this differs from mean of mean, because not all queries have the same duration
evaluate.get_survey_monitoring()
# host data obtained from config
evaluate.get_survey_hostdata()
# example for merging two dataframes (by index)
df1 = evaluate.get_survey_monitoring()
df2 = evaluate.get_survey_hostdata()
tools.dataframehelper.merge(df1, df2)



###################
##### per query details
###################

# get measures of a query
# 
# df1 = measures for plot, boxplot, histogramm
# df2 = statistics as table
df1, df2 = evaluate.get_measures_and_statistics(numQuery, type='timer', name='connection')
evaluate.get_measures_and_statistics(numQuery, type='timer', name='execution')
evaluate.get_measures_and_statistics(numQuery, type='timer', name='datatransfer')
evaluate.get_measures_and_statistics(numQuery, type='timer', name='run')
evaluate.get_measures_and_statistics(numQuery, type='timer', name='session')
evaluate.get_measures_and_statistics(numQuery, type='throughput', name='session')
evaluate.get_measures_and_statistics(numQuery, type='monitoring', name='total_gpu_util')
evaluate.get_measures_and_statistics(numQuery, type='monitoring', name='total_cpu_throttled')
evaluate.get_measures_and_statistics(numQuery, type='monitoring', name='total_gpu_memory')
evaluate.get_measures_and_statistics(numQuery, type='monitoring', name='total_gpu_memory', dbms_filter=list_omnisci, factor_base='Min')

# example: filter dbms, ignore 1 warmup
df1,df2=evaluate.get_measures_and_statistics(6, type='timer', name='run', warmup=1, dbms_filter=list_connections_dbms['MemSQL'])
# example: add statistics per column
evaluator.addStatistics(df2.T).T

# get errors for query
list_errors = evaluate.get_error(numQuery)
# restrict to connection
list_errors[connection]

# get warnings for query
list_warnings = evaluate.get_warning(numQuery)
# restrict to connection
list_warnings[connection]

# get data storage for query as list
list_storage = evaluate.get_datastorage_list(numQuery)
# restrict to run
list_storage[numRun]

# get data storage for query and run as dataframe
df = evaluate.get_datastorage_df(numQuery, numRun)

# get data storage for query and run as dataframe
# this only contains results if they differ from data storage 
df2 = evaluate.get_resultset_df(numQuery, connection, numRun)

inspector.getDifference12(df2,df)

# get (sql) parameters of a query as dataframe
df = evaluate.get_parameter_df(numQuery)

# get size of data storage for query
evaluate.get_datastorage_size(numQuery)

# get size of received data of query in bytes
list_received_sizes = evaluate.get_received_sizes(numQuery)
# restrict to connection
list_received_sizes[connection]
# pretty print it
tools.sizeof_fmt(list_received_sizes[connection])




###################
##### per query aggregation
###################

# collect 3 aggregated metrics for query
df1, df2 = evaluate.get_measures_and_statistics(numQuery, type='timer', name='run')
df = tools.dataframehelper.collect(df2, 'Mean', 'timer_run_mean')
df1, df2 = evaluate.get_measures_and_statistics(numQuery, type='timer', name='session')
df = tools.dataframehelper.collect(df2, 'Mean', 'timer_session_mean', df)
df1, df2 = evaluate.get_measures_and_statistics(numQuery, type='throughput', name='session')
df = tools.dataframehelper.collect(df2, 'Median', 'throughput_session_median', df)
# show as table and bar chart
df



###################
##### examples
###################

# colors by dbms
list_connections_dbms = evaluate.get_experiment_list_connections_by_dbms()
connection_colors = evaluate.get_experiment_list_connection_colors(list_connections_dbms)
#connection_colors = evaluate.get_experiment_list_connection_colors(list_connections_node)

numQuery=21
df1,df2=evaluate.get_measures_and_statistics(numQuery, type='timer', name='run', warmup=0)
# using colors in matplotlib plots
#import matplotlib.pyplot as plt
#df1.T.plot(color=[connection_colors.get(x, '#333333') for x in df1.T.columns])
#plt.show()


# Some plotly figures
import plotly.graph_objects as go

# Plots
fig = go.Figure()
for i in range(len(df1.index)):
    t = fig.add_trace(go.Scatter(x=df1.T.index, y=df1.iloc[i], name=df1.index[i], line=dict(color=connection_colors[df1.index[i]], width=1)))

fig.show()

# Boxplots
fig = go.Figure()
for i in range(len(df1.index)):
    t = fig.add_trace(go.Box(y=df1.iloc[i], name=df1.index[i], line=dict(color=connection_colors[df1.index[i]], width=1), boxmean='sd'))

fig.show()

# Histograms
fig = go.Figure(layout = go.Layout(barmode='overlay'))
for i in range(len(df1.index)):
    t = fig.add_trace(go.Histogram(x=df1.iloc[i], name=df1.index[i], opacity=0.75, marker=dict(color=connection_colors[df1.index[i]])))

fig.show()

# Table of statistics
df=df2.applymap(lambda x: ("%.2f" % float(x)))
df_2 = df.reset_index()
fig = go.Figure(data=[go.Table(
    header=dict(values=list(df_2.columns),
                fill_color='paleturquoise',
                align='left'),
    cells=dict(values=df_2.T.values.tolist(),
               fill_color='lavender',
               align='left'))
])
fig.show()

# Table of aggregated measures
df = evaluate.get_aggregated_query_statistics(type='timer', name='run', query_aggregate='Mean')
df=df.applymap(lambda x: ("%.2f" % x))
df_2 = df.reset_index()
fig = go.Figure(data=[go.Table(
    header=dict(values=list(df_2.columns),
                fill_color='paleturquoise',
                align='left'),
    cells=dict(values=df_2.T.values.tolist(),
               fill_color='lavender',
               align='left'))
])
fig.show()

# Heatmap
df = evaluate.get_aggregated_query_statistics(type='timer', name='run', query_aggregate='factor')
df = df.sort_index()
df_2=df.applymap(lambda x: ("%.2f" % x))
fig = go.Figure(data=[go.Heatmap(z=df_2.T.values.tolist(),x=df_2.index, y=df_2.columns,colorscale='Reds')])
# optionally fixed size
#t = fig.update_layout(autosize=False,height=2000)
fig.show()

# Bar
df1, df2 = evaluate.get_measures_and_statistics(numQuery, type='timer', name='run')
df = tools.dataframehelper.collect(df2, 'Mean', 'timer_run_mean')
fig = go.Figure()
for i in range(len(df.index)):
    t = fig.add_trace(go.Bar(x=[df.index[i]], y=df.iloc[i], name=df.index[i], marker=dict(color=connection_colors[df.index[i]])))

fig.show()
