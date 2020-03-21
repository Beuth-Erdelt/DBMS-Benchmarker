import pickle
from tabulate import tabulate
import pandas as pd
from os import listdir
from os.path import isdir, isfile, join
import sys
import ast
from colour import Color

from dbmsbenchmarker import benchmarker, tools, evaluator, monitor

color_ranges = [
    ["#ff0000", "#ffcccc"],
    ["#006600", "#ccffcc"],
    ["#000066", "#ccccff"],
    ["#666600", "#ffffcc"],
    ["#660066", "#ffccff"],
    ["#006666", "#ccffff"],
    ["#000000", "#ffffff"],
]

def getIntersection(df1, df2):
    return pd.merge(df1, df2, how='inner')
def getUnion(df1, df2):
    return pd.concat([df1, df2])
def getDifference12(df1, df2):
    return pd.concat([df1, df2, df2]).drop_duplicates(keep=False)
def completeSort(df):
    return df.sort_values(by=[df_union_all.columns[i] for i in range(0,len(df.columns))], ascending=True)
def list_intersection(lst1, lst2): 
    lst3 = [value for value in lst1 if value in lst2] 
    return lst3 


class inspector():
    """
    Class for inspecting done benchmarks
    """
    def __init__(self, result_path):
        self.result_path = result_path
        self.list_experiments = [f for f in listdir(self.result_path) if isdir(join(self.result_path, f)) and f.isdigit()]
        self.queries_successful = []
    def get_experiments_preview(self):
        workload_preview = {}
        for code in self.list_experiments:
            filename = self.result_path+'/'+code+'/queries.config'
            with open(filename,'r') as inp:
                workload_properties = ast.literal_eval(inp.read())
            filename = self.result_path+'/'+code+'/connections.config'
            with open(filename,'r') as inp:
                connection_properties = ast.literal_eval(inp.read())
            workload_preview[code] = {}
            workload_preview[code]['name'] = workload_properties['name']
            workload_preview[code]['info'] = workload_properties['info']
            workload_preview[code]['intro'] = workload_properties['intro']
            l = [q for q in workload_properties['queries'] if q['active'] == True]
            workload_preview[code]['queries'] = len(l)
            l = [c for c in connection_properties if c['active'] == True]
            workload_preview[code]['connections'] = len(l)
        return pd.DataFrame(workload_preview).T
    def load_experiment(self, code):
        self.queries_successful = []
        self.benchmarks = benchmarker.inspector(self.result_path, code)
        self.benchmarks.computeTimerRun()
        self.benchmarks.computeTimerSession()
        self.e = evaluator.evaluator(self.benchmarks, load=True, force=True)
    def get_experiment_list_queries(self):
        # list of successful queries
        return self.benchmarks.listQueries()
    def get_experiment_list_connections(self):
        # list of connections
        return self.benchmarks.listConnections()
    def get_experiment_list_dbms(self):
        # list all different dbms
        return list(self.get_experiment_list_connections_by_dbms().keys())
    def get_experiment_list_connections_by_dbms(self):
        # dict of lists of dbms
        dbms_list = {}
        for c,d in self.e.evaluation['dbms'].items():
            if not d['docker'] in dbms_list:
                dbms_list[d['docker']] = []
            dbms_list[d['docker']].append(c)
        return dbms_list
    def get_experiment_list_nodes(self):
        # list all different nodes
        return list(self.get_experiment_list_connections_by_node().keys())
    def get_experiment_list_connections_by_node(self):
        # dict of lists of node
        return self.get_experiment_list_connections_by_hostsystem('node')
    def get_experiment_list_connections_by_connectionmanagement(self, property):
        # dict of lists of node
        dbms_list = {}
        for c,d in self.e.evaluation['dbms'].items():
            if not d['connectionmanagement'][property] in dbms_list:
                dbms_list[d['connectionmanagement'][property]] = []
            dbms_list[d['connectionmanagement'][property]].append(c)
        return dbms_list
    def get_experiment_list_connections_by_hostsystem(self, property):
        # dict of lists of node
        dbms_list = {}
        for c,d in self.e.evaluation['dbms'].items() if 'hostsystem' in d:
            if not d['hostsystem'][property] in dbms_list:
                dbms_list[d['hostsystem'][property]] = []
            dbms_list[d['hostsystem'][property]].append(c)
        return dbms_list
    def get_experiment_list_connection_colors(self, list_connections):
        #list_connections_dbms = self.get_experiment_list_connections_by_dbms()
        dbms_colors={}
        list_colors = []
        num_colorset = 0
        for i,j in list_connections.items():
            list_colors.append(list(Color(color_ranges[num_colorset][0]).range_to(Color(color_ranges[num_colorset][1]), len(j))))
            for k,c in enumerate(j):
                dbms_colors[c] = '#%02x%02x%02x' % (int(255*list_colors[num_colorset][k].red), int(255*list_colors[num_colorset][k].green), int(255*list_colors[num_colorset][k].blue))
            num_colorset = num_colorset + 1
        return dbms_colors
    def get_experiment_connection_properties(self, connection=None):
        # dict of dict of dbms info
        if connection is not None:
            return self.e.evaluation['dbms'][connection]
        else:
            return self.e.evaluation['dbms']
    def get_experiment_query_properties(self, numQuery=None):
        # dict of query properties
        if numQuery is not None:
            return self.e.evaluation['query'][str(numQuery)]['config']
        else:
            return self.e.evaluation['query']
    def get_experiment_workload_properties(self):
        # dict of workload properties
        return self.e.evaluation['general']
    #def get_measures(self, numQuery, timer, warmup=0, cooldown=0):
    def get_timer(self, numQuery, timer, warmup=0, cooldown=0):
        # dataframe of dbms x measures
        return evaluator.dfMeasuresQ(numQuery, timer, warmup, cooldown)
    def get_lat(self, numQuery, name='run', warmup=0, cooldown=0):
        # dataframe of dbms x latencies
        return self.get_timer(numQuery, name, warmup, cooldown)
    def get_tp(self, numQuery, name='run', warmup=0, cooldown=0):
        # dataframe of dbms x throughput
        df = self.get_lat(numQuery, name, warmup, cooldown)
        for i,c in enumerate(df.index):
            numClients = self.e.evaluation['query'][str(numQuery)]['dbms'][c]['connectionmanagement']['numProcesses']
            df.loc[c] = numClients*1000.0/df.loc[c]
        return df
    def get_measures_and_statistics(self, numQuery, type='timer', name='run', dbms_filter=[], warmup=0, cooldown=0, factor_base='Mean'):
        if type == 'timer':
            df = self.get_timer(numQuery, name, warmup=warmup, cooldown=cooldown)
            df = evaluator.dfSubRows(df, dbms_filter)
            df_stat = evaluator.addStatistics(df, drop_nan=False, drop_measures=True)
        elif type == 'monitoring':
            df = self.get_hardware_metrics(numQuery, name, warmup=warmup, cooldown=cooldown)
            df = evaluator.dfSubRows(df, dbms_filter)
            df_stat = evaluator.addStatistics(df, drop_nan=False, drop_measures=True)
        elif type == 'latency':
            df = self.get_lat(numQuery, name, warmup=warmup, cooldown=cooldown)
            df = evaluator.dfSubRows(df, dbms_filter)
            df_stat = evaluator.addStatistics(df, drop_nan=False, drop_measures=True)
        elif type == 'throughput':
            df = self.get_tp(numQuery, name, warmup=warmup, cooldown=cooldown)
            df = evaluator.dfSubRows(df, dbms_filter)
            df_stat = evaluator.addStatistics(df, drop_nan=False, drop_measures=True)
        else:
            print("Unknown type")
            return None, None
        if len(factor_base) > 0:
            df_stat = evaluator.addFactor(df_stat, factor_base)
        return df, df_stat
    def get_aggregated_query_statistics(self, type='timer', name='run', dbms_filter=[], warmup=0, cooldown=0, factor_base='Mean', query_aggregate='Mean'):
        l = self.get_experiment_queries_successful()
        epos = [i for i,t in enumerate(self.benchmarks.timers) if t.name=='run']
        queries = l[epos[0]]
        column = query_aggregate
        df_aggregated = None
        for q in queries:
            column_new = 'Q{}'.format(q+1)
            df_measures, df_statistics = self.get_measures_and_statistics(q+1, type, name, dbms_filter, warmup, cooldown, factor_base)
            if df_aggregated is None:
                df_aggregated = pd.DataFrame(df_statistics[column]).rename(columns = {column: column_new})
            else:
                df_column = pd.DataFrame(df_statistics[column]).rename(columns = {column: column_new})
                df_aggregated = tools.dataframehelper.merge(df_aggregated, df_column)
        return df_aggregated
    def get_aggregated_experiment_statistics(self, type='timer', name='run', dbms_filter=[], warmup=0, cooldown=0, factor_base='Mean', query_aggregate='Mean', total_aggregate='Mean'):
        df = self.get_aggregated_query_statistics(type, name, dbms_filter, warmup, cooldown, factor_base, query_aggregate)
        df_stat = evaluator.addStatistics(df, drop_nan=False, drop_measures=True)
        return pd.DataFrame(df_stat[total_aggregate]).rename(columns = {total_aggregate: "total_"+type+"_"+name})
    def get_error(self, numQuery, connection=None):
        # error message of connection at query
        return self.benchmarks.getError(numQuery, connection)
    def get_warning(self, numQuery, connection=None):
        # warning message of connection at query
        return self.benchmarks.getWarning(numQuery, connection)
    def get_datastorage_list(self, numQuery):
        # list of data storage for query
        return self.benchmarks.protocol['query'][str(numQuery)]['dataStorage']
    def get_datastorage_df(self, numQuery, numRun=0):
        # dataframe of data storage for query and run
        return self.benchmarks.readDataStorage(numQuery, numRun)
    def get_resultset_df(self, numQuery, connection, numRun=0):
        # dataframe of received result set for query, connection and run
        return self.benchmarks.readResultSet(numQuery, connection=connection, numRun=numRun)
    def get_parameter_df(self, numQuery):
        # dataframe of run x parameter
        return self.benchmarks.getParameterDF(numQuery)
    def get_datastorage_size(self, numQuery):
        # size of data storage for query in bytes
        l = self.get_datastorage_list(numQuery)
        if len(l) > 0 and len(l[0]) > 0 and len(l[0][0]) > 0:
            # flatten
            l = [x for l1 in l for l2 in l1 for x in l2]
        return sys.getsizeof(l)
    def get_received_sizes(self, numQuery):
        # size of received data for connection and query in bytes
        return self.benchmarks.protocol['query'][str(numQuery)]['sizes']
    def get_experiment_workflow(self):
        # dataframe of experiment workflow
        return tools.dataframehelper.getWorkflow(self.benchmarks)
    def get_experiment_initscript(self, connection):
        # string of init script for connection
        def initfilename(c, i):
            return self.benchmarks.path+'/'+c+'_init_'+str(i)+'.log'
        def hasInitScript(c):
            return isfile(initfilename(c,0))
        script = ''
        if hasInitScript(connection):
            i = 0
            while True:
                filename=initfilename(connection, i)
                if isfile(filename):
                    script += open(filename).read()
                    i = i + 1
                else:
                    break
        return script
    def get_total_resultsize_normalized(self):
        return tools.dataframehelper.evaluateNormalizedResultsizeToDataFrame(self.e.evaluation).T
    def get_total_resultsize(self):
        return tools.dataframehelper.evaluateResultsizeToDataFrame(self.e.evaluation).T
    def get_total_errors(self):
        return tools.dataframehelper.evaluateErrorsToDataFrame(self.e.evaluation).T
    def get_total_warnings(self):
        return tools.dataframehelper.evaluateWarningsToDataFrame(self.e.evaluation).T
    def get_total_times(self):
        df, title = tools.dataframehelper.totalTimes(self.benchmarks)
        return df.T
    def get_total_times_normalized(self):
        df = self.get_total_times().T
        # adds to 100% per query
        return df.div(df.sum(axis=1)/100.0, axis=0).T
    def get_total_times_relative(self):
        df = self.get_total_times().T
        # is relative to mean per query
        return df.div(df.mean(axis=1)/100.0, axis=0).T
    def get_total_queuesize(self):
        return tools.dataframehelper.evaluateQueuesizeToDataFrame(self.e.evaluation).T
    def get_total_lat(self):
        return tools.dataframehelper.evaluateLatToDataFrame(self.e.evaluation).T
    def get_total_throughput(self):
        return tools.dataframehelper.evaluateTPSToDataFrame(self.e.evaluation).T
    def get_total_timer_factors(self, timername):
        epos = [i for i,t in enumerate(self.benchmarks.timers) if t.name==timername]
        timer = self.benchmarks.timers[epos[0]]
        return tools.dataframehelper.evaluateTimerfactorsToDataFrame(self.e.evaluation, timer).T
    def get_survey_monitoring(self):
        return tools.dataframehelper.evaluateMonitoringToDataFrame(self.e.evaluation)
    def get_survey_hostdata(self):
        return tools.dataframehelper.evaluateHostToDataFrame(self.e.evaluation)
    def get_survey_arithmetic(self):
        dfts, title = self.benchmarks.getSumPerTimer()
        return dfts
    def get_survey_geometrics(self):
        dftp, title = self.benchmarks.getProdPerTimer() # session and run missing
        return dftp
    def get_survey_ranking(self):
        dftr, title = self.benchmarks.generateSortedTotalRanking()
        return dftr
    def get_experiment_queries_successful(self):
        # list of active queries for timer e[0] = execution
        if len(self.queries_successful) > 0:
            return self.queries_successful
        else:
            self.queries_successful = tools.findSuccessfulQueriesAllDBMS(self.benchmarks, None, self.benchmarks.timers)
            return self.queries_successful
    def get_survey_successful(self, timername=None):
        # list of active queries for timer e[0] = execution
        if not timername is None:
            epos = [i for i,t in enumerate(self.benchmarks.timers) if t.name==timername]
            l = self.get_experiment_queries_successful()[epos[0]]
            return l
        else:
            return self.get_experiment_queries_successful()
    def get_hardware_metrics(self, numQuery, metric, warmup=0, cooldown=0):
        hw = monitor.metrics(self.benchmarks)
        df = hw.dfHardwareMetrics(numQuery, metric)
        numRunBegin = warmup
        numRunEnd = len(df.columns)-cooldown
        df = df.T[numRunBegin:numRunEnd].T
        return df
