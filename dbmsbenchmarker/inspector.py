"""
    Classes for inspecting benchmarking results of the Python Package DBMS Benchmarker
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
import pickle
from tabulate import tabulate
import pandas as pd
from os import listdir, stat
from os.path import isdir, isfile, join
import sys
import ast
from colour import Color
from numpy import nan
from datetime import datetime, timezone
import copy
import re

from dbmsbenchmarker import benchmarker, tools, evaluator, monitor

color_ranges = [
    ["#ff0000", "#ffcccc"],
    ["#006600", "#ccffcc"],
    ["#000066", "#ccccff"],
    ["#666600", "#ffffcc"],
    ["#660066", "#ffccff"],
    ["#006666", "#ccffff"],
    ["#000000", "#ffffff"],
    ["#666666", "#cccccc"],
    ["#2288aa", "#aaccff"],
    ["#ff0000", "#ffcccc"],
    ["#006600", "#ccffcc"],
    ["#000066", "#ccccff"],
    ["#666600", "#ffffcc"],
    ["#660066", "#ffccff"],
    ["#006666", "#ccffff"],
    ["#000000", "#ffffff"],
    ["#666666", "#cccccc"],
    ["#2288aa", "#aaccff"],
]

def getIntersection(df1, df2):
    """
    Intersection of two dataframes.

    :param df1: First dataframe
    :param df2: Second dataframe
    :return: Intersection
    """
    return pd.merge(df1, df2, how='inner')
def getUnion(df1, df2):
    """
    Union of two dataframes.

    :param df1: First dataframe
    :param df2: Second dataframe
    :return: Union
    """
    return pd.concat([df1, df2])
def getDifference12(df1, df2):
    """
    Difference of two dataframes.

    :param df1: First dataframe
    :param df2: Second dataframe
    :return: Difference
    """
    return pd.concat([df1, df2, df2]).drop_duplicates(keep=False)
def completeSort(df):
    """
    Sort dataframe by all columns.

    :param df: Dataframe
    :return: Sorted dataframe
    """
    return df.sort_values(by=[df_union_all.columns[i] for i in range(0,len(df.columns))], ascending=True)
def list_intersection(lst1, lst2): 
    """
    Intersection of two lists.

    :param lst1: First list
    :param lst2: Second list
    :return: Intersection
    """
    lst3 = [value for value in lst1 if value in lst2] 
    return lst3 
def natural_sort(l): 
    convert = lambda text: int(text) if text.isdigit() else text.lower()
    alphanum_key = lambda key: [convert(c) for c in re.split('([0-9]+)', key)]
    return sorted(l, key=alphanum_key)



class inspector():
    """
    Class for inspecting done benchmarks
    """
    def __init__(self, result_path, anonymize=False):
        self.result_path = result_path
        self.anonymize = anonymize
        self.list_experiments = list(reversed(sorted([f for f in listdir(self.result_path) if isdir(join(self.result_path, f)) and f.isdigit()])))
        self.queries_successful = []
    def get_experiments_preview(self):
        workload_preview = {}
        for code in self.list_experiments:
            filename_query = self.result_path+'/'+code+'/queries.config'
            filename_connections = self.result_path+'/'+code+'/connections.config'
            filename_protocol = self.result_path+'/'+code+'/protocol.json'
            # skip incomplete result folders
            if isfile(filename_query) and isfile(filename_connections) and isfile(filename_protocol):
                try:
                    with open(filename_query,'r') as inp:
                        workload_properties = ast.literal_eval(inp.read())
                    with open(filename_connections,'r') as inp:
                        connection_properties = ast.literal_eval(inp.read())
                    workload_preview[code] = {}
                    workload_preview[code]['name'] = workload_properties['name']
                    workload_preview[code]['info'] = workload_properties.get('info', '')
                    workload_preview[code]['intro'] = workload_properties.get('intro', '')
                    l = [q for q in workload_properties['queries'] if ('active' in q and q['active'] == True) or 'active' not in q]
                    workload_preview[code]['queries'] = len(l)
                    l = [c for c in connection_properties if c['active'] == True]
                    workload_preview[code]['connections'] = len(l)
                    statbuf = stat(filename_protocol)
                    #print("Modification time: {}".format(statbuf.st_mtime))
                    modified = datetime.fromtimestamp(statbuf.st_mtime).isoformat(sep=' ', timespec='seconds')#, tz=timezone.utc)
                    workload_preview[code]['time'] = modified
                except Exception as e:
                    raise e
                finally:
                    pass
        return pd.DataFrame(workload_preview).T
    def load_experiment(self, code, anonymize=None, load=True, silent=True):
        if anonymize is not None:
            self.anonymize = anonymize
        # TODO: force clean dbms aliases
        self.queries_successful = []
        self.benchmarks = benchmarker.inspector(self.result_path, code, anonymize=self.anonymize, silent=silent)
        self.benchmarks.computeTimerRun()
        self.benchmarks.computeTimerSession()
        self.e = evaluator.evaluator(self.benchmarks, load=load, force=True, silent=silent)
        self.workload = copy.deepcopy(self.e.evaluation['general'])
        # remove metrics
        if 'loadingmetrics' in self.workload:
            del(self.workload['loadingmetrics'])
        if 'streamingmetrics' in self.workload:
            del(self.workload['streamingmetrics'])
        if 'reporting' in self.workload:
            del(self.workload['reporting'])
    def get_experiment_list_queries(self):
        # list of successful queries
        return self.benchmarks.listQueries()
    def get_experiment_list_connections(self):
        # list of connections
        #print(sorted(self.benchmarks.listConnections()))
        return sorted(self.benchmarks.listConnections())
    def get_experiment_list_dbms(self):
        # list all different dbms
        return list(self.get_experiment_list_connections_by_dbms().keys())
    def get_experiment_list_connections_by_dbms(self):
        # dict of lists of dbms
        dbms_list = {}
        for c,d in self.e.evaluation['dbms'].items():
            if self.benchmarks.anonymize and 'docker_alias' in d:
                dbms_name = d['docker_alias']
            else:
                dbms_name = d['docker']
            if not dbms_name in dbms_list:
                dbms_list[dbms_name] = []
            dbms_list[dbms_name].append(c)
        return dbms_list
    def get_experiment_list_connections_by_script(self):
        # dict of lists of scripts
        dbms_list = {}
        for c,d in self.e.evaluation['dbms'].items():
            dbms_name = d['script'] if 'script' in d else ''
            if not dbms_name in dbms_list:
                dbms_list[dbms_name] = []
            dbms_list[dbms_name].append(c)
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
            dbms_list[d['connectionmanagement'][property]].append(str(c))
        if len(dbms_list) == 0:
            dbms_list = {'': self.get_experiment_list_connections()}
        return dbms_list
    def get_experiment_list_connections_by_parameter(self, property):
        # dict of lists of node
        dbms_list = {}
        for c,d in self.e.evaluation['dbms'].items():
            if 'parameter' in d and property in d['parameter']:
                if not d['parameter'][property] in dbms_list:
                    dbms_list[d['parameter'][property]] = []
                dbms_list[d['parameter'][property]].append(str(c))
        if len(dbms_list) == 0:
            dbms_list = {'': self.get_experiment_list_connections()}
        return dbms_list
    def get_experiment_list_connections_by_hostsystem(self, property):
        # dict of lists of node
        dbms_list = {}
        for c,d in self.e.evaluation['dbms'].items():
            if 'hostsystem' in d and property in d['hostsystem']:
                if not d['hostsystem'][property] in dbms_list:
                    dbms_list[d['hostsystem'][property]] = []
                dbms_list[d['hostsystem'][property]].append(str(c))
        if len(dbms_list) == 0:
            dbms_list = {'': self.get_experiment_list_connections()}
        return dbms_list
    def get_experiment_list_connections_by_hostsystem_resource_requests(self, property):
        # dict of lists of node
        dbms_list = {}
        for c,d in self.e.evaluation['dbms'].items():
            if 'hostsystem' in d and 'resources' in d['hostsystem'] and 'requests' in d['hostsystem']['resources'] and property in d['hostsystem']['resources']['requests']:
                if not d['hostsystem']['resources']['requests'][property] in dbms_list:
                    dbms_list[d['hostsystem']['resources']['requests'][property]] = []
                dbms_list[d['hostsystem']['resources']['requests'][property]].append(str(c))
        if len(dbms_list) == 0:
            dbms_list = {'': self.get_experiment_list_connections()}
        return dbms_list
    def get_experiment_list_connections_by_hostsystem_resource_limits(self, property):
        # dict of lists of node
        dbms_list = {}
        for c,d in self.e.evaluation['dbms'].items():
            if 'hostsystem' in d and 'resources' in d['hostsystem'] and 'limits' in d['hostsystem']['resources'] and property in d['hostsystem']['resources']['limits']:
                if not d['hostsystem']['resources']['limits'][property] in dbms_list:
                    dbms_list[d['hostsystem']['resources']['limits'][property]] = []
                dbms_list[d['hostsystem']['resources']['limits'][property]].append(str(c))
        if len(dbms_list) == 0:
            dbms_list = {'': self.get_experiment_list_connections()}
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
            if connection in self.e.evaluation['dbms']:
                return self.e.evaluation['dbms'][connection]
            else:
                return dict()
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
        return self.workload
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
            df = self.dfCleanMonitoring(df) # remove extension interval
            #df = df_cleaned
            #df_cleaned = self.dfCleanMonitoring(df.copy())
            #df_stat = evaluator.addStatistics(df_cleaned, drop_nan=False, drop_measures=True)
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
        if df_stat.empty:
            #print("No data")
            return df, df_stat
        if len(factor_base) > 0:
            df_stat = evaluator.addFactor(df_stat, factor_base)
        return df, df_stat
    def get_measures_and_statistics_merged(self, numQuery, type='timer', name='run', dbms_filter=[], warmup=0, cooldown=0, factor_base='Mean'):
        dbms_list = {}
        for connection_nr, connection in self.benchmarks.dbms.items():
            c = connection.connectiondata
            if 'orig_name' in c:
                orig_name = c['orig_name']
            else:
                orig_name = c['name']
            if not orig_name in dbms_list:
                dbms_list[orig_name] = [c['name']]
            else:
                dbms_list[orig_name].append(c['name'])
        # get measures per stream
        df1, df2 = self.get_measures_and_statistics(numQuery, type=type, name=name)
        # mapping from derived name to orig_name
        reverse_mapping = {sub_db: main_db for main_db, sub_dbs in dbms_list.items() for sub_db in sub_dbs}
        df1['DBMS'] = df1.index.map(reverse_mapping)
        # concat values into single column per DBMS
        df_melted = pd.melt(df1, id_vars=['DBMS'], value_vars=df1.columns).sort_values('DBMS')
        df_melted = pd.DataFrame(df_melted[['DBMS', 'value']])
        #print(df_melted)
        #df_results = df_melted.set_index('DBMS')
        #df_results.columns = ['0']
        # compute stats per DBMS, i.e. per orig_name
        df_results = pd.DataFrame()
        df_results_stats = pd.DataFrame()
        df_groups = df_melted.groupby('DBMS')
        group_list = list(df_groups.groups.keys())
        for group in df_groups:
            df_dbms = pd.DataFrame(group[1]['value']).T
            df_dbms.index = [group[0]]
            df_dbms = df_dbms.T.reset_index(drop=True).T
            #print(df_dbms)
            df_results = pd.concat([df_results, df_dbms], ignore_index=False)
            df_dbms = evaluator.addStatistics(df_dbms, drop_measures=True)
            df_results_stats = pd.concat([df_results_stats, df_dbms], ignore_index=False)
        df_results.index.name = 'DBMS'
        if df_results_stats.empty:
            #print("No data")
            return df_results, df_results_stats
        if len(factor_base) > 0:
            df_results_stats = evaluator.addFactor(df_results_stats, factor_base)
        return df_results, df_results_stats
    def get_aggregated_query_statistics(self, type='timer', name='run', dbms_filter=[], warmup=0, cooldown=0, factor_base='Mean', query_aggregate='Mean'):
        # only successful queries (and all dbms)
        #print(dbms_filter)
        l = self.get_experiment_queries_successful(dbms_filter=dbms_filter)
        epos = [i for i,t in enumerate(self.benchmarks.timers) if t.name=='run']
        queries = l[epos[0]]
        # all queries (but only successful dbms)
        #l = self.get_experiment_list_queries()# _successful()
        #queries = [x-1 for x in l]
        column = query_aggregate
        df_aggregated = None
        for q in queries:
            column_new = 'Q{}'.format(q+1)
            df_measures, df_statistics = self.get_measures_and_statistics(q+1, type, name, dbms_filter, warmup, cooldown, factor_base)
            #print(df_measures, df_statistics)
            if df_statistics.empty:
                return pd.DataFrame()
            if df_aggregated is None:
                df_aggregated = pd.DataFrame(df_statistics[column]).rename(columns = {column: column_new})
            else:
                df_column = pd.DataFrame(df_statistics[column]).rename(columns = {column: column_new})
                df_aggregated = tools.dataframehelper.merge(df_aggregated, df_column)
        return df_aggregated
    def get_aggregated_experiment_statistics(self, type='timer', name='run', dbms_filter=[], warmup=0, cooldown=0, factor_base='Mean', query_aggregate='Mean', total_aggregate='Mean'):
        if query_aggregate is not None:
            df = self.get_aggregated_query_statistics(type, name, dbms_filter, warmup, cooldown, factor_base, query_aggregate)
            if df is None:
                return pd.DataFrame()
            if df.empty:
                return df
            #print(df)
            df_stat = evaluator.addStatistics(df, drop_nan=False, drop_measures=True)
            return pd.DataFrame(df_stat[total_aggregate]).rename(columns = {total_aggregate: "total_"+type+"_"+name})
        else:
            df_measures = {}
            for numQuery in self.get_experiment_queries_successful()[0]:
                df1,df2=self.get_measures_and_statistics(numQuery+1, type=type, name=name, dbms_filter=dbms_filter, warmup=warmup, cooldown=cooldown, factor_base=factor_base)
                df_measures[numQuery+1] = (df1.copy())
            #print(df_measures)
            n = len(df1.columns)
            #print(n)
            df_result = pd.DataFrame()
            for i in range(0,n):
                df_tmp = pd.DataFrame()
                for q, df in df_measures.items():
                    #print(df[i])
                    df_tmp.insert(loc=len(df_tmp.columns), column=q, value=df[i])
                #print(df_tmp)
                df_tmp = evaluator.dfSubRows(df_tmp, dbms_filter)
                df_stat = evaluator.addStatistics(df_tmp, drop_nan=False, drop_measures=True)
                #print(df_stat[total_aggregate])
                df_result.insert(loc=len(df_result.columns), column=i, value=df_stat[total_aggregate])
            #print(df_result)
            #return df_result
            newframe = df_result.copy()
            return newframe
    def get_aggregated_by_connection(self, dataframe, list_connections=[], connection_aggregate='Mean'):
        """
        Calculate the connection aggregate
        
        :param dataframe: DataFrame 
        :param list_connections:
        :param connection_aggregate: 
        :return: 
        """
        df_stats = pd.DataFrame()
        if len(list_connections) > 0:
            for i, l2 in list_connections.items():
                df = pd.DataFrame()
                for c in l2:
                    df.insert(len(df.columns), column=c, value=dataframe.loc[c])
                df = evaluator.addStatistics(df)
                df_stats.insert(len(df_stats.columns), column=i, value=df[connection_aggregate])
        else:
            df_stats = evaluator.addStatistics(dataframe.T, drop_measures=True)
            df_stats = pd.DataFrame(df_stats[connection_aggregate])
        #return df_stats.T
        newframe = df_stats.copy().T
        return newframe
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
    def get_resultset_dict(self, numQuery):
        return self.benchmarks.readResultSetDict(str(numQuery))
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
        df_workflow = tools.dataframehelper.getWorkflow(self.benchmarks)
        if df_workflow is None:
            df_workflow = pd.DataFrame()
        return df_workflow
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
    def get_monitoring_metric(self, metric, component="loading"):
        """
        Returns list of names of metrics using during monitoring.

        :return: List of monitoring metrics
        """
        filename = '/query_{component}_metric_{metric}.csv'.format(component=component, metric=metric)
        if isfile(self.benchmarks.path+"/"+filename):
            df = pd.read_csv(self.benchmarks.path+"/"+filename).T
            #print(df)
            df = df.reindex(index=natural_sort(df.index))
            return df.T
        else:
            return pd.DataFrame()
    def get_loading_metrics(self, metric):
        return evaluator.dfLoadingMetric(self.e.evaluation, metric)
    def get_streaming_metrics(self, metric):
        return evaluator.dfStreamingMetric(self.e.evaluation, metric)
    def get_loader_metrics(self, metric):
        return evaluator.dfLoaderMetric(self.e.evaluation, metric)
    def get_benchmarker_metrics(self, metric):
        return evaluator.dfBenchmarkerMetric(self.e.evaluation, metric)
    def get_datagenerator_metrics(self, metric):
        return evaluator.dfDatageneratorMetric(self.e.evaluation, metric)
    def get_total_resultsize_normalized(self):
        return tools.dataframehelper.evaluateNormalizedResultsizeToDataFrame(self.e.evaluation).T
    def get_total_resultsize(self):
        return tools.dataframehelper.evaluateResultsizeToDataFrame(self.e.evaluation).T
    def get_total_errors(self, dbms_filter=[], query_filter=[]):
        df = tools.dataframehelper.evaluateErrorsToDataFrame(self.e.evaluation).T
        if df is None:
            return pd.DataFrame()
        if len(dbms_filter)>0:
            df = df[df.index.isin(dbms_filter)]
        # currently ignored: print(query_filter)
        return df
    def get_total_warnings(self, dbms_filter=[], query_filter=[]):
        df = tools.dataframehelper.evaluateWarningsToDataFrame(self.e.evaluation).T
        if df is None:
            return pd.DataFrame()
        if len(dbms_filter)>0:
            df = df[df.index.isin(dbms_filter)]
        return df
    def get_total_times(self, dbms_filter=[]):
        df, title = tools.dataframehelper.totalTimes(self.benchmarks, dbms_filter)
        if df is None:
            return pd.DataFrame()
        df = df.T
        if len(dbms_filter)>0:
            df = df[df.index.isin(dbms_filter)]
        return df
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
    def get_experiment_queries_successful(self, dbms_filter=[]):
        # list of active queries for timer e[0] = execution
        #if len(self.queries_successful) > 0:
        #    return self.queries_successful
        #else:
        self.queries_successful = tools.findSuccessfulQueriesAllDBMS(self.benchmarks, None, self.benchmarks.timers, dbms_filter)
        return self.queries_successful
    def get_survey_successful(self, timername=None, dbms_filter=[]):
        # list of active queries for timer e[0] = execution
        if not timername is None:
            #print(self.benchmarks.timers)
            #timers = [(i, t.name) for i,t in enumerate(self.benchmarks.timers)]
            #print(timers)
            epos = [i for i,t in enumerate(self.benchmarks.timers) if t.name==timername]
            #print(epos, self.get_experiment_queries_successful(dbms_filter=dbms_filter))
            l = self.get_experiment_queries_successful(dbms_filter=dbms_filter)[epos[0]]
            return l
        else:
            return self.get_experiment_queries_successful(dbms_filter=dbms_filter)
    def get_hardware_metrics(self, numQuery, metric, warmup=0, cooldown=0):
        return evaluator.dfMonitoringQ(numQuery, metric, warmup, cooldown)
        #hw = monitor.metrics(self.benchmarks)
        #df = hw.dfHardwareMetrics(numQuery, metric)
        #numRunBegin = warmup
        #numRunEnd = len(df.columns)-cooldown
        #df = df.T[numRunBegin:numRunEnd].T
        #return df
    def dfCleanMonitoring(self, dataframe):
        # remove extend for statistics
        for c, connection in self.benchmarks.dbms.items():
            if 'monitoring' in connection.connectiondata and 'extend' in connection.connectiondata['monitoring']:
                add_interval = int(connection.connectiondata['monitoring']['extend'])
            else:
                add_interval = 0
            #print(c)
            #print(add_interval)
            if c in dataframe.index:
                s = dataframe.loc[c]
                x = s.last_valid_index()
                #print(x)
                s[x-add_interval+1:x+1]=nan
                s[0:add_interval-1]=nan
        #print(dataframe)
        dataframe = dataframe.loc[:,add_interval:len(dataframe.columns)-add_interval-1]
        #df_all.columns = df_all.columns.map(mapper=(lambda i: i-add_interval))
        #dataframe = dataframe.T.reset_index().T
        #print(dataframe)
        return dataframe
    def get_querystring(self, numQuery, connectionname, numRun):
        return self.benchmarks.getQueryString(numQuery, connectionname, numRun)
    def measures_reset_zero(self, dataframe):
        return dataframe.sub(dataframe.min(axis=1), axis=0)
    def get_number_of_measures(self, dataframe):
        return dataframe.isnull().sum(axis=1)+len(dataframe.columns)
    def measures_rolling_difference(self, dataframe, periods=1):
        dataframe = dataframe.diff(periods=periods, axis=1)/periods
        for i in range(0, periods):
            dataframe.T.iloc[i] = 0.0
        return dataframe
    def measures_smoothing(self, dataframe, window=1, number=1):
        if window > 0:
            for i in range(0, number):
                dataframe = dataframe.rolling(window=window, axis=1).mean()
        return dataframe
    def plot_measures(self, dataframe, connection_colors):
        print(evaluator.addStatistics(dataframe)['Mean'])            # little affected by window
        print(evaluator.addStatistics(dataframe)['Median'])          # much affected by window
        print(evaluator.addStatistics(dataframe,drop_measures=True)) # variation and max drops
        df_nums = df_get_number_of_measures(dataframe)
        # total CPU in unit CPU-seconds
        print(dataframe.mean(axis=1)*df_nums)
        fig = go.Figure()
        for i in range(len(dataframe.index)):
            t = fig.add_trace(go.Scatter(x=dataframe.T.index, y=dataframe.iloc[i], name=dataframe.index[i], line=dict(color=connection_colors[dataframe.index[i]], width=1)))
        n = fig.update_layout(yaxis=dict(range=[0,dataframe.max().max()]))
        fig.show()

