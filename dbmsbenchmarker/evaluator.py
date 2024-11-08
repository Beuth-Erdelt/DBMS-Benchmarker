"""
    Classes for generating evaluation cubes for benchmarks of the Python Package DBMS Benchmarker
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
from tabulate import tabulate
import logging
import pandas as pd
import matplotlib
import matplotlib.pyplot as plt
import os
import csv
import json
from dbmsbenchmarker import tools, monitor, reporter
import datetime
from tqdm import tqdm
import pickle
import sys
import math
import pprint
import ast
from scipy import stats
import numpy as np

class evaluator():
    evaluation = {}
    """
    Class for generating evaluation cube.
    """
    def __init__(self, benchmarker, load=False, force=False, silent=False):
        """
        Construct a new 'evaluator' object.

        :param benchmarker: Object of benchmarker containing information about queries, connections and benchmark times
        :param silent: No output of status
        :return: returns nothing
        """
        self.benchmarker = benchmarker
        if force:
            evaluator.evaluation = {}
        if len(evaluator.evaluation) == 0:
            if load:
                self.load(silent)
            else:
                evaluator.evaluation = self.generate()
                # force to use stored format
                self.load(silent)
    def get_evaluation(self):
        return evaluator.evaluation
    def generate(self):
        """
        Prepares a dict containing evaluation data about experiment, dbms query and timer.
        Anonymizes dbms if activated.

        :param numQuery: Number of query to collect data from. numQuery=0 for title page
        :param numTimer: Number of timer of this query. First timer is treated differently (subsubsection)
        :param timer: List of timers to collect data from. timer=None for title page
        :return: returns dict of data about query and timer
        """
        #result = self.benchmarker.queryconfig.copy()
        print("Generate Evaluation")
        self.benchmarker.computeTimerRun()
        self.benchmarker.computeTimerSession()
        evaluation = {}
        # general report information
        evaluation['general'] = {}
        evaluation['general']['now'] = str(datetime.datetime.now())
        evaluation['general']['path'] = self.benchmarker.path
        evaluation['general']['code'] = self.benchmarker.code
        # format title of benchmark
        if len(self.benchmarker.queryconfig["name"]) > 0:
            benchmarkName = self.benchmarker.queryconfig["name"]
        else:
            benchmarkName = self.benchmarker.path
        evaluation['general']['name'] = benchmarkName
        evaluation['general']['intro'] = ''
        evaluation['general']['info'] = ''
        if "intro" in self.benchmarker.queryconfig and len(self.benchmarker.queryconfig["intro"]) > 0:
            evaluation['general']['intro'] = self.benchmarker.queryconfig["intro"]
        if "info" in self.benchmarker.queryconfig and len(self.benchmarker.queryconfig["info"]) > 0:
            evaluation['general']['info'] = self.benchmarker.queryconfig["info"]
        if "defaultParameters" in self.benchmarker.queryconfig:
            evaluation['general']['defaultParameters'] = self.benchmarker.queryconfig["defaultParameters"]
        # general connectionmanagement
        evaluation['general']['connectionmanagement'] = {}
        if self.benchmarker.connectionmanagement['timeout'] is None:
            evaluation['general']['connectionmanagement']['timeout'] = "Unlimited"
        else:
            evaluation['general']['connectionmanagement']['timeout'] = str(self.benchmarker.connectionmanagement['timeout'])+"s"
        evaluation['general']['connectionmanagement']['numProcesses'] = self.benchmarker.connectionmanagement['numProcesses']
        if self.benchmarker.connectionmanagement['runsPerConnection'] is None:
            evaluation['general']['connectionmanagement']['runsPerConnection'] = "Unlimited"
        else:
            evaluation['general']['connectionmanagement']['runsPerConnection']=self.benchmarker.connectionmanagement['runsPerConnection']
        def findTimesOfSuccessfulQueries():
            # find position of execution timer
            e = [i for i,t in enumerate(self.benchmarker.timers) if t.name=="execution"]
            # list of active queries for timer e[0] = execution
            qs = tools.findSuccessfulQueriesAllDBMS(self.benchmarker, None, self.benchmarker.timers)[e[0]]
            #print(qs)
            # compute total times
            times = {}
            l = list({c for c in {c for q,d in self.benchmarker.protocol['query'].items() if int(q)-1 in qs for c in d['durations'].items()}})
            #print(l)
            for i,element in enumerate(l):
                #print(element)
                if not element[0] in times:
                    times[element[0]] = element[1]
                else:
                    times[element[0]] += element[1]
            return times
        times = findTimesOfSuccessfulQueries()
        # format dbms infos
        def initfilename(c, i):
            return self.benchmarker.path+'/'+c+'_init_'+str(i)+'.log'
        def hasInitScript(c):
            return os.path.isfile(initfilename(c,0))
        evaluation['dbms'] = {}
        for c in sorted(self.benchmarker.dbms, key=lambda kv: self.benchmarker.dbms[kv].name):
            if self.benchmarker.dbms[c].connectiondata['active']:
                evaluation['dbms'][c] = {}
                evaluation['dbms'][c]['name'] = self.benchmarker.dbms[c].getName()
                if 'script' in self.benchmarker.dbms[c].connectiondata:
                    evaluation['dbms'][c]['script'] = self.benchmarker.dbms[c].connectiondata["script"]
                else:
                    evaluation['dbms'][c]['script'] = ""
                if 'docker' in self.benchmarker.dbms[c].connectiondata:
                    evaluation['dbms'][c]['docker'] = self.benchmarker.dbms[c].connectiondata["docker"]
                else:
                    evaluation['dbms'][c]['docker'] = ""
                if 'docker_alias' in self.benchmarker.dbms[c].connectiondata:
                    evaluation['dbms'][c]['docker_alias'] = self.benchmarker.dbms[c].connectiondata["docker_alias"]
                else:
                    evaluation['dbms'][c]['docker_alias'] = evaluation['dbms'][c]['docker']
                if 'alias' in self.benchmarker.dbms[c].connectiondata:
                    evaluation['dbms'][c]['alias'] = self.benchmarker.dbms[c].connectiondata["alias"]
                else:
                    evaluation['dbms'][c]['alias'] = evaluation['dbms'][c]['name']
                evaluation['dbms'][c]['version'] = self.benchmarker.dbms[c].connectiondata["version"]
                evaluation['dbms'][c]['info'] = self.benchmarker.dbms[c].connectiondata["info"]
                if 'parameter' in self.benchmarker.dbms[c].connectiondata:
                    evaluation['dbms'][c]['parameter'] = self.benchmarker.dbms[c].connectiondata['parameter']
                else:
                    evaluation['dbms'][c]['parameter'] = {}
                if 'connectionmanagement' in self.benchmarker.dbms[c].connectiondata:
                    # settings of connection
                    connectionmanagement = self.benchmarker.dbms[c].connectiondata['connectionmanagement']
                else:
                    # global setting
                    connectionmanagement = self.benchmarker.connectionmanagement
                evaluation['dbms'][c]['connectionmanagement'] = {}
                if "numProcesses" in connectionmanagement:
                    evaluation['dbms'][c]['connectionmanagement']["numProcesses"] = connectionmanagement["numProcesses"]
                if "runsPerConnection" in connectionmanagement:
                    if connectionmanagement["runsPerConnection"] is not None and connectionmanagement["runsPerConnection"] > 0:
                        evaluation['dbms'][c]['connectionmanagement']["runsPerConnection"] = connectionmanagement["runsPerConnection"]
                    else:
                        evaluation['dbms'][c]['connectionmanagement']["runsPerConnection"] = "Unlimited"
                if "timeout" in connectionmanagement:
                    if connectionmanagement["timeout"] is not None and connectionmanagement["timeout"] > 0:
                        evaluation['dbms'][c]['connectionmanagement']["timeout"] = connectionmanagement["timeout"]
                    else:
                        evaluation['dbms'][c]['connectionmanagement']["timeout"] = "Unlimited"
                if "hostsystem" in self.benchmarker.dbms[c].connectiondata:
                    evaluation['dbms'][c]['hostsystem'] = self.benchmarker.dbms[c].connectiondata["hostsystem"].copy()
                if "worker" in self.benchmarker.dbms[c].connectiondata:
                    evaluation['dbms'][c]['worker'] = self.benchmarker.dbms[c].connectiondata["worker"].copy()
                evaluation['dbms'][c]['times'] = {}
                evaluation['dbms'][c]['times']['total'] = self.benchmarker.protocol['total']#['time_start']#self.benchmarker.time_start
                #evaluation['dbms'][c]['times']['time_end'] = self.benchmarker.protocol['total']['time_end']#self.benchmarker.time_end
                evaluation['dbms'][c]['prices'] = {}
                # copy timing information from connection infos (create schema, ingest data, create index, ...)
                if "timeLoad" in self.benchmarker.dbms[c].connectiondata:
                    evaluation['dbms'][c]['times']['load_ms'] = self.benchmarker.dbms[c].connectiondata["timeLoad"]*1000.0
                else:
                    evaluation['dbms'][c]['times']['load_ms'] = 0
                if "timeIngesting" in self.benchmarker.dbms[c].connectiondata:
                    evaluation['dbms'][c]['times']['ingest_ms'] = self.benchmarker.dbms[c].connectiondata["timeIngesting"]*1000.0
                else:
                    evaluation['dbms'][c]['times']['ingest_ms'] = 0
                if "timeGenerate" in self.benchmarker.dbms[c].connectiondata:
                    evaluation['dbms'][c]['times']['generate_ms'] = self.benchmarker.dbms[c].connectiondata["timeGenerate"]*1000.0
                else:
                    evaluation['dbms'][c]['times']['generate_ms'] = 0
                if "timeSchema" in self.benchmarker.dbms[c].connectiondata:
                    evaluation['dbms'][c]['times']['schema_ms'] = self.benchmarker.dbms[c].connectiondata["timeSchema"]*1000.0
                else:
                    evaluation['dbms'][c]['times']['schema_ms'] = 0
                if "timeIndex" in self.benchmarker.dbms[c].connectiondata:
                    evaluation['dbms'][c]['times']['index_ms'] = self.benchmarker.dbms[c].connectiondata["timeIndex"]*1000.0
                else:
                    evaluation['dbms'][c]['times']['index_ms'] = 0
                if "script_times" in self.benchmarker.dbms[c].connectiondata:
                    evaluation['dbms'][c]['times']['script_times'] = self.benchmarker.dbms[c].connectiondata["script_times"]#*1000.0
                else:
                    evaluation['dbms'][c]['times']['script_times'] = []
                if c in times:
                    evaluation['dbms'][c]['times']['benchmark_ms'] = times[c]
                    if 'priceperhourdollar' in self.benchmarker.dbms[c].connectiondata:
                        if "timeLoad" in self.benchmarker.dbms[c].connectiondata:
                            time = times[c] + self.benchmarker.dbms[c].connectiondata["timeLoad"]*1000.0
                        else:
                            time = times[c]
                        evaluation['dbms'][c]['prices']['perHour_usd'] = self.benchmarker.dbms[c].connectiondata['priceperhourdollar']
                        evaluation['dbms'][c]['prices']['benchmark_usd'] = self.benchmarker.dbms[c].connectiondata['priceperhourdollar']*time/3600000
                if self.benchmarker.dbms[c].hasHardwareMetrics():
                    evaluation['dbms'][c]['hardwaremetrics'] = {}
                    evaluation['general']['loadingmetrics'] = {}
                    evaluation['general']['streamingmetrics'] = {}
                    evaluation['general']['loadermetrics'] = {}
                    evaluation['general']['benchmarkermetrics'] = {}
                    evaluation['general']['datageneratormetrics'] = {}
                    metricsReporter = monitor.metrics(self.benchmarker)
                    hardwareAverages = metricsReporter.computeAverages()
                    if c in hardwareAverages:
                        for m, avg in hardwareAverages[c].items():
                            evaluation['dbms'][c]['hardwaremetrics'][m] = avg
                        if 'total_gpu_power' in hardwareAverages[c]:
                            # basis: per second average power, total time in ms
                            evaluation['dbms'][c]['hardwaremetrics']['total_gpu_energy'] = hardwareAverages[c]['total_gpu_power']*times[c]/3600000
                        for m, avg in hardwareAverages[c].items():
                            # load test metrics
                            df = metricsReporter.dfHardwareMetricsLoading(m)
                            df.drop_duplicates(inplace=True) # TODO: Why are there duplicates sometimes?
                            evaluation['general']['loadingmetrics'][m] = df.to_dict(orient='index')
                            # streaming metrics
                            df = metricsReporter.dfHardwareMetricsStreaming(m)
                            df.drop_duplicates(inplace=True) # TODO: Why are there duplicates sometimes?
                            evaluation['general']['streamingmetrics'][m] = df.to_dict(orient='index')
                            # loader metrics
                            df = metricsReporter.dfHardwareMetricsLoader(m)
                            df.drop_duplicates(inplace=True) # TODO: Why are there duplicates sometimes?
                            evaluation['general']['loadermetrics'][m] = df.to_dict(orient='index')
                            # benchmarker metrics
                            df = metricsReporter.dfHardwareMetricsBenchmarker(m)
                            df.drop_duplicates(inplace=True) # TODO: Why are there duplicates sometimes?
                            evaluation['general']['benchmarkermetrics'][m] = df.to_dict(orient='index')
                            #  datagenerator metrics
                            df = metricsReporter.dfHardwareMetricsDatagenerator(m)
                            df.drop_duplicates(inplace=True) # TODO: Why are there duplicates sometimes?
                            evaluation['general']['datageneratormetrics'][m] = df.to_dict(orient='index')
        # appendix start: query survey
        evaluation['query'] = {}
        for i in range(1, len(self.benchmarker.queries)+1):
            evaluation['query'][i] = {}
            evaluation['query'][i]['config'] = self.benchmarker.queries[i-1]
            queryObject = tools.query(self.benchmarker.queries[i-1])
            evaluation['query'][i]['title'] = queryObject.title
            evaluation['query'][i]['active'] = queryObject.active
            evaluation['query'][i]['dbms'] = {}
            if not queryObject.active:
                continue
            if len(self.benchmarker.protocol['query'][str(i)]['parameter']) > 0:
                evaluation['query'][i]['is_parametrized'] = True
            else:
                evaluation['query'][i]['is_parametrized'] = False
            l = self.benchmarker.protocol['query'][str(i)]['dataStorage']
            if len(l) > 0 and len(l[0]) > 0 and len(l[0][0]) > 0:
                l = [x for l1 in l for l2 in l1 for x in l2]
                evaluation['query'][i]['storage_size_byte'] = sys.getsizeof(l)
                evaluation['query'][i]['storage_type'] = queryObject.result
            for connection, dbms in self.benchmarker.dbms.items():
                evaluation['query'][i]['dbms'][connection] = {}
            if 'errors' in self.benchmarker.protocol['query'][str(i)]:
                for connection, error in self.benchmarker.protocol['query'][str(i)]['errors'].items():
                    if len(error) > 0 and self.benchmarker.dbms[connection].connectiondata['active']:
                        evaluation['query'][i]['dbms'][connection]['error'] = error
            #print(self.benchmarker.dbms)
            if 'warnings' in self.benchmarker.protocol['query'][str(i)]:
                for connection, warning in self.benchmarker.protocol['query'][str(i)]['warnings'].items():
                    if len(warning) > 0 and self.benchmarker.dbms[connection].connectiondata['active']:
                        evaluation['query'][i]['dbms'][connection]['warning'] = warning
            if 'sizes' in self.benchmarker.protocol['query'][str(i)]:
                for connection, size in self.benchmarker.protocol['query'][str(i)]['sizes'].items():
                    if size > 0 and self.benchmarker.dbms[connection].connectiondata['active']:
                        evaluation['query'][i]['dbms'][connection]['received_size_byte'] = size
            # are there benchmarks for this query?
            numQuery = i
            if self.benchmarker.timerExecution.checkForSuccessfulBenchmarks(numQuery):
                #evaluation['queryNumber']=numQuery
                query = tools.query(self.benchmarker.queries[numQuery-1])
                # format duration
                evaluation['query'][numQuery]['duration'] = self.benchmarker.protocol['query'][str(numQuery)]['duration']
                for c, d in self.benchmarker.protocol['query'][str(numQuery)]['durations'].items():
                    evaluation['query'][i]['dbms'][c]['duration'] = d
                if "reporting" in self.benchmarker.queryconfig:
                    evaluation['general']['reporting'] = self.benchmarker.queryconfig["reporting"]
                for c, dbms in self.benchmarker.dbms.items():
                    if not self.benchmarker.dbms[c].connectiondata['active']:
                        continue
                    # query settings (connection manager)
                    cm = self.benchmarker.getConnectionManager(numQuery, c)
                    evaluation['query'][i]['connectionmanagement'] = cm
                    evaluation['query'][i]['dbms'][c]['connectionmanagement'] = cm
                    if c in self.benchmarker.protocol['query'][str(numQuery)]['durations']:
                        # latency / throughput
                        evaluation['query'][i]['dbms'][c]['metrics'] = {}
                        totaltime_s = self.benchmarker.protocol['query'][str(numQuery)]['durations'][c]/1000.0
                        evaluation['query'][i]['dbms'][c]['metrics']['totaltime_ms'] = self.benchmarker.protocol['query'][str(numQuery)]['durations'][c]
                        if totaltime_s > 0:
                            tps = query.numRun/totaltime_s
                            evaluation['query'][i]['dbms'][c]['metrics']['throughput_run_total_ps'] = tps
                            evaluation['query'][i]['dbms'][c]['metrics']['throughput_run_total_ph'] = tps*3600.0
                            tps = query.numRun/cm['runsPerConnection']/totaltime_s
                            evaluation['query'][i]['dbms'][c]['metrics']['throughput_session_total_ps'] = tps
                            evaluation['query'][i]['dbms'][c]['metrics']['throughput_session_total_ph'] = tps*3600.0
                        if c in self.benchmarker.timerRun.stats[numQuery-1]:
                            meantime_run_s = self.benchmarker.timerRun.stats[numQuery-1][c][1]/1000.0
                            #print(self.benchmarker.timerRun.stats[numQuery-1][c][1])
                            if meantime_run_s > 0:
                                tps = cm['numProcesses']/meantime_run_s
                                evaluation['query'][i]['dbms'][c]['metrics']['throughput_run_mean_ps'] = tps
                                evaluation['query'][i]['dbms'][c]['metrics']['latency_run_mean_ms'] = meantime_run_s*1000.0
                                evaluation['query'][i]['dbms'][c]['metrics']['throughput_run_mean_ph'] = tps*3600.0
                        else:
                            print(c+" missing in timerRun statistics for query Q"+str(numQuery))
                        if c in self.benchmarker.timerSession.stats[numQuery-1]:
                            meantime_session_s = self.benchmarker.timerSession.stats[numQuery-1][c][1]/1000.0
                            if meantime_session_s > 0:
                                tps = cm['numProcesses']/meantime_session_s
                                evaluation['query'][i]['dbms'][c]['metrics']['throughput_session_mean_ps'] = tps
                                evaluation['query'][i]['dbms'][c]['metrics']['latency_session_mean_ms'] = meantime_session_s*1000.0
                                evaluation['query'][i]['dbms'][c]['metrics']['throughput_session_mean_ph'] = tps*3600.0
                        else:
                            print(c+" missing in timerSession statistics for query Q"+str(numQuery))
                        if 'throughput_run_total_ps' in evaluation['query'][i]['dbms'][c]['metrics'] and 'latency_run_mean_ms' in evaluation['query'][i]['dbms'][c]['metrics']:
                            evaluation['query'][i]['dbms'][c]['metrics']['queuesize_run'] = evaluation['query'][i]['dbms'][c]['metrics']['throughput_run_total_ps'] * evaluation['query'][i]['dbms'][c]['metrics']['latency_run_mean_ms'] / 1000.0
                            evaluation['query'][i]['dbms'][c]['metrics']['queuesize_run_percent'] = evaluation['query'][i]['dbms'][c]['metrics']['queuesize_run'] / cm['numProcesses'] * 100.0
                        if 'throughput_session_total_ps' in evaluation['query'][i]['dbms'][c]['metrics'] and 'latency_session_mean_ms' in evaluation['query'][i]['dbms'][c]['metrics']:
                            evaluation['query'][i]['dbms'][c]['metrics']['queuesize_session'] = evaluation['query'][i]['dbms'][c]['metrics']['throughput_session_total_ps'] * evaluation['query'][i]['dbms'][c]['metrics']['latency_session_mean_ms'] / 1000.0
                            evaluation['query'][i]['dbms'][c]['metrics']['queuesize_session_percent'] = evaluation['query'][i]['dbms'][c]['metrics']['queuesize_session'] / cm['numProcesses'] * 100.0
                evaluation['query'][i]['start'] = self.benchmarker.protocol['query'][str(numQuery)]['start']
                evaluation['query'][i]['end'] = self.benchmarker.protocol['query'][str(numQuery)]['end']
                evaluation['query'][i]['benchmarks'] = {}
                # report per timer
                for numTimer, timer in enumerate(self.benchmarker.timers):
                    evaluation['query'][i]['benchmarks'][timer.name] = {}
                    evaluation['query'][i]['benchmarks'][timer.name]['benchmarks'] = self.benchmarker.benchmarksToDataFrame(i, timer).to_dict(orient='index')
                    evaluation['query'][i]['benchmarks'][timer.name]['statistics'] = self.benchmarker.statsToDataFrame(i, timer).to_dict(orient='index')
                # compute hardware metrics per query
                evaluation['query'][i]['hardwaremetrics'] = {}
                metricsReporter = monitor.metrics(self.benchmarker)
                for m, metric in monitor.metrics.metrics.items():
                    df = metricsReporter.dfHardwareMetrics(i, m)
                    evaluation['query'][i]['hardwaremetrics'][m] = df.to_dict(orient='index')
        # dbms metrics
        # find position of execution timer
        epos = [i for i,t in enumerate(self.benchmarker.timers) if t.name=="execution"]
        # list of active queries for timer e[0] = execution
        qs = tools.findSuccessfulQueriesAllDBMS(self.benchmarker, None, self.benchmarker.timers)[epos[0]]
        tps = {}
        num = {}
        for i, q in evaluation['query'].items():
            if not i-1 in qs:
                continue
            if 'dbms' in q:
                for c, d in q['dbms'].items():
                    if not c in tps:
                        tps[c] = {}
                        num[c] = {}
                    if 'metrics' in d:
                        for m, metric in d['metrics'].items():
                            #print(m, metric)
                            if not m in tps[c]:
                                tps[c][m] = 1.0
                                num[c][m] = 0
                            if '_ms' in m:
                                tps[c][m] += math.log(metric/1000.) # ms too big numbers
                                #print(metric/1000.)
                            elif '_ph' in m:
                                tps[c][m] += math.log(metric*3600.) # ph too big numbers
                                #print(metric*3600.)
                            elif '_ps' in m:
                                tps[c][m] += math.log(metric/3600.) # ps too big numbers
                                #print(metric/3600.)
                            else:
                                tps[c][m] *= metric
                            num[c][m] += 1
        #print(tps)
        #print(num)
        for c, t in tps.items():
            if not c in evaluation['dbms']:
                continue
            evaluation['dbms'][c]['metrics'] = {}
            for m, v in t.items():
                if '_ps' in m:
                    #tps[c][m] = math.pow(tps[c][m], 1.0 / num[c][m])
                    #evaluation['dbms'][c]['metrics'][m] = tps[c][m]
                    #evaluation['dbms'][c]['metrics'][m.replace('_ps', '_ph')] = tps[c][m]*3600.0
                    evaluation['dbms'][c]['metrics'][m.replace('_ps', '_ph')] = math.exp(tps[c][m]/num[c][m])
                    evaluation['dbms'][c]['metrics'][m] = math.exp(tps[c][m]/num[c][m])/3600.
                elif '_ph' in m:
                    #tps[c][m] = math.pow(tps[c][m], 1.0 / num[c][m])
                    evaluation['dbms'][c]['metrics'][m.replace('_ph', '_ps')] = math.exp(tps[c][m]/num[c][m])
                    evaluation['dbms'][c]['metrics'][m] = math.exp(tps[c][m]/num[c][m])*3600.
                elif '_ms' in m:
                    #print(tps[c][m])
                    evaluation['dbms'][c]['metrics'][m.replace('_ms', '_s')] = math.exp(tps[c][m]/num[c][m])
                    evaluation['dbms'][c]['metrics'][m] = math.exp(tps[c][m]/num[c][m])*1000.
                else:
                    tps[c][m] = math.pow(tps[c][m], 1.0 / num[c][m])
                    evaluation['dbms'][c]['metrics'][m] = tps[c][m]
            #print(evaluation['dbms'][c]['metrics'])
        evaluation['general']['results'] = {}
        #del evaluation['dbms'][c]['metrics']
        #print(evaluation)
        def find_non_serializable(obj, path=""):
            import json
            non_serializable_items = []

            # Try to serialize the object; if it fails, dive deeper
            try:
                json.dumps(obj)
                return []  # If it’s serializable, no issue
            except TypeError:
                # If it’s a dictionary, check each item
                if isinstance(obj, dict):
                    for k, v in obj.items():
                        new_path = f"{path}.{k}" if path else str(k)
                        non_serializable_items.extend(find_non_serializable(v, path=new_path))
                # If it’s a list or tuple, check each element
                elif isinstance(obj, (list, tuple)):
                    for i, item in enumerate(obj):
                        new_path = f"{path}[{i}]"
                        non_serializable_items.extend(find_non_serializable(item, path=new_path))
                # If it’s a non-serializable type, record the path and type
                else:
                    non_serializable_items.append((path, type(obj).__name__))
            return non_serializable_items
        list_nonserializable = find_non_serializable(evaluation)
        if len(list_nonserializable) > 0:
            print("ERROR: Non serializable evaluation")
            print(list_nonserializable)
        #print(evaluation['query'][50]['config']['dbms'])
        #if 'dbms' in evaluation['query'][51]['config']:
        #    print(evaluation['query'][51]['config']['dbms'])
        #    del evaluation['query'][51]['config']['dbms']
        # Convert Python to JSON  
        #json_object = json.dumps(evaluation, indent = 4) 
        # Print JSON object
        #print(json_object) 
        # total diagrams
        """
        reporterBar = reporter.barer(self.benchmarker)
        dfTotalSum = reporterBar.generate(numQuery=None, timer=self.benchmarker.timers, ensembler='sum')
        dfTotalProd = reporterBar.generate(numQuery=None, timer=self.benchmarker.timers, ensembler='product')
        # generate area plot of total time
        reporterArea = reporter.arear(self.benchmarker)
        dfTotalTime = reporterArea.generate(numQuery=None, timer=self.benchmarker.timers)
        # generate barh plot of total ranking
        dfTotalRank, timers = self.benchmarker.generateSortedTotalRanking()
        if dfTotalSum is not None:
            evaluation['general']['results']['totalSum'] = dfTotalSum.to_dict(orient='index')
        if dfTotalProd is not None:
            evaluation['general']['results']['totalProd'] = dfTotalProd.to_dict(orient='index')
        if dfTotalTime is not None:
            evaluation['general']['results']['totalTime'] = dfTotalTime.to_dict()
        if dfTotalRank is not None:
            evaluation['general']['results']['totalRank'] = dfTotalRank.to_dict(orient='index')
        """
        filename = self.benchmarker.path+'/evaluation.dict'
        with open(filename, 'w') as f:
            f.write(str(evaluation))
            #pprint.pprint(evaluation, f)
        filename = self.benchmarker.path+'/evaluation.json'
        with open(filename, 'w') as f:
            json.dump(evaluation, f)
        return evaluation
    def load(self, silent=False):
        if not silent:
            print("Load Evaluation")
        filename = self.benchmarker.path+'/evaluation.json'
        with open(filename,'r') as f:
            evaluator.evaluation = json.load(f)
            #evaluator.evaluation = ast.literal_eval(inp.read())
        #filename = self.benchmarker.path+'/evaluation.dict'
        #with open(filename,'r') as f:
        #    #evaluator.evaluation = ast.literal_eval(inp.read())
    def pretty(self, d="", indent=0):
        if len(d) == 0:
            d = evaluator.evaluation
        pretty(d, indent)
    def structure(self, d="", indent=0):
        if len(d) == 0:
            d = evaluator.evaluation
        for key, value in d.items():
            if isinstance(value, dict):
                print('  ' * indent + str(key))
                self.structure(value, indent+1)
            #else:
            #    print('  ' * indent + str(key) + ":" + str(value))

def pretty(d, indent=0):
    for key, value in d.items():
        if isinstance(value, dict):
            print('  ' * indent + str(key))
            pretty(value, indent+1)
        else:
            print('  ' * indent + str(key) + ":" + str(value))

def dfMonitoringQ(query, metric, warmup=0, cooldown=0):
    #print("{}:{}".format(query, timer))
    if not 'hardwaremetrics' in evaluator.evaluation['query'][str(query)]:
        print('No hardware metrics for query {}'.format(str(query)))
        return pd.DataFrame()
    l={c: [x for i,x in b.items()] for c,b in evaluator.evaluation['query'][str(query)]['hardwaremetrics'][metric].items()}
    df = pd.DataFrame(l)
    numRunBegin = warmup
    numRunEnd = len(df.index)-cooldown
    df = df[numRunBegin:numRunEnd].T
    df.index.name = 'DBMS'
    #print(df)
    return df
def dfMeasuresQ(query, timer, warmup=0, cooldown=0):
    #print("{}:{}".format(query, timer))
    if 'benchmarks' in evaluator.evaluation['query'][str(query)]:
        l={c: [x for i,x in b.items()] for c,b in evaluator.evaluation['query'][str(query)]['benchmarks'][timer]['benchmarks'].items()}
        df = pd.DataFrame(l)
        numRunBegin = warmup
        numRunEnd = len(df.index)-cooldown
        df = df[numRunBegin:numRunEnd].T
        df.index.name = 'DBMS'
        #print(df)
        return df
    else:
        return pd.DataFrame()
def dfLatQ(query, warmup=0, cooldown=0):
    l={c: [x for i,x in b.items()] for c,b in evaluator.evaluation['query'][str(query)]['benchmarks']['run']['benchmarks'].items()}
    df = pd.DataFrame(l)
    numRunBegin = warmup
    numRunEnd = len(df.index)-cooldown
    df = df[numRunBegin:numRunEnd].T
    df.index.name = 'DBMS'
    #print(df)
    return df
def addStatistics(dataframe, drop_nan=True, drop_measures=False):
    df = dataframe.copy().T
    # treat 0 as missing value
    df = df.replace(0., np.nan)
    if drop_nan and df.isnull().any().any():
        #print("Missing!")
        with_nan = True
        df = df.dropna()
    if df.empty:
        return pd.DataFrame()
    num_measures = len(df.index)
    stat_mean = df.mean()
    stat_std = df.std(ddof=0)
    stat_q1 = df.quantile(0.25)
    stat_q2 = df.quantile(0.5)
    stat_q3 = df.quantile(0.75)
    stat_90 = df.quantile(0.90)
    stat_95 = df.quantile(0.95)
    stat_min = df.min()
    stat_max = df.max()
    stat_first = df.iloc[0]
    stat_last = df.iloc[len(df.dropna().index)-1]
    stat_sum = df.sum()
    #stat_geo = np.exp(np.log(df.prod(axis=0))/df.notna().sum(1))
    #print("Geo", df)
    # remove 0 and nan
    stat_geo = stats.gmean(df.replace(0., np.nan).dropna(), axis=0)
    stat_n = df.count(axis=0).array
    #df.loc['n']= len(df.index)
    df.loc['n']= stat_n
    df.loc['Mean']= stat_mean
    df.loc['Std Dev']= stat_std
    df.loc['Std Dev'] = stat_std.map(lambda x: x if not np.isnan(x) else 0.0)
    df.loc['cv [%]']= df.loc['Std Dev']/df.loc['Mean']*100.0
    df.loc['Median']= stat_q2
    df.loc['iqr']=stat_q3-stat_q1
    df.loc['qcod [%]']=(stat_q3-stat_q1)/(stat_q3+stat_q1)*100.0
    df.loc['Min'] = stat_min
    df.loc['Max'] = stat_max
    df.loc['Range'] = stat_max - stat_min
    df.loc['Geo'] = stat_geo
    df.loc['1st'] = stat_first
    df.loc['Last'] = stat_last
    df.loc['Sum'] = stat_sum
    df.loc['P25'] = stat_q1
    df.loc['P75'] = stat_q3
    df.loc['P90'] = stat_90
    df.loc['P95'] = stat_95
    #if with_nan:
    #   print(df)
    #print(df.T)
    #dfFactor(df, 'Mean')
    if drop_measures:
        df = df[num_measures:]
    return df.T
def dfStatisticsQ(query, timer, warmup=0, cooldown=0):
    dataframe = dfMeasuresQ(query, timer, warmup, cooldown)
    numValues = len(dataframe.columns)
    dataframe = addStatistics(dataframe)
    #df = tools.dataframehelper.addStatistics(df.T)
    return dataframe.iloc[:,numValues:]
def addFactor(dataframe, factor):
    #print(dataframe)
    #dataframe = dfStatistics(evaluation, query, timer)
    if dataframe.empty:
        return dataframe
    # select column 0 = connections
    #connections = dataframe.iloc[0:,0].values.tolist()
    # only consider not 0, starting after n
    #dataframe_non_zero = dataframe[(dataframe.T[1:] != 0).any()]
    if dataframe[factor].max() == 0:
        # there are only zero values
        dataframe.insert(loc=0, column='factor', value=[1 for item in range(len(dataframe.index))])
        return dataframe
    dataframe = dataframe.T
    dataframe_non_zero = dataframe[(dataframe.T != 0).any()]
    # select column for factor and find minimum in cleaned dataframe
    factorlist = dataframe.loc[factor]
    minimum = dataframe_non_zero.loc[factor].min()
    # norm list to mean = 1
    if minimum > 0:
        mean_list_normed = [round(float(item/minimum),4) for item in factorlist]
    else:
        #print(dataframe_non_zero)
        mean_list_normed = [round(float(item),4) for item in factorlist]
    dataframe = dataframe.T
    # insert factor column
    dataframe.insert(loc=0, column='factor', value=mean_list_normed)
    # sort by factor
    dataframe = dataframe.sort_values(dataframe.columns[0], ascending = True)
    # replace float by string
    dataframe = dataframe.replace(0.00, "0.00")
    # drop rows of only 0 (starting after factor and n)
    #dataframe = dataframe[(dataframe.T[3:] != "0.00").any()]
    # replace string by float
    dataframe = dataframe.astype(float)
    #dataframe = dataframe.replace("0.00", 0.00)
    # anonymize dbms
    #dataframe.iloc[0:,0] = dataframe.iloc[0:,0].map(dbms.anonymizer)
    #dataframe = dataframe.set_index(dataframe.columns[0])
    return dataframe
def dfTPSQ(query, numClients):
    df = dfMeasuresQ(query, 'run')
    df = df.T.apply(lambda x: numClients*1000/x)
    #tools.dataframehelper.addStatistics(df2.T).T
    return df
def dfHardware():
    df1=pd.DataFrame.from_dict({c:d['hardwaremetrics'] for c,d in evaluator.evaluation['dbms'].items()}).transpose()
    df2=pd.DataFrame.from_dict({c:d['hostsystem'] for c,d in evaluator.evaluation['dbms'].items()}).transpose()
    if 'CUDA' in df2.columns:
        df2 = df2.drop(['CUDA'],axis=1)
    if 'GPUIDs' in df2.columns:
        df2 = df2.drop(['GPUIDs'],axis=1)
    if 'node' in df2.columns:
        df2 = df2.drop(['node'],axis=1)
    if 'instance' in df2.columns:
        df2 = df2.drop(['instance'],axis=1)
    df = df1.merge(df2,left_index=True,right_index=True).drop(['host','CPU','GPU','RAM','Cores'],axis=1)
    #df3=df1.merge(df2,left_index=True,right_index=True).drop(['CUDA','host','CPU','GPU','instance','RAM','Cores'],axis=1)
    df = df.astype(float)
    df.index = df.index.map(tools.dbms.anonymizer)
    #df = self.addStatistics(df.T).T
    df = df.applymap(lambda x: x if not np.isnan(x) else 0.0)
    return df
def dfSubRows(dataframe, l):
    if len(l) > 0:
        return dataframe[dataframe.index.isin(l)]
    else:
        return dataframe
def dfLoadingMetric(evaluation, metric):
    if 'loadingmetrics' in evaluation['general'] and metric in evaluation['general']['loadingmetrics']:
        df = pd.DataFrame.from_dict(evaluation['general']['loadingmetrics'][metric]).transpose()
        df.index.name = 'DBMS'
    else:
        df = pd.DataFrame()
    return df
def dfStreamingMetric(evaluation, metric):
    if 'streamingmetrics' in evaluation['general'] and metric in evaluation['general']['streamingmetrics']:
        df = pd.DataFrame.from_dict(evaluation['general']['streamingmetrics'][metric]).transpose()
        df.index.name = 'DBMS'
    else:
        df = pd.DataFrame()
    return df
def dfLoaderMetric(evaluation, metric):
    if 'loadermetrics' in evaluation['general'] and metric in evaluation['general']['loadermetrics']:
        df = pd.DataFrame.from_dict(evaluation['general']['loadermetrics'][metric]).transpose()
        df.index.name = 'DBMS'
    else:
        df = pd.DataFrame()
    return df
def dfBenchmarkerMetric(evaluation, metric):
    if 'benchmarkermetrics' in evaluation['general'] and metric in evaluation['general']['benchmarkermetrics']:
        df = pd.DataFrame.from_dict(evaluation['general']['benchmarkermetrics'][metric]).transpose()
        df.index.name = 'DBMS'
    else:
        df = pd.DataFrame()
    return df
def dfDatageneratorMetric(evaluation, metric):
    if 'datageneratormetrics' in evaluation['general'] and metric in evaluation['general']['datageneratormetrics']:
        df = pd.DataFrame.from_dict(evaluation['general']['datageneratormetrics'][metric]).transpose()
        df.index.name = 'DBMS'
    else:
        df = pd.DataFrame()
    return df
