"""
:Date: 2018-01-03
:Version: 0.9
:Authors: Patrick Erdelt

Classes for generating reports for benchmarks.
This uses the class benchmarker of dbmsbenchmarker.

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


class evaluator():
	evaluation = {}
	"""
	Class for generating reports.
	This class generates a survey in latex and saves it to disk.
	The survey has one page per timer.
	"""
	def __init__(self, benchmarker):
		"""
		Construct a new 'reporter' object.

		:param benchmarker: Object of benchmarker containing information about queries, connections and benchmark times
		:return: returns nothing
		"""
		self.benchmarker = benchmarker
		if len(evaluator.evaluation) == 0:
			evaluator.evaluation = self.generate()
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
				evaluation['dbms'][c]['version'] = self.benchmarker.dbms[c].connectiondata["version"]
				evaluation['dbms'][c]['info'] = self.benchmarker.dbms[c].connectiondata["info"]
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
					evaluation['dbms'][c]['hostsystem'] = self.benchmarker.dbms[c].connectiondata["hostsystem"]
				evaluation['dbms'][c]['times'] = {}
				evaluation['dbms'][c]['prices'] = {}
				if "timeLoad" in self.benchmarker.dbms[c].connectiondata:
					evaluation['dbms'][c]['times']['load_ms'] = self.benchmarker.dbms[c].connectiondata["timeLoad"]*1000.0
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
					metricsReporter = monitor.metrics(self.benchmarker)
					hardwareAverages = metricsReporter.computeAverages()
					if c in hardwareAverages:
						for m, avg in hardwareAverages[c].items():
							evaluation['dbms'][c]['hardwaremetrics'][m] = avg
						if 'total_gpu_power' in hardwareAverages[c]:
							# basis: per second average power, total time in ms
							evaluation['dbms'][c]['hardwaremetrics']['total_gpu_energy'] = hardwareAverages[c]['total_gpu_power']*times[c]/3600000
		# appendix start: query survey
		evaluation['query'] = {}
		for i in range(1, len(self.benchmarker.queries)+1):
			evaluation['query'][i] = {}
			evaluation['query'][i]['config'] = self.benchmarker.queries[i-1]
			queryObject = tools.query(self.benchmarker.queries[i-1])
			evaluation['query'][i]['title'] = queryObject.title
			evaluation['query'][i]['active'] = queryObject.active
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
			evaluation['query'][i]['dbms'] = {}
			for connection, dbms in self.benchmarker.dbms.items():
				evaluation['query'][i]['dbms'][connection] = {}
			if 'errors' in self.benchmarker.protocol['query'][str(i)]:
				for connection, error in self.benchmarker.protocol['query'][str(i)]['errors'].items():
					if len(error) > 0 and self.benchmarker.dbms[connection].connectiondata['active']:
						evaluation['query'][i]['dbms'][connection]['error'] = error
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
							if not m in tps[c]:
								tps[c][m] = 1.0
								num[c][m] = 0
							tps[c][m] *= metric
							num[c][m] += 1
		#print(tps)
		#print(num)
		for c, t in tps.items():
			if not c in evaluation['dbms']:
				continue
			evaluation['dbms'][c]['metrics'] = {}
			for m, v in t.items():
				tps[c][m] = math.pow(tps[c][m], 1.0 / num[c][m])
				evaluation['dbms'][c]['metrics'][m] = tps[c][m]
				if '_ps' in m:
					evaluation['dbms'][c]['metrics'][m.replace('_ps', '_ph')] = tps[c][m]*3600.0
		reporterBar = reporter.barer(self.benchmarker)
		dfTotalSum = reporterBar.generate(numQuery=None, timer=self.benchmarker.timers, ensembler='sum')
		dfTotalProd = reporterBar.generate(numQuery=None, timer=self.benchmarker.timers, ensembler='product')
		# generate area plot of total time
		reporterArea = reporter.arear(self.benchmarker)
		dfTotalTime = reporterArea.generate(numQuery=None, timer=self.benchmarker.timers)
		# generate barh plot of total ranking
		dfTotalRank, timers = self.benchmarker.generateSortedTotalRanking()
		evaluation['general']['results'] = {}
		evaluation['general']['results']['totalSum'] = dfTotalSum.to_dict(orient='index')
		evaluation['general']['results']['totalProd'] = dfTotalProd.to_dict(orient='index')
		evaluation['general']['results']['totalTime'] = dfTotalTime.to_dict()
		evaluation['general']['results']['totalRank'] = dfTotalRank.to_dict(orient='index')
		filename = self.benchmarker.path+'/evaluation.dict'
		with open(filename, 'w') as f:
			f.write(str(evaluation))
			#pprint.pprint(evaluation, f)
		return evaluation
	def load(self):
		filename = self.benchmarker.path+'/evaluation.dict'
		with open(filename,'r') as inp:
			evaluator.evaluation = ast.literal_eval(inp.read())
	def pretty(self, d="", indent=0):
		if len(d) == 0:
			d = evaluator.evaluation
		for key, value in d.items():
			if isinstance(value, dict):
				print('  ' * indent + str(key))
				self.pretty(value, indent+1)
			else:
				print('  ' * indent + str(key) + ":" + str(value))
	def structure(self, d="", indent=0):
		if len(d) == 0:
			d = evaluator.evaluation
		for key, value in d.items():
			if isinstance(value, dict):
				print('  ' * indent + str(key))
				self.structure(value, indent+1)
			#else:
			#	print('  ' * indent + str(key) + ":" + str(value))
	def dfMeasures(self, query, timer):
		l={c: [x for i,x in b.items()] for c,b in evaluator.evaluation['query'][query]['benchmarks'][timer]['benchmarks'].items()}
		df = pd.DataFrame(l).T
		df.index.name = 'DBMS'
		#print(df)
		return df
