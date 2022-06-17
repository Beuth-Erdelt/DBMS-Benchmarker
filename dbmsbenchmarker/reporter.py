"""
    Classes for generating reports of benchmarks for the Python Package DBMS Benchmarker
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
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import os
import csv
import json
from dbmsbenchmarker import tools, monitor, evaluator
#import datetime
from datetime import datetime
from tqdm import tqdm
import pickle
import sys
import numpy as np
#import math


class reporter():
	"""
	Class for generating reports.
	This class serves as a base class and does not produce anything.
	"""
	def __init__(self, benchmarker):
		"""
		Construct a new 'reporter' object.

		:param benchmarker: Object of benchmarker containing information about queries, connections and benchmark times
		:return: returns nothing
		"""
		self.benchmarker = benchmarker
	def init(self):
		"""
		Initializes report. This method is called every time a report is generated.

		:return: returns nothing
		"""
		pass
	def save(self, dataframe, title, subtitle, filename):
		"""
		Saves a report.

		:param dataframe: Report data given as a pandas DataFrame
		:param title: Title of the report
		:param subtitle: Subtitle of the report
		:param filename: Name of the file the report will be saved to
		:return: returns nothing
		"""
		pass
	def generate(self, numQuery, timer):
		"""
		Generates a report for a given query and a given timer.

		:param numQuery: Number of the query the report is about. Starts at 1.
		:param timer: Timer object
		:return: returns nothing
		"""
		# are there benchmarks for this query?
		print("Generate report")
		if not self.benchmarker.timerExecution.checkForSuccessfulBenchmarks(numQuery):
			print("Missing data")
	def generateAll(self, timer):
		"""
		Generates all reports for the benchmarker object for a given timer.
		If benchmarker has a fixed query, only reports for this query are generated.

		:param timer: Timer object
		:return: returns nothing
		"""
		if not self.benchmarker.fixedQuery is None:
			self.generate(self.benchmarker.fixedQuery, timer)
		else:
			if self.benchmarker.bBatch:
				range_runs = range(1,len(self.benchmarker.queries)+1)
			else:
				range_runs = tqdm(range(1,len(self.benchmarker.queries)+1))
			for numQuery in range_runs:
				self.generate(numQuery, timer)




class storer(reporter):
	"""
	Class for generating reports.
	This class saves a protocol in json and benchmarks as csv files.
	It also provides a load() method to restore previous benchmarks.
	"""
	def __init__(self, benchmarker):
		reporter.__init__(self, benchmarker)
	def save(self, dataframe, filename):
		"""
		Saves benchmark table of a query as csv file.

		:param dataframe: DataFrame containing connection names (columns) and benchmark times (rows)
		:param filename: Filename for csv file
		:return: returns nothing
		"""
		# transpose
		dft = dataframe.transpose()
		# set column names
		dft.columns = dft.iloc[0]
		# remove first row
		df_transposed = dft[1:]
		# convert to csv
		csv = df_transposed.to_csv(index_label=False,index=False)
		# save
		csv_file = open(filename, "w")
		csv_file.write(csv)
		csv_file.close()
	def generate(self, numQuery, timer):
		"""
		Generates and saves a benchmark table of a given query as csv files per timer.

		:param numQuery: Number of query to save benchmarks of
		:param timer: Timer containing benchmark results
		:return: returns nothing
		"""
		self.writeProtocol()
		for t in timer:
			# are there benchmarks for this query?
			if not t.checkForSuccessfulBenchmarks(numQuery):
				continue
			logging.debug("saveBenchmarkOfQuery {}, timer={} ".format(numQuery, t.name))
			query = tools.query(self.benchmarker.queries[numQuery-1])
			df = t.toDataFrame(numQuery)
			# save as csv
			self.save(
				dataframe = df,
				filename = self.benchmarker.path+'/query_'+str(numQuery)+'_'+t.name+'.csv')
	def load(self, query, numQuery, timer):
		"""
		Loads benchmark table of a given query from csv files per timer.

		:param numQuery: Number of query to save benchmarks of
		:param query: Query object, because number of warmups is needed
		:param timer: Timer containing benchmark results
		:return: True if successful
		"""
		for t in timer:
			# load execution benchmarks
			filename = self.benchmarker.path+'/query_'+str(numQuery)+'_'+t.name+'.csv'
			if os.path.isfile(filename):
				df = pd.read_csv(filename)
				df_t = df.transpose()
				d = df.to_dict(orient="list")
				t.appendTimes(d, query)#.warmup)
				logging.debug("Read "+filename)
			else:
				t.appendTimes({}, query)#.warmup)
				# timer is missing
				#logging.debug(filename + " not found")
				#return False
		return True
	def writeProtocol(self):
		"""
		Saves procol of benchmarker in JSON format.

		:return: returns nothing
		"""
		filename = self.benchmarker.path+'/protocol.json'
		with open(filename, 'w') as f:
			json.dump(self.benchmarker.protocol, f)
	def readProtocol(self):
		"""
		Loads procol of benchmarker in JSON format.

		:return: returns nothing
		"""
		try:
			filename = self.benchmarker.path+'/protocol.json'
			with open(filename, 'r') as f:
				self.benchmarker.protocol = json.load(f)
		except Exception as e:
				print("No protocol found")
		finally:
			pass





class printer(reporter):
	"""
	Class for generating reports.
	This class prints a survey to console.
	"""
	def __init__(self, benchmarker):
		reporter.__init__(self, benchmarker)
	def generate(self, numQuery, timer):
		"""
		Generates report for console output.
		This currently always prints the execution timer.
		Anonymizes dbms if activated.

		:param numQuery: Number of query to generate report of
		:param timer: Timer containing benchmark results
		:return: returns nothing
		"""
		include_benchmarks = False
		# are there benchmarks for this query?
		if not self.benchmarker.timerExecution.checkForSuccessfulBenchmarks(numQuery):
			return False
		# is query active?
		query = tools.query(self.benchmarker.queries[numQuery-1])
		if not query.active:
			return False
		# get data
		stats = self.benchmarker.timerExecution.stats[numQuery-1]
		times = self.benchmarker.timerExecution.times[numQuery-1]
		# construct table header
		header = tools.timer.header_stats.copy()
		if include_benchmarks:
			header_times = list(map(lambda h: "w"+str(h)+"", list(range(1,query.warmup+1))))
			header.extend(header_times)
			header_times = list(map(lambda h: str(h)+"", list(range(query.warmup+1,query.numRun+1))))
			header.extend(header_times)
		# format stats and times
		stats_output = {k: [list(map(lambda x: '{:.{prec}f}'.format(x, prec=2) if x is not None else 0, sublist)) for sublist in [stat_q]] for k,stat_q in stats.items()}
		times_output = {k: [list(map(lambda x: '{:.{prec}f}'.format(x, prec=2) if x is not None else 0, sublist)) for sublist in [time_q]] for k,time_q in times.items()}
		# add connection names
		data = []
		for c in sorted(self.benchmarker.dbms.keys()):
			# is dbms active and we have data
			if self.benchmarker.dbms[c].connectiondata['active']:
				if c in stats_output and c in times_output:
					l = list([self.benchmarker.dbms[c].getName()])
					l.extend(*stats_output[c])
					if include_benchmarks:
						l.extend(*times_output[c])
					data.append(l)
		# print table
		print('Q'+str(numQuery)+': '+query.title+" - timerExecution")
		print(tabulate(data,headers=header, tablefmt="grid", floatfmt=".2f"))
		# print errors
		for key, value in self.benchmarker.protocol['query'][str(numQuery)]['errors'].items():
			if key in self.benchmarker.dbms and self.benchmarker.dbms[key].connectiondata['active']:
				if len(value) > 0:
					firstpos = value.find(': ')
					if firstpos > 0:
						print(key + ': ' + value[firstpos:])
					else:
						print(key + ': ' + value)
		return data




class pickler(reporter):
	"""
	Class for generating reports.
	Generates a pickle file of statistics as a dataframe and saves it to disk.
	"""
	def __init__(self, benchmarker):
		reporter.__init__(self, benchmarker)
	def save(self, dataframe, filename):
		"""
		Saves report of a given query as pickle file of a dataframe per timer.
		Anonymizes dbms if activated.

		:param dataframe: Report data given as a pandas DataFrame (rows=dbms, cols=benchmarks)
		:param title: Title of the report
		:param subtitle: Subtitle of the report
		:param filename: Name of the file the report will be saved to
		:return: returns nothing
		"""
		f = open(filename, "wb")
		pickle.dump(dataframe, f)
		f.close()
	def generate(self, numQuery, timer):
		"""
		Generates a pickle file of statistics as a dataframe and saves it to disk.
		Anonymizes dbms if activated.

		:param numQuery: Number of query to generate report of
		:param timer: Timer containing benchmark results
		:return: returns nothing
		"""
		for t in timer:
			# are there benchmarks for this query?
			if not t.checkForSuccessfulBenchmarks(numQuery):
				continue
			query = tools.query(self.benchmarker.queries[numQuery-1])
			# is timer active for this query?
			if not query.timer[t.name]['active']:
				continue
			# is query active?
			if not query.active:
				continue
			# convert statistics to DataFrame
			dataframe = self.benchmarker.statsToDataFrame(numQuery, t)
			# test if any rows left
			#if (dataframe[(dataframe.T[1:] != 0).any()]).empty:
			if dataframe.empty:
				# ignore empty dataframes
				return
			# remove inactive connections
			#dataframe = tools.dataframehelper.removeInactiveConnections(dataframe, self.benchmarker)
			# add factor column
			#dataframe = tools.dataframehelper.addFactor(dataframe, self.benchmarker.queryconfig['factor'])
			logging.debug("Pickle Q"+str(numQuery)+" for timer "+t.name)
			# save as plot
			self.save(
				dataframe = dataframe,
				filename = self.benchmarker.path+'/query_'+str(numQuery)+'_'+t.name+'_statistics.pickle')




class metricer(reporter):
	"""
	Class for generating reports.
	Generates a plot of the benchmarks as a time series and saves it to disk.
	"""
	def __init__(self, benchmarker, per_stream=False):
		reporter.__init__(self, benchmarker)
		self.per_stream = per_stream
	def save(self, dataframe, title, subtitle, filename):
		"""
		Saves report of a given query as plot image per timer.
		Anonymizes dbms if activated.

		:param dataframe: Report data given as a pandas DataFrame (rows=dbms, cols=benchmarks)
		:param title: Title of the report
		:param subtitle: Subtitle of the report
		:param filename: Name of the file the report will be saved to
		:return: returns nothing
		"""
		pass
	def generate(self, numQuery, timer):
		"""
		Generates a plot of the benchmarks as a time series and saves it to disk.
		Anonymizes dbms if activated.

		:param numQuery: Number of query to generate report of
		:param timer: Timer containing benchmark results
		:return: returns nothing
		"""
		for connectionname, connection in self.benchmarker.dbms.items():
			if connection.hasHardwareMetrics():
				logging.debug("Hardware metrics for Q"+str(numQuery))
				metricsReporter = monitor.metrics(self.benchmarker)
				#metricsReporter.generatePlotForQuery(numQuery)
				metricsReporter.fetchMetricPerQuery(numQuery)
				break
	def generateAll(self, timer):
		"""
		Generates all reports for the benchmarker object for a given timer.
		If benchmarker has a fixed query, only reports for this query are generated.
		A plot of the total times is generated in any case.
		Anonymizes dbms if activated.

		:param timer: Timer object
		:return: returns nothing
		"""
		# per query
		if not self.per_stream:
			if self.benchmarker.bBatch:
				range_runs = self.benchmarker.protocol['query'].items()
			else:
				range_runs = tqdm(self.benchmarker.protocol['query'].items())
			for numQuery, protocol in range_runs:
				query = tools.query(self.benchmarker.queries[int(numQuery)-1])
				if not query.active:
					continue
				self.generate(numQuery, [])
		#for q, d in self.benchmarker.protocol['query'].items():
		#	query = tools.query(self.benchmarker.queries[int(q)-1])
		#	# is query active?
		#	if not query.active:
		#		continue
		#	self.generate(q, [])
		# per stream
		if self.per_stream:
			number_of_queries = len(self.benchmarker.protocol['query'].items())
			for c, connection in self.benchmarker.dbms.items():
				if connection.hasHardwareMetrics():
					logging.info("Hardware metrics for stream of connection {}".format(c))
					times = self.benchmarker.protocol['query'][str(1)]
					time_start = int(datetime.timestamp(datetime.strptime(times["starts"][c],'%Y-%m-%d %H:%M:%S.%f')))
					times = self.benchmarker.protocol['query'][str(number_of_queries)]
					time_end = int(datetime.timestamp(datetime.strptime(times["ends"][c],'%Y-%m-%d %H:%M:%S.%f')))
					#logging.debug(connection.connectiondata['monitoring']['prometheus_url'])
					query='stream'
					if 'metrics' in connection.connectiondata['monitoring']:
						metrics_dict = connection.connectiondata['monitoring']['metrics']
					else:
						metrics_dict = monitor.metrics.metrics
					for m, metric in metrics_dict.items():
						logging.debug("Metric {}".format(m))
						monitor.metrics.fetchMetric(query, m, c, connection.connectiondata, time_start, time_end, '{result_path}/'.format(result_path=self.benchmarker.path))































































class DEPRECATED_ploter(reporter):
	"""
	Class for generating reports.
	Generates a plot of the benchmarks as a time series and saves it to disk.
	"""
	def __init__(self, benchmarker):
		reporter.__init__(self, benchmarker)
		self.e = None
	def save(self, dataframe, query, title, subtitle, filename, timer):
		"""
		Saves report of a given query as plot image per timer.
		Anonymizes dbms if activated.

		:param dataframe: Report data given as a pandas DataFrame (rows=dbms, cols=benchmarks)
		:param query: Query object
		:param title: Title of the report
		:param subtitle: Subtitle of the report
		:param filename: Name of the file the report will be saved to
		:return: returns nothing
		"""
		# test if any rows left
		if len(dataframe.index) < 1:
			return False
		# transpose
		df_transposed = dataframe.transpose()
		# add index name
		df_transposed.index.name = subtitle
		# plot
		#print(tools.dbms.dbmscolors)
		plotdata = df_transposed.plot(title=title, figsize=(12,8), color=[tools.dbms.dbmscolors.get(x, '#333333') for x in df_transposed.columns])
		# scale y-axis: 0 to max plus 10%
		plotdata.set_ylim(0, df_transposed.max().max()*1.10,0)
		if timer.perRun:
			if query.warmup > 0:
				plt.axvline(x=query.numRunBegin+1, linestyle="--", color="black")
			if query.cooldown > 0:
				plt.axvline(x=query.numRunEnd, linestyle="--", color="black")
		plt.legend(title="DBMS")
		plt.savefig(filename, bbox_inches='tight')
		plt.close('all')
	def generate(self, numQuery, timer):
		"""
		Generates a plot of the benchmarks as a time series and saves it to disk.
		Anonymizes dbms if activated.

		:param numQuery: Number of query to generate report of
		:param timer: Timer containing benchmark results
		:return: returns nothing
		"""
		for t in timer:
			# are there benchmarks for this query?
			if not t.checkForSuccessfulBenchmarks(numQuery):
				continue
			query = tools.query(self.benchmarker.queries[numQuery-1])
			# is timer active for this query?
			if not query.timer[t.name]['active']:
				continue
			# is query active?
			if not query.active:
				continue
			# benchmark times as a dataframe
			#df = self.benchmarker.benchmarksToDataFrame(numQuery, t)
			#print(df)
			if self.e is None:
				self.e = evaluator.evaluator(self.benchmarker)
			df = evaluator.dfMeasuresQ(numQuery, t.name)
			#df.index = df.index.map(tools.dbms.anonymizer)
			#print(df)
			logging.debug("Plot Q"+str(numQuery)+" for timer "+t.name)
			if t.perRun:
				#subtitle = "Warmup = "+str(query.warmup)+", test runs = "+str(query.numRun-query.warmup-query.cooldown)+", cooldown = "+str(query.cooldown)
				subtitle = "Number of run"
			else:
				subtitle = "Number of session"
			# save as plot
			self.save(
				dataframe = df,
				query = query,
				#title = "Q"+str(numQuery)+": Time "+t.name+" [ms]",
				title = "Q"+str(numQuery)+": Time "+t.name+" [ms] in "+str(len(df.columns))+" observations",
				subtitle = subtitle,
				filename = self.benchmarker.path+'/query_'+str(numQuery)+'_'+t.name+'_plot.png',
				timer=t)



class DEPRECATED_dataframer(reporter):
	"""
	Class for generating reports.
	Generates a data frame of the benchmark times and saves it to disk as a pickle file.
	"""
	def __init__(self, benchmarker):
		reporter.__init__(self, benchmarker)
	def save(self, dataframe, title, subtitle, filename):
		"""
		Saves report of a given query as a dataframe.
		Anonymizes dbms if activated.

		:param dataframe: Report data given as a pandas DataFrame (rows=dbms, cols=benchmarks)
		:param title: Title of the report
		:param subtitle: Subtitle of the report
		:param filename: Name of the file the report will be saved to
		:return: returns nothing
		"""
		# test if any rows left
		if len(dataframe.index) < 1:
			return False
		# transpose
		df_transposed = dataframe.transpose()
		# add index name
		df_transposed.index.name = subtitle
		# plot
		f = open(filename, "wb")
		pickle.dump(df_transposed, f)
		f.close()
	def generate(self, numQuery, timer):
		"""
		Generates a data frame of the benchmark times and saves it to disk as a pickle file.
		Anonymizes dbms if activated.

		:param numQuery: Number of query to generate report of
		:param timer: Timer containing benchmark results
		:return: returns nothing
		"""
		for t in timer:
			# are there benchmarks for this query?
			if not t.checkForSuccessfulBenchmarks(numQuery):
				continue
			query = tools.query(self.benchmarker.queries[numQuery-1])
			# is timer active for this query?
			if not query.timer[t.name]['active']:
				continue
			# is query active?
			if not query.active:
				continue
			# benchmark times as a datarame
			df = self.benchmarker.benchmarksToDataFrame(numQuery, t)
			logging.debug("Dataframe Q"+str(numQuery)+" for timer "+t.name)
			# save as plot
			self.save(
				dataframe = df,
				title = "Q"+str(numQuery)+": Time "+t.name+" [ms]",
				subtitle = "Warmup = "+str(query.warmup)+", test runs = "+str(query.numRun)+", cooldown = "+str(query.cooldown),
				filename = self.benchmarker.path+'/query_'+str(numQuery)+'_'+t.name+'_dataframe.pickle')




class DEPRECATED_boxploter(reporter):
	"""
	Class for generating reports.
	Generates a boxplot of the time series and saves it to disk.
	"""
	def __init__(self, benchmarker):
		reporter.__init__(self, benchmarker)
		self.e = None
	def save(self, dataframe, title, filename):
		"""
		Saves report of a given query as boxplot image per timer.
		Anonymizes dbms if activated.

		:param dataframe: Report data given as a pandas DataFrame (rows=dbms, cols=benchmarks)
		:param title: Title of the report
		:param filename: Name of the file the report will be saved to
		:return: returns nothing
		"""
		# test if any rows left
		if len(dataframe.index) < 1:
			return False
		# move index to first column
		dataframe.reset_index(level=0, inplace=True)
		# unpivot
		df_unpivot = pd.melt(dataframe, id_vars='DBMS')
		# drop one column
		df_unpivot=df_unpivot.drop(labels='variable',axis=1)
		# name columns
		df_unpivot.columns = ['DBMS', 'time [ms]']
		# anonymize dbms already done
		#df_unpivot['DBMS'] = df_unpivot['DBMS'].map(tools.dbms.anonymizer)
		# no results left
		if df_unpivot.empty:
			return
		# plot
		boxplot = df_unpivot.boxplot(by='DBMS', grid=False, figsize=(12,10))
		# rotate labels
		plt.xticks(rotation=90)
		# align box to labels
		plt.tight_layout()
		# set title and no subtitle
		plt.title(title)
		plt.suptitle("")
		# save
		plt.savefig(filename, bbox_inches='tight')
		plt.close('all')
	def generate(self, numQuery, timer):
		"""
		Generates a boxplot of the time series and saves it to disk.
		Anonymizes dbms if activated.

		:param numQuery: Number of query to generate report of
		:param timer: Timer containing benchmark results
		:return: returns nothing
		"""
		for t in timer:
			# are there benchmarks for this query?
			if not t.checkForSuccessfulBenchmarks(numQuery):
				continue
			query = tools.query(self.benchmarker.queries[numQuery-1])
			# is timer active for this query?
			if not query.timer[t.name]['active']:
				continue
			# is query active?
			if not query.active:
				continue
			# benchmark times as a dataframe
			#df = self.benchmarker.benchmarksToDataFrame(numQuery, t)
			if self.e is None:
				self.e = evaluator.evaluator(self.benchmarker)
			df = evaluator.dfMeasuresQ(numQuery, t.name)
			logging.debug("Boxplot Q"+str(numQuery)+" for timer "+t.name)
			#print(df)
			#print(query.warmup)
			#print(query.numRunEnd)
			#print(query.numRun)
			if t.perRun:
				# leave out warumup/cooldown
				df = df.drop(range(query.warmup),axis=1)
				df = df.drop(range(query.numRunEnd, query.numRun),axis=1)
			df = df.loc[:, (df != 0).any(axis=0)]
			# save as boxplot
			self.save(
				dataframe = df,
				title = "Q"+str(numQuery)+": Time "+t.name+" [ms] in "+str(len(df.columns))+" observations",
				filename = self.benchmarker.path+'/query_'+str(numQuery)+'_'+t.name+'_boxplot.png')



class DEPRECATED_barer(reporter):
	"""
	Class for generating reports.
	Generates a bar chart of the time series and saves it to disk.
	"""
	def __init__(self, benchmarker):
		reporter.__init__(self, benchmarker)
	def save(self, dataframe, title, filename):
		"""
		Saves report of a given query as bar chart image per timer.
		Anonymizes dbms if activated.

		:param dataframe: Report data given as a pandas DataFrame
		:param title: Title of the report
		:param filename: Name of the file the report will be saved to
		:return: returns nothing
		"""
		# leave out not stackable
		# TODO: read from object
		for i,t in enumerate(self.benchmarker.timers):
			if not t.stackable:
				if t.name in dataframe.columns:
					dataframe = dataframe.drop([t.name],axis=1)
		#if 'session' in dataframe.columns:
		#	dataframe = dataframe.drop(['session'],axis=1)
		#if 'run' in dataframe.columns:
		#	dataframe = dataframe.drop(['run'],axis=1)
		# plot
		dataframe.plot.bar(stacked=True, figsize=(12,12))
		# rotate labels
		#plt.xticks(rotation=70, fontsize=12)
		# align box to labels
		plt.tight_layout()
		# set title
		plt.legend(title=title)
		# save
		plt.savefig(filename)
		plt.close('all')
	def generate(self, numQuery, timer, ensembler='sum'):
		"""
		Generates a bar chart of the time series and saves it to disk.
		Anonymizes dbms if activated.

		:param numQuery: Number of query to generate report of
		:param timer: Timer containing benchmark results
		:param type: Can be 'sum' or 'product'
		:return: returns nothing
		"""
		if ensembler == 'sum':
			dataframe, title = tools.dataframehelper.sumPerTimer(self.benchmarker, numQuery=numQuery, timer=timer)
		else:
			dataframe, title = tools.dataframehelper.multiplyPerTimer(self.benchmarker, numQuery=numQuery, timer=timer)
		if dataframe is None:
			return None
		if numQuery is None:
			#title = chartlabel+" in "+str(numQueriesEvaluated)+" benchmarks ("+str(numBenchmarks)+" runs) [ms]"
			filename = self.benchmarker.path+'/total_bar_'+ensembler+'.png'
		else:
			#title = "Q"+str(numQuery)+": "+chartlabel+" [ms] in "+str(query.numRun-query.warmup)+" benchmark test runs"
			filename = self.benchmarker.path+'/query_'+str(numQuery)+'_bar.png'
		#print(title)
		#print(tabulate(dataframe,headers=dataframe.columns,tablefmt="grid", floatfmt=".2f"))
		# save as boxplot
		self.save(
			dataframe = dataframe,
			title = title,
			filename = filename)
		return dataframe
	def generateAll(self, timer):
		"""
		Generates all reports for the benchmarker object for a given timer.
		If benchmarker has a fixed query, only reports for this query are generated.
		A plot of the total times is generated in any case.
		Anonymizes dbms if activated.

		:param timer: Timer object
		:return: returns nothing
		"""
		if not self.benchmarker.fixedQuery is None:
			self.generate(self.benchmarker.fixedQuery, timer)
		else:
			if self.benchmarker.bBatch:
				#range_runs = range(0, query.warmup+query.numRun)
				range_runs = range(1,len(self.benchmarker.queries)+1)
			else:
				#range_runs = tqdm(range(0, query.warmup+query.numRun))
				range_runs = tqdm(range(1,len(self.benchmarker.queries)+1))
			for numQuery in range_runs:
				query = tools.query(self.benchmarker.queries[numQuery-1])
				# is query active?
				if not query.active:
					continue
				logging.debug("Bar Chart Q"+str(numQuery))
				self.generate(numQuery, timer)
		self.generate(numQuery=None, timer=timer)



class DEPRECATED_tps(reporter):
	"""
	Class for generating reports.
	Generates a bar chart of the time series and saves it to disk.
	"""
	def __init__(self, benchmarker):
		reporter.__init__(self, benchmarker)
		self.e = None
	def save(self, dataframe, filename_lat, filename_tps):
		"""
		Saves report of a given query as bar chart image per timer.
		Anonymizes dbms if activated.

		:param dataframe: Report data given as a pandas DataFrame
		:param title: Title of the report
		:param filename: Name of the file the report will be saved to
		:return: returns nothing
		"""
		settings_translate = {
			'latency_run_mean_ms':'lat_r',
			'latency_session_mean_ms':'lat_s',
			'throughput_run_total_ps':'tps_r1',
			'throughput_run_mean_ps':'tps_r2',
			'throughput_session_total_ps':'tps_s1',
			'throughput_session_mean_ps':'tps_s2',
			'throughput_run_total_ph':'tph_r1',
			'throughput_run_mean_ph':'tph_r2',
			'throughput_session_total_ph':'tph_s1',
			'throughput_session_mean_ph':'tph_s2',
			'queuesize_run':'qs_r',
			'queuesize_sesion':'qs_s'
		}
		dataframe = dataframe.reindex(['throughput_run_total_ps','throughput_run_mean_ps','throughput_session_total_ps','throughput_session_mean_ps','latency_run_mean_ms','latency_session_mean_ms','throughput_run_mean_ph','queuesize_run','queuesize_session'], axis=1)
		dataframe = dataframe.rename(columns = settings_translate)
		dataframe = dataframe.reindex(sorted(dataframe.index), axis=0)
		#print(dataframe)
		plt.figure()
		df1 = dataframe[['tps_r1','tps_r2','tps_s1','tps_s2']].transpose()
		df1.plot.bar(title="Throughputs [Hz]", color=[tools.dbms.dbmscolors.get(x, '#333333') for x in df1.columns])
		# rotate labels
		plt.xticks(rotation=70, fontsize=12)
		# align box to labels
		plt.tight_layout()
		plt.legend(title="DBMS")
		#filename = self.benchmarker.path+'/total_bar_tps.png'
		plt.savefig(filename_tps)
		plt.close('all')
		plt.figure()
		df2 = dataframe[['lat_r','lat_s']].transpose()
		df2.plot.bar(title="Latencies [ms]", color=[tools.dbms.dbmscolors.get(x, '#333333') for x in df2.columns])
		# rotate labels
		plt.xticks(rotation=70, fontsize=12)
		# align box to labels
		plt.tight_layout()
		plt.legend(title="DBMS")
		#filename = self.benchmarker.path+'/total_bar_lat.png'
		plt.savefig(filename_lat)
		plt.close('all')
	def generate(self, numQuery, timer, ensembler='sum'):
		"""
		Generates a bar chart of the time series and saves it to disk.
		Anonymizes dbms if activated.

		:param numQuery: Number of query to generate report of
		:param timer: Timer containing benchmark results
		:param type: Can be 'sum' or 'product'
		:return: returns nothing
		"""
		#if len(evaluator.evaluator.evaluation) == 0:
		#	self.e = evaluator.evaluator(self.benchmarker)
		if self.e is None:
			self.e = evaluator.evaluator(self.benchmarker)
		#evaluation = evaluator.evaluator.evaluation
		evaluation = self.e.get_evaluation()
		if numQuery is None:
			dfTotalLatTPS = pd.DataFrame.from_dict({c:{m:metric for m,metric in dbms['metrics'].items()} for c,dbms in evaluation['dbms'].items()}).transpose()
			if dfTotalLatTPS.empty:
				return None
			dfTotalLatTPS = dfTotalLatTPS.drop(columns='totaltime_ms')
		else:
			dfTotalLatTPS = pd.DataFrame.from_dict({c:d['metrics'] for c,d in evaluation['query'][str(numQuery)]['dbms'].items() if 'metrics' in d and c in evaluation['dbms'] and not 'error' in d}).transpose()
		if dfTotalLatTPS is None or dfTotalLatTPS.empty:
			return None
		dfTotalLatTPS.index = dfTotalLatTPS.index.map(tools.dbms.anonymizer)
		if numQuery is None:
			#title = chartlabel+" in "+str(numQueriesEvaluated)+" benchmarks ("+str(numBenchmarks)+" runs) [ms]"
			filename_tps = self.benchmarker.path+'/total_bar_tps.png'
			filename_lat = self.benchmarker.path+'/total_bar_lat.png'
		else:
			#title = "Q"+str(numQuery)+": "+chartlabel+" [ms] in "+str(query.numRun-query.warmup)+" benchmark test runs"
			filename_tps = self.benchmarker.path+'/query_'+str(numQuery)+'_tps.png'
			filename_lat = self.benchmarker.path+'/query_'+str(numQuery)+'_lat.png'
		#print(title)
		#print(tabulate(dataframe,headers=dataframe.columns,tablefmt="grid", floatfmt=".2f"))
		# save as boxplot
		self.save(
			dataframe = dfTotalLatTPS,
			filename_lat = filename_lat,
			filename_tps = filename_tps)
		dfTotalLatTPS = tools.dataframehelper.addStatistics(dfTotalLatTPS)
		return dfTotalLatTPS
	def generateAll(self, timer):
		"""
		Generates all reports for the benchmarker object for a given timer.
		If benchmarker has a fixed query, only reports for this query are generated.
		A plot of the total times is generated in any case.
		Anonymizes dbms if activated.

		:param timer: Timer object
		:return: returns nothing
		"""
		if not self.benchmarker.fixedQuery is None:
			self.generate(self.benchmarker.fixedQuery, timer)
		else:
			if self.benchmarker.bBatch:
				#range_runs = range(0, query.warmup+query.numRun)
				range_runs = range(1,len(self.benchmarker.queries)+1)
			else:
				#range_runs = tqdm(range(0, query.warmup+query.numRun))
				range_runs = tqdm(range(1,len(self.benchmarker.queries)+1))
			for numQuery in range_runs:
				query = tools.query(self.benchmarker.queries[numQuery-1])
				# is query active?
				if not query.active:
					continue
				logging.debug("TPS Bar Chart Q"+str(numQuery))
				self.generate(numQuery, timer)
		self.generate(numQuery=None, timer=timer)


class DEPRECATED_hister(reporter):
	"""
	Class for generating reports.
	Generates a bar chart of the time series and saves it to disk.
	"""
	def __init__(self, benchmarker):
		reporter.__init__(self, benchmarker)
		self.e = None
	def save(self, dataframe, filename, title):
		"""
		Saves report of a given query as bar chart image per timer.
		Anonymizes dbms if activated.

		:param dataframe: Report data given as a pandas DataFrame
		:param title: Title of the report
		:param filename: Name of the file the report will be saved to
		:return: returns nothing
		"""
		# align box to labels
		#hist = dataframe.plot.hist(bins=len(dataframe.index), figsize=(12,8))
		#print(dataframe)
		df1_nonzero = ((dataframe != 0) & (dataframe is not None) & (dataframe.notnull())).astype(int).sum(axis=0)
		bins = df1_nonzero.min()
		#print(bins)
		#bins = int((len(dataframe.index)+1)/4)
		#print(df1_nonzero)
		#hist = dataframe.hist(bins=int(len(dataframe.columns)/2+1), figsize=(12,8), label=dataframe.columns)#, color=[tools.dbms.dbmscolors.get(x, '#333333') for x in dataframe.columns])
		hist = dataframe.hist(bins=bins, figsize=(12,8), label=dataframe.columns, ec='black')#, color=[tools.dbms.dbmscolors.get(x, '#333333') for x in dataframe.columns])
		for i in range(len(hist)):
			#print(i)
			for j in range(len(hist[i])):
				#print(hist[i][j].title)
				if not hist[i][j].title:
					continue
				dbms = hist[i][j].title.get_text()
				if len(dbms) == 0:
					continue
				#print(dbms)
				obs = df1_nonzero[dbms]
				hist[i][j].set_xlabel(dbms)
				hist[i][j].title.set_text(str(obs)+" observations in "+str(bins)+" bins")
		plt.subplots_adjust(hspace=0.5)
		#dataframe.plot.hist(bins=len(dataframe.index), title=title, figsize=(12,8), alpha=0.5, color=[tools.dbms.dbmscolors.get(x, '#333333') for x in dataframe.columns])
		#plt.axvline(dataframe.mean(), color='b', linestyle='dashed', linewidth=1)
		#plt.show()
		plt.tight_layout()
		#ax = plt.gca()
		#ax.set_title(title)
		plt.legend()
		#plt.legend(title=title)
		#filename = self.benchmarker.path+'/total_bar_lat.png'
		plt.savefig(filename)
		plt.close('all')
	def generate(self, numQuery, timer):
		"""
		Generates a boxplot of the time series and saves it to disk.
		Anonymizes dbms if activated.

		:param numQuery: Number of query to generate report of
		:param timer: Timer containing benchmark results
		:return: returns nothing
		"""
		for t in timer:
			# are there benchmarks for this query?
			if not t.checkForSuccessfulBenchmarks(numQuery):
				continue
			query = tools.query(self.benchmarker.queries[numQuery-1])
			# is timer active for this query?
			if not query.timer[t.name]['active']:
				continue
			# is query active?
			if not query.active:
				continue
			# benchmark times as a dataframe
			#if len(evaluator.evaluator.evaluation) == 0:
			if self.e is None:
				self.e = evaluator.evaluator(self.benchmarker)
			df = evaluator.dfMeasuresQ(numQuery, t.name)
			logging.debug("Hist Q"+str(numQuery)+" for timer "+t.name)
			#print(df)
			#print(query.warmup)
			#print(query.numRunEnd)
			#print(query.numRun)
			if t.perRun:
				# leave out warumup/cooldown
				#print(df)
				df = df.drop(range(0,query.warmup),axis=1)
				df = df.drop(range(query.numRunEnd, query.numRun),axis=1)
			#df = df.replace(np.nan, 0)
			df = df.replace(0, np.nan)
			#df = df.loc[(df != 0).any(axis=0),:]
			df = df[(df.T.notnull()).any()]
			#df = df[(df.T != 0).any()]
			#df = df.loc[:, (df.notnull()).any(axis=0)]
			#df = df[(df[0:].notnull()).any(axis=0)]
			#print(df)
			#print(numQuery)
			#print(t.name)
			if df.empty:
				continue
			# save as boxplot
			self.save(
				dataframe = df.T,
				title = "Q"+str(numQuery)+": Time "+t.name+" [ms] in "+str(len(df.index))+" observations",
				filename = self.benchmarker.path+'/query_'+str(numQuery)+'_'+t.name+'_hist.png')



class DEPRECATED_arear(reporter):
	"""
	Class for generating reports.
	Generates a plot of the benchmarks as a time series and saves it to disk.
	"""
	def __init__(self, benchmarker):
		reporter.__init__(self, benchmarker)
		self.normed = False
	def save(self, dataframe, title, subtitle, filename):
		"""
		Saves report of a given query as plot image per timer.
		Anonymizes dbms if activated.

		:param dataframe: Report data given as a pandas DataFrame (rows=dbms, cols=benchmarks)
		:param title: Title of the report
		:param subtitle: Subtitle of the report
		:param filename: Name of the file the report will be saved to
		:return: returns nothing
		"""
		# test if any rows left
		if len(dataframe.index) < 1:
			return False
		# plot
		#plotdata = df_transposed.plot(title=title)
		dataframe.plot.area(title=title, figsize=(12,8), color=[tools.dbms.dbmscolors.get(x, '#333333') for x in dataframe.columns])#, xticks=list(dataframe.index))
		# rotate labels
		plt.xticks(range(len(dataframe.index)), dataframe.index)
		#plt.xticks(rotation=70, fontsize=12)
		# align box to labels
		plt.tight_layout()
		#ax.set_xticklabels(dataframe.index)
		# plot
		plt.legend(title="DBMS")
		plt.savefig(filename, bbox_inches='tight')
		plt.close('all')
	def generate(self, numQuery, timer):
		"""
		Generates a plot of the benchmarks as a time series and saves it to disk.
		Anonymizes dbms if activated.

		:param numQuery: Number of query to generate report of
		:param timer: Timer containing benchmark results
		:return: returns nothing
		"""
		dataframe, title = tools.dataframehelper.totalTimes(self.benchmarker)
		if dataframe is None:
			return dataframe
		if self.normed:
			# normalize
			dataframePlot = dataframe.div(dataframe.sum(axis=1)/100.0, axis=0)
			dataframe = dataframe.div(dataframe.mean(axis=1)/100.0, axis=0)
			#dataframePlot.index.name = '[%]'
			#dataframe.columns.name = '[%]'
			dataframe.index.name = '[%]'
			#dataframePlot = dataframePlot.reset_index(drop=True)
			#dataframe = dataframe.reset_index(drop=True)
		else:
			dataframePlot = dataframe
			#dataframePlot.index.name = '[ms]'
			#dataframe.columns.name = '[ms]'
			dataframe.index.name = '[ms]'
			#dataframePlot = dataframePlot.reset_index(drop=True)
			#dataframe = dataframe.reset_index(drop=True)
		#print(dataframe)
		if numQuery is None:
			if not self.normed:
				filename = self.benchmarker.path+'/total_time_area.png'
				title = "Total times [ms]"
			else:
				filename = self.benchmarker.path+'/total_time_normarea.png'
				title = "Normalized Total times [%]"
		else:
			filename = self.benchmarker.path+'/query_'+str(numQuery)+'_area.png'
		# save as plot
		self.save(
			dataframe = dataframePlot,
			title = title,
			subtitle = "",
			filename = filename)
		if self.normed:
			dataframe.loc['Mean']= dataframe.mean()
		else:
			dataframe.loc['Total']= dataframe.sum()
		return dataframe
	def generateAll(self, timer):
		"""
		Generates all reports for the benchmarker object for a given timer.
		If benchmarker has a fixed query, only reports for this query are generated.
		A plot of the total times is generated in any case.
		Anonymizes dbms if activated.

		:param timer: Timer object
		:return: returns nothing
		"""
		self.generate(numQuery=None, timer=timer)



class DEPRECATED_normarear(DEPRECATED_arear):
	"""
	Class for generating reports.
	Generates a plot of the benchmarks as a time series and saves it to disk.
	"""
	def __init__(self, benchmarker):
		arear.__init__(self, benchmarker)
		self.normed = True




class DEPRECATED_latexer(reporter):
	"""
	Class for generating reports.
	This class generates a survey in latex and saves it to disk.
	The survey has one page per timer.
	"""
	def __init__(self, benchmarker, templatefolder="simple"):
		reporter.__init__(self, benchmarker)
		self.template = {}
		self.templatefolder = templatefolder
		self.readTemplates(templatefolder)
		self.e = None
	def readTemplates(self, templatefolder):
		"""
		Reads content of a template folder

		:param timer: Timer object
		:return: returns nothing
		"""
		templates = ['reportHead', 'reportFoot', 'query', 'timer', 'appendix', 'appendixquery', 'appendixtimer']
		for t in templates:
			with open(os.path.dirname(__file__)+'/latex/'+templatefolder+"/"+t, 'r') as f:
				self.template[t]=f.read()
	def useTemplate(self, name, parameter):
		"""
		Applies parameter to a template and returns filled template

		:param name: Name of the template
		:param parameter: Dict of parameters
		:return: returns template with filled paramter
		"""
		logging.debug("useTemplate: "+self.templatefolder+"/"+name)
		return self.template[name].format(**parameter)
	def generateSummary(self):
		"""
		Generates data for the front page of latex report.

		:return: returns nothing
		"""
	def init(self):
		"""
		Generates and saves front page of latex report.

		:return: returns nothing
		"""
		# generate bar plot of total times
		reporterBar = barer(self.benchmarker)
		dfTotalSum = reporterBar.generate(numQuery=None, timer=self.benchmarker.timers, ensembler='sum')
		dfTotalProd = reporterBar.generate(numQuery=None, timer=self.benchmarker.timers, ensembler='product')
		# generate area plot of total time
		reporterArea = arear(self.benchmarker)
		dfTotalTime = reporterArea.generate(numQuery=None, timer=self.benchmarker.timers)
		if dfTotalTime is not None:
			print("Total Times")
			cols = ['ms']
			cols.extend(dfTotalTime.columns)
			print(tabulate(dfTotalTime,headers=cols,tablefmt="grid", floatfmt=".2f", showindex=True))
		# generate normed area plot of total time
		reporterNormArea = normarear(self.benchmarker)
		dfTotalTimeNorm = reporterNormArea.generate(numQuery=None, timer=self.benchmarker.timers)
		if dfTotalTimeNorm is not None:
			print("Total Times normed")
			cols = ['%']
			cols.extend(dfTotalTimeNorm.columns)
			print(tabulate(dfTotalTimeNorm,headers=cols,tablefmt="grid", floatfmt=".2f", showindex=True))
		# generate barh plot of total ranking
		dfTotalRank, timers = self.benchmarker.generateSortedTotalRanking()
		filename = self.benchmarker.path+'/total_barh_rank.png'
		title = 'Ranking of '+str(timers)+' timers'
		dfTotalRank.plot.barh()#color=[tools.dbms.dbmscolors.get(x, '#333333') for x in dfTotalRank.index])
		plt.xticks(fontsize=14)
		# align box to labels
		plt.tight_layout()
		# set title
		plt.legend(title=title)
		# save
		plt.savefig(filename)
		plt.close('all')
		#print(title)
		#print(tabulate(dfTotalRank,headers=dfTotalRank.columns,tablefmt="grid", floatfmt=".2f"))
		#if dfTotalSum is not None:
		#	dfTotalSum = dfTotalSum.applymap(lambda x: '{:.{prec}f}'.format(x, prec=2))
		if dfTotalProd is not None:
			dfTotalProd = dfTotalProd.applymap(lambda x: '{:.{prec}f}'.format(x, prec=2))
		dfTotalRank = dfTotalRank.applymap(lambda x: '{:.{prec}f}'.format(x, prec=2))
		# time for ingest
		timesLoad = {}
		for c,cd in self.benchmarker.dbms.items():
			if cd.connectiondata['active'] and 'timeLoad' in cd.connectiondata: 
				timesLoad[self.benchmarker.dbms[c].getName()] = cd.connectiondata['timeLoad']
		# Heatmap of normalized total times
		filename = self.benchmarker.path+'/total_heatmap_normalized.png'
		title = 'Heatmap of Normalized Total Times [%]'
		df = dfTotalTimeNorm
		fig = plt.figure(figsize = (10,12))
		plt.imshow(df, cmap="Reds", aspect='auto')
		#plt.imshow(df, cmap="YlOrRd", aspect='auto')
		#plt.imshow(df, cmap="Greys", aspect='auto')
		plt.colorbar()
		plt.xticks(range(len(df.columns)),df.columns, rotation=90)
		plt.yticks(range(len(df)),df.index)
		ax = plt.gca()
		ax.set_title(title)
		data = df.values
		max_cell = np.nanmax(df.values)
		min_cell = np.nanmin(df.values)
		for y in range(data.shape[0]):
			for x in range(data.shape[1]):
				color_cell = 'white' if data[y, x] > (min_cell+max_cell)/2 else 'black'
				plt.text(x, y, '%.2f' % data[y, x],
					horizontalalignment='center',
					verticalalignment='center',
					color=color_cell, fontsize=8)
		ax.set_xticks(np.arange(len(df.columns)+1)-.5, minor=True)
		ax.set_yticks(np.arange(len(df.index)+1)-.5, minor=True)
		ax.grid(which="minor", color="black", linestyle='-', linewidth=1)
		ax.tick_params(which="minor", bottom=False, left=False)
		plt.tight_layout()
		# set title
		plt.title(title)
		# save
		plt.savefig(filename, bbox_inches='tight')
		plt.close('all')
		# store in parameter array for replacement in templates
		parameter = self.prepare(0, 0)
		#if len(evaluator.evaluator.evaluation) == 0:
		if self.e is None:
			self.e = evaluator.evaluator(self.benchmarker)
		#evaluation = evaluator.evaluator.evaluation
		evaluation = self.e.get_evaluation()
		# Heatmaps of timers
		for i, t in enumerate(self.benchmarker.timers):
			filename = self.benchmarker.path+'/total_heatmap_timer_'+t.name+'.png'
			title = 'Heatmap of factors of timer '+t.name
			df = tools.dataframehelper.evaluateTimerfactorsToDataFrame(evaluation, t)
			#print(t.name)
			#print(df)
			df = df[(df.T != "0.00").any()]
			df.fillna(value=pd.np.nan, inplace=True)
			#print(df)
			#df = df[(df.T[0:] != 0).any()]
			if df.empty:
				continue
			fig = plt.figure(figsize = (10,12))
			#print(df)
			plt.imshow(df, cmap="Reds", aspect='auto')
			plt.colorbar()
			plt.xticks(range(len(df.columns)),df.columns, rotation=90)
			plt.yticks(range(len(df)),df.index)
			ax = plt.gca()
			data = df.values
			max_cell = np.nanmax(df.values)
			min_cell = np.nanmin(df.values)
			for y in range(data.shape[0]):
				for x in range(data.shape[1]):
					color_cell = 'white' if data[y, x] > (min_cell+max_cell)/2 else 'black'
					plt.text(x, y, '%.2f' % data[y, x],
						horizontalalignment='center',
						verticalalignment='center',
						color=color_cell, fontsize=8)
			ax.set_xticks(np.arange(len(df.columns)+1)-.5, minor=True)
			ax.set_yticks(np.arange(len(df.index)+1)-.5, minor=True)
			ax.grid(which="minor", color="black", linestyle='-', linewidth=1)
			ax.tick_params(which="minor", bottom=False, left=False)
			plt.tight_layout()
			#plt.show()
			# set title
			plt.title(title)
			# save
			plt.savefig(filename, bbox_inches='tight')
			plt.close('all')
		# Heatmaps of tps
		filename = self.benchmarker.path+'/total_heatmap_tps.png'
		title = 'Heatmap of Throughputs [Hz]'
		df = tools.dataframehelper.evaluateTPSToDataFrame(evaluation)
		fig = plt.figure(figsize = (10,12))
		plt.imshow(df, cmap="Reds_r", aspect='auto')
		plt.colorbar()
		plt.xticks(range(len(df.columns)),df.columns, rotation=90)
		plt.yticks(range(len(df)),df.index)
		ax = plt.gca()
		data = df.values
		max_cell = np.nanmax(df.values)
		min_cell = np.nanmin(df.values)
		for y in range(data.shape[0]):
			for x in range(data.shape[1]):
				color_cell = 'black' if data[y, x] > (min_cell+max_cell)/2 else 'white'
				if np.isnan(data[y, x]):
					color_cell = 'black'
				plt.text(x, y, '%.2f' % data[y, x],
					horizontalalignment='center',
					verticalalignment='center',
					color=color_cell, fontsize=8)
		ax.set_xticks(np.arange(len(df.columns)+1)-.5, minor=True)
		ax.set_yticks(np.arange(len(df.index)+1)-.5, minor=True)
		ax.grid(which="minor", color="black", linestyle='-', linewidth=1)
		ax.tick_params(which="minor", bottom=False, left=False)
		plt.tight_layout()
		# set title
		plt.title(title)
		# save
		plt.savefig(filename, bbox_inches='tight')
		plt.close('all')
		# Heatmaps of latency
		filename = self.benchmarker.path+'/total_heatmap_lat.png'
		title = 'Heatmap of Latencies [s]'
		df = tools.dataframehelper.evaluateLatToDataFrame(evaluation)
		fig = plt.figure(figsize = (10,12))
		plt.imshow(df, cmap="Reds", aspect='auto')
		plt.colorbar()
		plt.xticks(range(len(df.columns)),df.columns, rotation=90)
		plt.yticks(range(len(df)),df.index)
		ax = plt.gca()
		data = df.values
		max_cell = np.nanmax(df.values)
		min_cell = np.nanmin(df.values)
		for y in range(data.shape[0]):
			for x in range(data.shape[1]):
				color_cell = 'white' if data[y, x] > (min_cell+max_cell)/2 else 'black'
				plt.text(x, y, '%.2f' % data[y, x],
					horizontalalignment='center',
					verticalalignment='center',
					color=color_cell, fontsize=8)
		ax.set_xticks(np.arange(len(df.columns)+1)-.5, minor=True)
		ax.set_yticks(np.arange(len(df.index)+1)-.5, minor=True)
		ax.grid(which="minor", color="black", linestyle='-', linewidth=1)
		ax.tick_params(which="minor", bottom=False, left=False)
		plt.tight_layout()
		# set title
		plt.title(title)
		# save
		plt.savefig(filename, bbox_inches='tight')
		plt.close('all')
		# Heatmaps of queuesize
		filename = self.benchmarker.path+'/total_heatmap_queuesize.png'
		title = 'Heatmap of Queue Sizes'
		df = tools.dataframehelper.evaluateQueuesizeToDataFrame(evaluation)
		fig = plt.figure(figsize = (10,12))
		plt.imshow(df, cmap="Reds_r", aspect='auto')
		plt.colorbar()
		plt.xticks(range(len(df.columns)),df.columns, rotation=90)
		plt.yticks(range(len(df)),df.index)
		ax = plt.gca()
		data = df.values
		max_cell = np.nanmax(df.values)
		min_cell = np.nanmin(df.values)
		for y in range(data.shape[0]):
			for x in range(data.shape[1]):
				color_cell = 'black' if data[y, x] > (min_cell+max_cell)/2 else 'white'
				plt.text(x, y, '%.2f' % data[y, x],
					horizontalalignment='center',
					verticalalignment='center',
					color=color_cell, fontsize=8)
		ax.set_xticks(np.arange(len(df.columns)+1)-.5, minor=True)
		ax.set_yticks(np.arange(len(df.index)+1)-.5, minor=True)
		ax.grid(which="minor", color="black", linestyle='-', linewidth=1)
		ax.tick_params(which="minor", bottom=False, left=False)
		plt.tight_layout()
		# set title
		plt.title(title)
		# save
		plt.savefig(filename, bbox_inches='tight')
		plt.close('all')
		# Heatmaps of result size
		filename = self.benchmarker.path+'/total_heatmap_resultsize.png'
		title = 'Heatmap of Size of Resultsets [%]'
		df = tools.dataframehelper.evaluateNormalizedResultsizeToDataFrame(evaluation)
		fig = plt.figure(figsize = (10,12))
		plt.imshow(df, cmap="Reds", aspect='auto')
		plt.colorbar()
		plt.xticks(range(len(df.columns)),df.columns, rotation=90)
		plt.yticks(range(len(df)),df.index)
		ax = plt.gca()
		data = df.values
		max_cell = np.nanmax(df.values)
		min_cell = np.nanmin(df.values)
		for y in range(data.shape[0]):
			for x in range(data.shape[1]):
				color_cell = 'white' if data[y, x] > (min_cell+max_cell)/2 else 'black'
				plt.text(x, y, '%.2f' % data[y, x],
					horizontalalignment='center',
					verticalalignment='center',
					color=color_cell, fontsize=8)
		ax.set_xticks(np.arange(len(df.columns)+1)-.5, minor=True)
		ax.set_yticks(np.arange(len(df.index)+1)-.5, minor=True)
		ax.grid(which="minor", color="black", linestyle='-', linewidth=1)
		ax.tick_params(which="minor", bottom=False, left=False)
		plt.tight_layout()
		# set title
		plt.title(title)
		# save
		plt.savefig(filename, bbox_inches='tight')
		plt.close('all')
		# Heatmaps of errors
		filename = self.benchmarker.path+'/total_heatmap_errors.png'
		title = 'Heatmap of Errors'
		df = tools.dataframehelper.evaluateErrorsToDataFrame(evaluation)
		#df = df[((df is not None) & (df.notnull()))]
		df = df.dropna().astype(int)
		#print(df)
		fig = plt.figure(figsize = (10,12))
		plt.imshow(df, cmap="Reds", aspect='auto')
		#plt.colorbar()
		plt.xticks(range(len(df.columns)),df.columns, rotation=90)
		plt.yticks(range(len(df)),df.index)
		ax = plt.gca()
		data = df.values
		max_cell = np.nanmax(df.values)
		min_cell = np.nanmin(df.values)
		for y in range(data.shape[0]):
			for x in range(data.shape[1]):
				color_cell = 'white' if data[y, x] > (min_cell+max_cell)/2 else 'black'
				plt.text(x, y, 'E' if data[y,x] else '',
					horizontalalignment='center',
					verticalalignment='center',
					color=color_cell, fontsize=8)
		ax.set_xticks(np.arange(len(df.columns)+1)-.5, minor=True)
		ax.set_yticks(np.arange(len(df.index)+1)-.5, minor=True)
		ax.grid(which="minor", color="black", linestyle='-', linewidth=1)
		ax.tick_params(which="minor", bottom=False, left=False)
		plt.tight_layout()
		# set title
		plt.title(title)
		# save
		plt.savefig(filename, bbox_inches='tight')
		plt.close('all')
		# Heatmaps of warnings
		filename = self.benchmarker.path+'/total_heatmap_warnings.png'
		title = 'Heatmap of Warnings'
		df = tools.dataframehelper.evaluateWarningsToDataFrame(evaluation)
		df = df.dropna().astype(int)
		#df = df[((df is not None) & (df.notnull()))]
		fig = plt.figure(figsize = (10,12))
		plt.imshow(df, cmap="Reds", aspect='auto')
		#plt.colorbar()
		plt.xticks(range(len(df.columns)),df.columns, rotation=90)
		plt.yticks(range(len(df)),df.index)
		ax = plt.gca()
		data = df.values
		max_cell = np.nanmax(df.values)
		min_cell = np.nanmin(df.values)
		for y in range(data.shape[0]):
			for x in range(data.shape[1]):
				color_cell = 'white' if data[y, x] > (min_cell+max_cell)/2 else 'black'
				plt.text(x, y, 'W' if data[y,x] else '',
					horizontalalignment='center',
					verticalalignment='center',
					color=color_cell, fontsize=8)
		ax.set_xticks(np.arange(len(df.columns)+1)-.5, minor=True)
		ax.set_yticks(np.arange(len(df.index)+1)-.5, minor=True)
		ax.grid(which="minor", color="black", linestyle='-', linewidth=1)
		ax.tick_params(which="minor", bottom=False, left=False)
		plt.tight_layout()
		# set title
		plt.title(title)
		# save
		plt.savefig(filename, bbox_inches='tight')
		plt.close('all')
		# Monitoring
		metrics = [True if 'hardwaremetrics' in d else False for c,d in evaluation['dbms'].items()]
		settings_translate = {
			'total_cpu_memory':'RAM [MiB]',
			'total_cpu_memory_cached':'Cached [MiB]',
			'total_cpu_util':'CPU [%]',
			'total_gpu_util':'GPU [%]',
			'total_gpu_power':'GPU [W]',
			'total_gpu_memory':'VRAM [MiB]',
			'total_gpu_energy':'GPU [Wh]',
			'RAM':'RAM [MiB]',
			'disk':'Disk [MiB]',
			'datadisk':'Data [MiB]',
			'benchmark_usd':'Cost [$]',
			'benchmark_time_s':'Benchmark [s]',
			'total_time_s':'Total [s]'}
		if True in metrics:
			dfTotalHardware = tools.dataframehelper.evaluateMonitoringToDataFrame(evaluation)
			#dfTotalHardware.index = dfTotalHardware.index.map(tools.dbms.anonymizer)
			dfTotalHardware_units = dfTotalHardware.rename(columns = settings_translate)
			#print(dfTotalHardware_units)
			parameter['totalHardwareMonitoring'] = tabulate(dfTotalHardware_units, headers=dfTotalHardware_units.columns, tablefmt="latex", floatfmt=",.2f", stralign="right", showindex=True)
		else:
			parameter['totalHardwareMonitoring'] = ""
			#parameter['totalHardwareHost'] = ""
		# Hardware
		dfTotalHardware = tools.dataframehelper.evaluateHostToDataFrame(evaluation)
		#dfTotalHardware.index = dfTotalHardware.index.map(tools.dbms.anonymizer)
		dfTotalHardware_units = dfTotalHardware.rename(columns = settings_translate)
		parameter['totalHardwareHost'] = tabulate(dfTotalHardware_units, headers=dfTotalHardware_units.columns, tablefmt="latex", floatfmt=",.2f", stralign="right", showindex=True)
		# TPS / Latency
		dfTotalLatTPS = pd.DataFrame.from_dict({c:{m:metric for m,metric in dbms['metrics'].items()} for c,dbms in evaluation['dbms'].items()}).transpose()
		#print(dfTotalLatTPS)
		dfTotalLatTPS = dfTotalLatTPS.drop(columns=['totaltime_ms','throughput_run_total_ph','throughput_session_total_ph','throughput_session_mean_ph','queuesize_run', 'queuesize_session'])
		#print(dfTotalLatTPS)
		#dfTotalLatTPS = dfTotalLatTPS.reindex(sorted(dfTotalLatTPS.columns), axis=1)
		dfTotalLatTPS = dfTotalLatTPS.reindex(['throughput_run_total_ps','throughput_run_mean_ps','throughput_session_total_ps','throughput_session_mean_ps','latency_run_mean_ms','latency_session_mean_ms','throughput_run_mean_ph', 'queuesize_run_percent', 'queuesize_session_percent'], axis=1)
		#print(dfTotalLatTPS)
		dfTotalLatTPS.index = dfTotalLatTPS.index.map(tools.dbms.anonymizer)
		dfTotalLatTPS = dfTotalLatTPS.reindex(sorted(dfTotalLatTPS.index), axis=0)
		#print(dfTotalLatTPS)
		settings_translate = {
			'latency_run_mean_ms':'lat_r [ms]',
			'latency_session_mean_ms':'lat_s [ms]',
			'throughput_run_total_ps':'tps_r1 [Hz]',
			'throughput_run_mean_ps':'tps_r2 [Hz]',
			'throughput_session_total_ps':'tps_s1 [Hz]',
			'throughput_session_mean_ps':'tps_s2 [Hz]',
			'throughput_run_total_ph':'tph_r1 [ph]',
			'throughput_run_mean_ph':'tph_r2 [ph]',
			'throughput_session_total_ph':'tph_s1 [ph]',
			'throughput_session_mean_ph':'tph_s2 [ph]',
			'totaltime_ms':'total [s]',
			'queuesize_run':'qs_r',
			'queuesize_session':'qs_s',
			'queuesize_run_percent':'qs_r [%]',
			'queuesize_session_percent':'qs_s [%]',
		}
		dfTotalLatTPS_units = dfTotalLatTPS.rename(columns = settings_translate)
		settings_translate = {
			'latency_run_mean_ms':'lat_r',
			'latency_session_mean_ms':'lat_s',
			'throughput_run_total_ps':'tps_r1',
			'throughput_run_mean_ps':'tps_r2',
			'throughput_session_total_ps':'tps_s1',
			'throughput_session_mean_ps':'tps_s2',
			'throughput_run_total_ph':'tph_r1',
			'throughput_run_mean_ph':'tph_r2',
			'throughput_session_total_ph':'tph_s1',
			'throughput_session_mean_ph':'tph_s2',
		}
		dfTotalLatTPS = dfTotalLatTPS.rename(columns = settings_translate)
		#print(dfTotalLatTPS)
		plt.figure()
		df1 = dfTotalLatTPS[['tps_r1','tps_r2','tps_s1','tps_s2']].transpose()
		df1.plot.bar(title="Throughputs [Hz]", color=[tools.dbms.dbmscolors.get(x, '#333333') for x in df1.columns])
		# rotate labels
		plt.xticks(rotation=70, fontsize=12)
		# align box to labels
		plt.tight_layout()
		plt.legend(title="DBMS")
		filename = self.benchmarker.path+'/total_bar_tps.png'
		plt.savefig(filename)
		plt.close('all')
		plt.figure()
		df2 = dfTotalLatTPS[['lat_r','lat_s']].transpose()
		df2.plot.bar(title="Latencies [ms]", color=[tools.dbms.dbmscolors.get(x, '#333333') for x in df2.columns])
		# rotate labels
		plt.xticks(rotation=70, fontsize=12)
		# align box to labels
		plt.tight_layout()
		plt.legend(title="DBMS")
		filename = self.benchmarker.path+'/total_bar_lat.png'
		plt.savefig(filename)
		plt.close('all')
		#print(tabulate(dfTotalLatTPS,headers=dfTotalLatTPS.columns,tablefmt="grid", floatfmt=".2f"))
		dfTotalLatTPS_units = tools.dataframehelper.addStatistics(dfTotalLatTPS_units)
		parameter['totalLatTPS'] = tabulate(dfTotalLatTPS_units, headers=dfTotalLatTPS_units.columns, tablefmt="latex", floatfmt=",.2f", stralign="right", showindex=True)
		if dfTotalSum is not None:
			dfTotalSum = tools.dataframehelper.addStatistics(dfTotalSum)
			dfTotalSum = dfTotalSum.applymap(lambda x: '{:.{prec}f}'.format(x, prec=2))
			parameter['totalSum'] = tabulate(dfTotalSum, headers=dfTotalSum.columns, tablefmt="latex", floatfmt=",.2f", stralign="right", showindex=True)
		else:
			parameter['totalSum'] = ""
		if dfTotalProd is not None:
			parameter['totalProd'] = tabulate(dfTotalProd, headers=dfTotalProd.columns, tablefmt="latex", floatfmt=",.2f", stralign="right", showindex=True)
		else:
			parameter['totalProd'] = ""
		parameter['totalRank'] = tabulate(dfTotalRank, headers=dfTotalRank.columns, tablefmt="latex", floatfmt=",.2f", stralign="right", showindex=True)
		parameter['totalTime'] = ""
		if dfTotalTime is not None:
			listofnames = ['DBMS '+str(l+1) for l in range(len(dfTotalTime.columns))]
			maxRows = 25-len(listofnames)
			dfTotalTimeTranslation = pd.DataFrame(dfTotalTime.columns,index=listofnames,columns=['DBMS Name'])
			dfTotalTime.columns = listofnames
			cols = ['[ms]']
			cols.extend(dfTotalTime.columns)
			if len(dfTotalTime.index) > maxRows:
				# more than max rows, too long for reporting
				#dfTotalTime = dfTotalTime.iloc[len(dfTotalTime.index)-maxRows:]
				#dfTotalTime = dfTotalTime.applymap(lambda s: f'{s:,.2f}')
				#dfTotalTime.iloc[0]=['...']*len(dfTotalTime.columns)
				parameter['totalTime'] += "\\caption{DBMS: Total times}\\end{figure}\\newpage\\begin{figure}[!htbp]\\centering"
			parameter['totalTime'] += "{\\scriptsize{"
			parameter['totalTime'] += tabulate(dfTotalTime, headers=cols, tablefmt="latex", floatfmt=",.2f", stralign="right", showindex=True)
			parameter['totalTime'] += "\\\\"+tabulate(dfTotalTimeTranslation, headers=dfTotalTimeTranslation.columns, tablefmt="latex", floatfmt=",.2f", stralign="right", showindex=True)
			parameter['totalTime'] += "}}"
		parameter['totalTimeNormed'] = ""
		if dfTotalTimeNorm is not None:
			listofnames = ['DBMS '+str(l+1) for l in range(len(dfTotalTimeNorm.columns))]
			maxRows = 25-len(listofnames)
			dfTotalTimeTranslation = pd.DataFrame(dfTotalTimeNorm.columns,index=listofnames,columns=['DBMS Name'])
			dfTotalTimeNorm.columns = listofnames
			cols = ['[%]']
			cols.extend(dfTotalTimeNorm.columns)
			if len(dfTotalTimeNorm.index) > maxRows:
				# more than max rows, too long for reporting
				#dfTotalTimeNorm = dfTotalTimeNorm.iloc[len(dfTotalTimeNorm.index)-maxRows:]
				#dfTotalTimeNorm = dfTotalTimeNorm.applymap(lambda s: f'{s:,.2f}')
				#dfTotalTimeNorm.iloc[0]=['...']*len(dfTotalTimeNorm.columns)
				parameter['totalTimeNormed'] += "\\caption{DBMS: Total times normalized}\\end{figure}\\newpage\\begin{figure}[!htbp]\\centering"
			parameter['totalTimeNormed'] += "{\\scriptsize{"
			parameter['totalTimeNormed'] += tabulate(dfTotalTimeNorm, headers=cols, tablefmt="latex", floatfmt=",.2f", stralign="right", showindex=True)
			parameter['totalTimeNormed'] += "\\\\"+tabulate(dfTotalTimeTranslation, headers=dfTotalTimeTranslation.columns, tablefmt="latex", floatfmt=",.2f", stralign="right", showindex=True)
			parameter['totalTimeNormed'] += "}}"
		#print(timesLoad)
		if len(timesLoad) > 0:
			dfIngest = pd.DataFrame.from_dict(timesLoad, orient='index')
			dfIngest.columns=['Time of Ingestion [s]']
			dfIngest.sort_values(by=['Time of Ingestion [s]'], inplace=True, ascending=False)
			filename = self.benchmarker.path+'/total_barh_ingest.png'
			title = 'Ingest into '+str(len(dfIngest.index))+' DBMS'
			dfIngest.plot.barh()#color=[tools.dbms.dbmscolors.get(x, '#333333') for x in dfIngest.index])
			plt.xticks(fontsize=14)
			# align box to labels
			plt.tight_layout()
			# set title
			plt.legend(title=title)
			# save
			plt.savefig(filename)
			plt.close('all')
			parameter['totalIngest'] = tabulate(dfIngest, headers=dfIngest.columns, tablefmt="latex", floatfmt=",.2f", stralign="right", showindex=True)
		else:
			parameter['totalIngest'] = ""
		# generate report head
		latex_output = self.useTemplate('reportHead', parameter)# self.template['reportHead'].format(**parameter)
		latex_file = open(self.benchmarker.path+"/benchmarks.tex", "w")
		latex_file.write(latex_output)
		latex_file.close()
	def prepare(self, numQuery=0, numTimer=0, timer=None):
		"""
		Prepares a dict containing data about query and timer.
		This will be used to fill the latex output template.
		Anonymizes dbms if activated.

		:param numQuery: Number of query to collect data from. numQuery=0 for title page
		:param numTimer: Number of timer of this query. First timer is treated differently (subsubsection)
		:param timer: List of timers to collect data from. timer=None for title page
		:return: returns dict of data about query and timer
		"""
		#if len(evaluator.evaluator.evaluation) == 0:
		if self.e is None:
			self.e = evaluator.evaluator(self.benchmarker)
		#evaluation = evaluator.evaluator.evaluation
		evaluation = self.e.get_evaluation()
		result = {}
		result['now'] = evaluation['general']['now']
		result['path'] = evaluation['general']['path']
		result['code'] = evaluation['general']['code']
		result['timeout'] = evaluation['general']['connectionmanagement']['timeout']
		result['numProcesses'] = evaluation['general']['connectionmanagement']['numProcesses']
		result['runsPerConnection'] = evaluation['general']['connectionmanagement']['numProcesses']
		result['intro'] = ''
		result['info'] = ''
		if len(evaluation['general']['intro']) > 0:
			result['intro'] = evaluation['general']['intro'] + "\\\\"
		if len(evaluation['general']['info']) > 0:
			result['info'] = evaluation['general']['info'] + "\\\\"
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
		#print(times)
		# format dbms infos
		def initfilename(c, i):
			return self.benchmarker.path+'/'+c+'_init_'+str(i)+'.log'
		def hasInitScript(c):
			return os.path.isfile(initfilename(c,0))
		dbmsinfos = ""
		#for c in sorted(self.benchmarker.dbms.keys()):
		for c in sorted(self.benchmarker.dbms, key=lambda kv: self.benchmarker.dbms[kv].name):
			if self.benchmarker.dbms[c].connectiondata['active']:
				dbmsdata = self.benchmarker.dbms[c].getName()
				if not self.benchmarker.anonymize:
					dbmsdata += " ("+self.benchmarker.dbms[c].connectiondata["version"]+")"
				dbmsinfos += "\\subsection{{{dbmsname}}}".format(dbmsname=dbmsdata)
				dbmsinfos += "\\begin{itemize}\n"
				info = self.benchmarker.dbms[c].connectiondata["info"]
				if len(info) > 0:
					if not str(info) == info:
						dbmsinfos += "\\item "+("\\item ".join(info))
				if 'connectionmanagement' in self.benchmarker.dbms[c].connectiondata:
					# settings of connection
					connectionmanagement = self.benchmarker.dbms[c].connectiondata['connectionmanagement']
					if "numProcesses" in connectionmanagement:
						dbmsinfos += "\\item \\textbf{Parallel Clients}: "+str(connectionmanagement["numProcesses"])
					if "runsPerConnection" in connectionmanagement:
						if connectionmanagement["runsPerConnection"] is not None and connectionmanagement["runsPerConnection"] > 0:
							dbmsinfos += "\\item \\textbf{Runs per Connection}: "+str(connectionmanagement["runsPerConnection"])
						else:
							dbmsinfos += "\\item \\textbf{Runs per Connection}: Unlimited"
					if "timeout" in connectionmanagement:
						if connectionmanagement["timeout"] is not None and connectionmanagement["timeout"] > 0:
							dbmsinfos += "\\item \\textbf{Timeout}: "+str(connectionmanagement["timeout"])
						else:
							dbmsinfos += "\\item \\textbf{Timeout}: Unlimited"
				else:
					# global setting
					connectionmanagement = self.benchmarker.connectionmanagement
					if "numProcesses" in connectionmanagement:
						dbmsinfos += "\\item \\textbf{Parallel Clients}: "+str(connectionmanagement["numProcesses"])
					if "runsPerConnection" in connectionmanagement:
						if connectionmanagement["runsPerConnection"] is not None and connectionmanagement["runsPerConnection"] > 0:
							dbmsinfos += "\\item \\textbf{Runs per Connection}: "+str(connectionmanagement["runsPerConnection"])
						else:
							dbmsinfos += "\\item \\textbf{Runs per Connection}: Unlimited"
					if "timeout" in connectionmanagement:
						if connectionmanagement["timeout"] is not None and connectionmanagement["timeout"] > 0:
							dbmsinfos += "\\item \\textbf{Timeout}: "+str(connectionmanagement["timeout"])
						else:
							dbmsinfos += "\\item \\textbf{Timeout}: Unlimited"
				if "hostsystem" in self.benchmarker.dbms[c].connectiondata:
					if "CPU" in self.benchmarker.dbms[c].connectiondata["hostsystem"]:
						dbmsinfos += "\\item \\textbf{CPU}: "+self.benchmarker.dbms[c].connectiondata["hostsystem"]["CPU"]
					if "Cores" in self.benchmarker.dbms[c].connectiondata["hostsystem"]:
						dbmsinfos += "\\item \\textbf{Cores}: "+str(self.benchmarker.dbms[c].connectiondata["hostsystem"]["Cores"])
					if "RAM" in self.benchmarker.dbms[c].connectiondata["hostsystem"]:
						dbmsinfos += "\\item \\textbf{RAM}: "+tools.sizeof_fmt(self.benchmarker.dbms[c].connectiondata["hostsystem"]["RAM"])
					if "GPU" in self.benchmarker.dbms[c].connectiondata["hostsystem"] and len(self.benchmarker.dbms[c].connectiondata["hostsystem"]["GPU"]) > 0:
						dbmsinfos += "\\item \\textbf{GPU}: "+self.benchmarker.dbms[c].connectiondata["hostsystem"]["GPU"]
					if "CUDA" in self.benchmarker.dbms[c].connectiondata["hostsystem"]:
						dbmsinfos += "\\item \\textbf{CUDA}: "+self.benchmarker.dbms[c].connectiondata["hostsystem"]["CUDA"]
					if "host" in self.benchmarker.dbms[c].connectiondata["hostsystem"]:
						dbmsinfos += "\\item \\textbf{Host}: "+self.benchmarker.dbms[c].connectiondata["hostsystem"]["host"]
					if "node" in self.benchmarker.dbms[c].connectiondata["hostsystem"]:
						dbmsinfos += "\\item \\textbf{Node}: "+self.benchmarker.dbms[c].connectiondata["hostsystem"]["node"]
					if "disk" in self.benchmarker.dbms[c].connectiondata["hostsystem"]:
						dbmsinfos += "\\item \\textbf{Docker Disk Space used}: "+tools.sizeof_fmt(int(self.benchmarker.dbms[c].connectiondata["hostsystem"]["disk"])*1024)
					if "datadisk" in self.benchmarker.dbms[c].connectiondata["hostsystem"]:
						dbmsinfos += "\\item \\textbf{Docker Disk Space used for Data}: "+tools.sizeof_fmt(int(self.benchmarker.dbms[c].connectiondata["hostsystem"]["datadisk"])*1024)
					if "instance" in self.benchmarker.dbms[c].connectiondata["hostsystem"]:
						dbmsinfos += "\\item \\textbf{Instance}: "+self.benchmarker.dbms[c].connectiondata["hostsystem"]["instance"]
					if "timeLoad" in self.benchmarker.dbms[c].connectiondata:
						dbmsinfos += "\\item \\textbf{Time Ingest}: "+tools.formatDuration(self.benchmarker.dbms[c].connectiondata["timeLoad"]*1000.0)
				if c in times:
					dbmsinfos += "\\item \\textbf{Time Benchmarks}: "+tools.formatDuration(times[c])
					if 'priceperhourdollar' in self.benchmarker.dbms[c].connectiondata:
						if "timeLoad" in self.benchmarker.dbms[c].connectiondata:
							time = times[c] + self.benchmarker.dbms[c].connectiondata["timeLoad"]*1000.0
							dbmsinfos += "\\item \\textbf{Time Total}: "+tools.formatDuration(time)
						else:
							time = times[c]
						dbmsinfos += "\\item \\textbf{{Price}}: \\${:.{prec}f} (\\${:.{prec}f}/h)".format(self.benchmarker.dbms[c].connectiondata['priceperhourdollar']*time/3600000, self.benchmarker.dbms[c].connectiondata['priceperhourdollar'], prec=2)
				if self.benchmarker.dbms[c].hasHardwareMetrics():
					#logging.debug("Hardware metrics for Q"+str(numQuery))
					metricsReporter = monitor.metrics(self.benchmarker)
					hardwareAverages = metricsReporter.computeAverages()
					#print(hardwareAverages)
					#print(times)
					for m, avg in hardwareAverages[c].items():
						if avg > 0:
							dbmsinfos += "\\item \\textbf{Average "+tools.tex_escape(monitor.metrics.metrics[m]['title'])+"}: "+"{:.{prec}f}".format(avg, prec=2)
					if 'total_gpu_power' in hardwareAverages[c] and hardwareAverages[c]['total_gpu_power']*times[c] > 0:
						# basis: per second average power, total time in ms
						dbmsinfos += "\\item \\textbf{GPU Energy Consumption [Wh]}: "+"{:.{prec}f}".format(hardwareAverages[c]['total_gpu_power']*times[c]/3600000, prec=2)
				if hasInitScript(c) and not self.benchmarker.dbms[c].anonymous:
					dbmsinfos += "\\item\\hyperref[initscript:{dbms}]{{Initialisation scripts}}".format(dbms=c)
				#dbmsinfos += "\\item {}\n".format(info)
				#dbmsinfos += "\\item \\textbf{{{}}}: {}\n".format(dbmsdata, info)
				dbmsinfos += "\\end{itemize}"
		result['dbmsinfos'] = dbmsinfos
		# appendix start: query survey
		result['querySurvey'] = ''
		for i in range(1, len(self.benchmarker.queries)+1):
			queryObject = tools.query(self.benchmarker.queries[i-1])
			result['querySurvey'] += "\n\\\\\\noindent\\hyperref[benchmark:{code}-Q{queryNumber}]{{\\textbf{{Q{queryNumber}: {queryTitle}}}}}\n".format(code=self.benchmarker.code, queryNumber=i, queryTitle=queryObject.title)
			if not queryObject.active:
				result['querySurvey'] += '\\\\inactive'
				continue
			if len(self.benchmarker.protocol['query'][str(i)]['parameter']) > 0:
				result['querySurvey'] += "\\\\\\hyperref[parameter:{code}-Q{queryNumber}]{{Parametrized}}".format(**result, queryNumber=i)
			l = self.benchmarker.protocol['query'][str(i)]['dataStorage']
			if len(l) > 0 and len(l[0]) > 0 and len(l[0][0]) > 0:
				l = [x for l1 in l for l2 in l1 for x in l2]
				result['querySurvey'] += "\\\\\\hyperref[data:{code}-Q{queryNumber}]{{Storage size}}: ".format(**result, queryNumber=i)+str(sys.getsizeof(l))+" bytes ("+queryObject.result+")"
			if 'errors' in self.benchmarker.protocol['query'][str(i)]:
				bFoundErrors = False
				for connection, error in self.benchmarker.protocol['query'][str(i)]['errors'].items():
					if len(error) > 0 and self.benchmarker.dbms[connection].connectiondata['active']:
						if not bFoundErrors:
							result['querySurvey'] += "\\\\\\noindent Errors:"
						bFoundErrors = True
						result['querySurvey'] += "\\\\\\noindent "+self.benchmarker.dbms[connection].getName()+": {\\textit{\\error{"+tools.tex_escape(error)+"}}}\n"
						#result['querySurvey'] += "\\\\\\noindent "+connection+": {\\tiny{\\begin{verbatim}"+error+"\\end{verbatim}}}\n"
						#result['querySurvey'] += "\\\\"+connection+": Error"
			if 'warnings' in self.benchmarker.protocol['query'][str(i)]:
				bFoundWarnings = False
				for connection, warning in self.benchmarker.protocol['query'][str(i)]['warnings'].items():
					if len(warning) > 0 and self.benchmarker.dbms[connection].connectiondata['active']:
						if not bFoundWarnings:
							result['querySurvey'] += "\\\\\\noindent Warnings:"
						bFoundWarnings = True
						result['querySurvey'] += "\\\\\\noindent "+self.benchmarker.dbms[connection].getName()+": {\\textit{\\error{"+tools.tex_escape(warning)+"}}}\n"
						#result['querySurvey'] += "\\\\\\noindent "+connection+": {\\tiny{\\begin{verbatim}"+error+"\\end{verbatim}}}\n"
						#result['querySurvey'] += "\\\\"+connection+": Error"
			for connection, size in self.benchmarker.protocol['query'][str(i)]['sizes'].items():
				if size > 0 and self.benchmarker.dbms[connection].connectiondata['active']:
					result['querySurvey'] += "\\\\\\noindent "+self.benchmarker.dbms[connection].getName()+": Received data = "+tools.sizeof_fmt(size)+"\n"
					#result['querySurvey'] += "\\\\"+connection+": Error"
		#print(result['querySurvey'])
		# format title of benchmark
		if len(self.benchmarker.queryconfig["name"]) > 0:
			benchmarkName = self.benchmarker.queryconfig["name"]
		else:
			benchmarkName = self.benchmarker.path
		# if no query: title is section
		if numQuery == 0:
			benchmarkName = '\\section{%s}' % (benchmarkName)
		result['benchmarkName'] = benchmarkName
		summary = ""
		numShownPlots = 0
		for i,q in enumerate(self.benchmarker.queries):
			if self.benchmarker.timerExecution.checkForSuccessfulBenchmarks(i+1):
				query = tools.query(q)
				if not query.active:
					#print("skip")
					continue
				numShownPlots = numShownPlots + 1
				summary += """\\begin{{minipage}}[ht]{{0.45\\textwidth}}
\\hyperref[benchmark:{code}-Q{queryNumber}]{{\\textbf{{Q{queryNumber}: {queryTitle}}}}}\\\\
\\includegraphics[width=0.9\\textwidth]{{query_{queryNumber}_bar.png}}
\\end{{minipage}}\n""".format(code=self.benchmarker.code, path=self.benchmarker.path, queryNumber=i+1, queryTitle=query.title)
				if i+1 < len(self.benchmarker.queries) and (numShownPlots)%2:
					summary += "\\hfill"
		result['summary'] = summary
		# init scripts
		result['initscript'] = ""
		for c in self.benchmarker.dbms:
			i = 0
			if hasInitScript(c) and self.benchmarker.dbms[c].connectiondata['active'] and not self.benchmarker.dbms[c].anonymous:
				result['initscript'] += "\\newpage\n\\subsection{{Initscript {dbms}}}\\label{{initscript:{dbms}}}\n".format(dbms=c)
				#result['initscript'] += "\\begin{figure}[h]\\centering\n
				result['initscript'] += "\\begin{sqlFormatSmall}\n"
				while True:
					filename=initfilename(c,i)
					if os.path.isfile(filename):
						result['initscript'] += open(filename).read()
						i = i + 1
					else:
						break
				result['initscript'] += "\n\\end{sqlFormatSmall}\n"
				#result['initscript'] += "\\caption{{Init script {dbms}}}\\end{{figure}}".format(dbms=c)
		# are there benchmarks for this query?
		if numQuery > 0 and self.benchmarker.timerExecution.checkForSuccessfulBenchmarks(numQuery):
			result['queryNumber']=numQuery
			query = tools.query(self.benchmarker.queries[numQuery-1])
			# format duration
			result['duration'] = tools.formatDuration(self.benchmarker.protocol['query'][str(numQuery)]['duration'])
			result['durations'] = "\\begin{itemize}\n"
			for c, d in self.benchmarker.protocol['query'][str(numQuery)]['durations'].items():
				result['durations'] += '\\item\\textbf{'+self.benchmarker.dbms[c].getName()+'}: '+tools.formatDuration(d)+'\n'
			result['durations'] += "\\end{itemize}\n"
			# format errors
			errors = ''
			for key, value in self.benchmarker.protocol['query'][str(numQuery)]['errors'].items():
				if not self.benchmarker.dbms[key].connectiondata['active']:
					continue
				if len(value) > 0:
					#firstpos = value.find(': ')
					#if firstpos > 0:
					#	#errors += '\\\\'+self.benchmarker.dbms[key].getName() + ': \\verb|' + value[(firstpos+2):] + '|\\\\'
					#	errors += '\\\\'+self.benchmarker.dbms[key].getName() + ': \\error{' + tools.tex_escape(value[(firstpos+2):]) + '}\\\\'
					#else:
					#	#errors += '\\\\'+self.benchmarker.dbms[key].getName() + ': \\verb|' + value + '|\\\\'
					#	errors += '\\\\'+self.benchmarker.dbms[key].getName() + ': \\error{' + tools.tex_escape(value) + '}\\\\'
					errors += self.benchmarker.dbms[key].getName() + ': \\error{' + tools.tex_escape(value) + '}\\\\'
			if len(errors) > 0:
				errors = '\\subsubsection{{Error}}\\label{{error:{code}-Q{queryNumber}}}'.format(**result)+errors
			errors += ''
			result['errors'] = errors
			# format warnings
			warnings = ''
			if 'warnings' in self.benchmarker.protocol['query'][str(numQuery)]:
				for key, value in self.benchmarker.protocol['query'][str(numQuery)]['warnings'].items():
					if not self.benchmarker.dbms[key].connectiondata['active']:
						continue
					if len(value) > 0:
						warnings += self.benchmarker.dbms[key].getName() + ': \\error{' + tools.tex_escape(value) + '}\\\\'
			if len(warnings) > 0:
				warnings = '\\subsubsection{{Warning}}\\label{{error:{code}-Q{queryNumber}}}'.format(**result)+warnings
			warnings += ''
			result['warnings'] = warnings
			# format parameter
			parameter = ''
			showParameter = self.benchmarker.queryconfig["reporting"]["queryparameter"]
			if showParameter is not None:
				if len(self.benchmarker.protocol['query'][str(numQuery)]['parameter']) > 0:
					parameter += '\\subsubsection{{Parameter}}\\label{{parameter:{code}-Q{queryNumber}}}'.format(**result)
					listParameter = self.benchmarker.protocol['query'][str(numQuery)]['parameter']
					dataframeParameter = pd.DataFrame.from_records(listParameter)
					dataframeParameter.index=range(1,len(listParameter)+1)
					# limit number of runs
					if showParameter is not None and showParameter == int(showParameter) and showParameter > 0:
						dataframeParameter = dataframeParameter[:int(showParameter)]
					# limit to first run
					if showParameter == 'first':
						dataframeParameter = dataframeParameter.iloc[0:1, :]
					parameter += tabulate(dataframeParameter, headers=dataframeParameter.columns, tablefmt="latex", stralign="right", showindex=True)
					parameter += '\\\\'
			result['queryParameter'] = parameter
			# data
			result['data'] = ""
			showStorage = self.benchmarker.queryconfig["reporting"]["resultsetPerQuery"]
			if "rowsPerResultset" in self.benchmarker.queryconfig["reporting"]:
				numberOfRows = self.benchmarker.queryconfig["reporting"]["rowsPerResultset"]
			else:
				numberOfRows = None
			if showStorage is not None:
				#print("resultsetPerQuery")
				if not self.benchmarker.protocol['query'][str(numQuery)]['dataStorage'] is None:
					if query.storeData != False:
						result['data'] += '\\subsubsection{{Received Data}}\\label{{data:{code}-Q{queryNumber}}}'.format(**result)
						for i,data in enumerate(self.benchmarker.protocol['query'][str(numQuery)]['dataStorage']):
							if showStorage == int(showStorage) and showStorage <= i and showStorage > 0:
								break
							if data is not None and len(data) > 0 and len(data[0]) > 0:
								df = pd.DataFrame.from_records(data)
								# first row contains column names
								df.columns = df.iloc[0]
								# remove first row
								df = df[1:]
								# limit number of rows in result set
								if numberOfRows is not None and numberOfRows == int(numberOfRows) and numberOfRows > 0:
									df = df[:int(numberOfRows)]
								if query.result == 'result':
									result['data'] += "\\noindent\\textbf{Result table of run "+str(i+1)+"}:\\\\"
								elif query.result == 'hash':
									result['data'] += "\\noindent\\textbf{Result hash of run "+str(i+1)+"}:\\\\"
								elif query.result == 'size':
									result['data'] += "\\noindent\\textbf{Result size of run "+str(i+1)+"}:\\\\"
								if query.restrict_precision:
									precision = query.restrict_precision
								else:
									precision = 2
								if df.empty:
									df.loc[len(df), :] = ['-' for i in range(1,len(df.columns)+1)]
								result['data'] += tabulate(df, headers=df.columns, tablefmt="latex", stralign="right", floatfmt=",."+str(precision)+"f", showindex=False)
								result['data'] += '\\\\\n\n'
								if showStorage == "first":
									break;
			# result sets
			result['resultSets'] = ""
			showResultsets = self.benchmarker.queryconfig["reporting"]["resultsetPerQueryConnection"]
			if showResultsets is not None:
				#print("resultsetPerQueryConnection")
				if not self.benchmarker.protocol['query'][str(numQuery)]['resultSets'] is None:
					for c, sets in self.benchmarker.protocol['query'][str(numQuery)]['resultSets'].items():
						if len(sets) > 0:
							# show result set because there is an error
							result['resultSets'] += '\\subsubsection{Received Data of '+self.benchmarker.dbms[c].getName()+'}'
							for i, data in enumerate(sets):
								if showStorage is not None and showStorage == int(showStorage) and showStorage <= i:
									break
								if data is not None and len(data) > 0:
									# if showResultsets == "differing":
									# only show result sets differing from data storage
									if len(self.benchmarker.protocol['query'][str(numQuery)]['dataStorage']) > i:
										if self.benchmarker.protocol['query'][str(numQuery)]['dataStorage'][i] == self.benchmarker.protocol['query'][str(numQuery)]['resultSets'][c][i]:
											#print("Same result for run "+str(i)+" of query "+str(numQuery))
											continue
									else:
										if self.benchmarker.protocol['query'][str(numQuery)]['dataStorage'][0] == self.benchmarker.protocol['query'][str(numQuery)]['resultSets'][c][i]:
											#print("Same result for run "+str(i)+" of query "+str(numQuery))
											continue
									df = pd.DataFrame.from_records(data)
									# first row contains column names
									df.columns = df.iloc[0]
									# remove first row
									df = df[1:]
									# limit number of rows in result set
									if numberOfRows is not None and numberOfRows == int(numberOfRows) and numberOfRows > 0:
										df = df[:int(numberOfRows)]
									if query.result == 'result':
										result['resultSets'] += "\\noindent\\textbf{Result table of run "+str(i+1)+"}:\\\\"
									elif query.result == 'hash':
										result['resultSets'] += "\\noindent\\textbf{Result hash of run "+str(i+1)+"}:\\\\"
									elif query.result == 'size':
										result['resultSets'] += "\\noindent\\textbf{Result size of run "+str(i+1)+"}:\\\\"
									if query.restrict_precision:
										precision = query.restrict_precision
									else:
										precision = 2
									if df.empty:
										df.loc[len(df), :] = ['-' for i in range(1,len(df.columns)+1)]
									result['resultSets'] += tabulate(df, headers=df.columns, tablefmt="latex", stralign="right", floatfmt=",."+str(precision)+"f", showindex=False)
									result['resultSets'] += '\\\\\n\n'
									if showResultsets == "first":
										break;
			# comparing
			result["comparesettings"] = ""
			if query.result:
				result["comparesettings"] += "\\subsubsection{Comparison of Result Sets}\n"
				result["comparesettings"] += "Compare by: "+query.result
				if len(str(query.restrict_precision)):
					result["comparesettings"] += "\\\\Restrict precision to "+str(query.restrict_precision)+" decimals"
				if query.sorted:
					result["comparesettings"] += "\\\\Sort result sets"
				if numberOfRows is not None:
					result["comparesettings"] += "\\\\Limit number of rows in reporting to "+str(numberOfRows)
				if showStorage is not None:
					result["comparesettings"] += "\\\\Show result sets: "+str(showStorage)+" of "+str(query.numRun)
				if showResultsets is not None:
					result["comparesettings"] += "\\\\Show differing result sets: "+str(showResultsets)+" of "+str(query.numRun)
				if showParameter is not None:
					result["comparesettings"] += "\\\\Show parameters: "+str(showParameter)+" of "+str(query.numRun)
			# query settings (connection manager)
			result["benchmarkmetrics"] = ""
			result["querysettings"] = ""#\\begin{itemize}"
			settings = {}
			for c, dbms in self.benchmarker.dbms.items():
				if not dbms.connectiondata['active']:
					continue
				#print(c)
				#print("-----------")
				dbmsname = self.benchmarker.dbms[c].getName()
				settings[dbmsname] = {}
				cm = self.benchmarker.getConnectionManager(numQuery, c)
				settings[dbmsname]['Clients'] = cm['numProcesses']
				settings[dbmsname]['Runs / Con'] = cm['runsPerConnection']
				settings[dbmsname]['Timeout'] = str(cm['timeout'])
				if cm['timeout'] is not None and cm['timeout'] != 0:
					settings[dbmsname]['Timeout'] += "s"
				else:
					settings[dbmsname]['Timeout'] = str(cm['timeout'])
				if c in self.benchmarker.protocol['query'][str(numQuery)]['durations']:
					settings[dbmsname]['Total Time'] = tools.formatDuration(self.benchmarker.protocol['query'][str(numQuery)]['durations'][c])
				#print(evaluation['query'][numQuery]['dbms'][c]['metrics'])
				if 'metrics' in evaluation['query'][str(numQuery)]['dbms'][c]:
					if c in evaluation['dbms'] and 'queuesize_run' in evaluation['query'][str(numQuery)]['dbms'][c]['metrics']:
						#settings[dbmsname]['qs_r / clients [%]'] = evaluation['query'][numQuery]['dbms'][c]['metrics']['queuesize_run']/cm['numProcesses']*100.0
						settings[dbmsname]['qs_r / clients [%]'] = evaluation['query'][str(numQuery)]['dbms'][c]['metrics']['queuesize_run_percent']
					if c in evaluation['dbms'] and 'queuesize_session' in evaluation['query'][str(numQuery)]['dbms'][c]['metrics']:
						#settings[dbmsname]['qs_s / clients [%]'] = evaluation['query'][numQuery]['dbms'][c]['metrics']['queuesize_session']/cm['numProcesses']*100.0
						settings[dbmsname]['qs_s / clients [%]'] = evaluation['query'][str(numQuery)]['dbms'][c]['metrics']['queuesize_session_percent']
				#print(settings)
			df = pd.DataFrame.from_dict(settings).transpose()
			result["querysettings"] = tabulate(df,headers=df.columns, tablefmt="latex", stralign="right", floatfmt=",.2f", showindex=True).replace("\\textbackslash{}", "\\").replace("\\{", "{").replace("\\}","}")
			# Lat and Tps
			df = pd.DataFrame.from_dict({c:d['metrics'] for c,d in evaluation['query'][str(numQuery)]['dbms'].items() if 'metrics' in d and c in evaluation['dbms'] and not 'error' in d}).transpose()
			df.index = df.index.map(tools.dbms.anonymizer)
			settings_translate = {
				'latency_run_mean_ms':'lat_r [ms]',
				'latency_session_mean_ms':'lat_s [ms]',
				'throughput_run_total_ps':'tps_r1 [Hz]',
				'throughput_run_mean_ps':'tps_r2 [Hz]',
				'throughput_session_total_ps':'tps_s1 [Hz]',
				'throughput_session_mean_ps':'tps_s2 [Hz]',
				'throughput_run_total_ph':'tph_r1 [ph]',
				'throughput_run_mean_ph':'tph_r2 [ph]',
				'throughput_session_total_ph':'tph_s1 [ph]',
				'throughput_session_mean_ph':'tph_s2 [ph]',
				'totaltime_ms':'total [s]',
				'queuesize_run':'qs_r',
				'queuesize_session':'qs_s',
				'queuesize_run_percent':'qs_r [%]',
				'queuesize_session_percent':'qs_s [%]',
			}
			df = df.reindex(['throughput_run_total_ps','throughput_run_mean_ps','throughput_session_total_ps','throughput_session_mean_ps','latency_run_mean_ms','latency_session_mean_ms','throughput_run_mean_ph','queuesize_run', 'queuesize_session'], axis=1)
			df = df.reindex(sorted(df.index), axis=0)
			df=df.rename(columns = settings_translate)
			df = tools.dataframehelper.addStatistics(df)
			result["benchmarkmetrics"] = tabulate(df,headers=df.columns, tablefmt="latex", stralign="right", floatfmt=",.2f", showindex=True).replace("\\textbackslash{}", "\\").replace("\\{", "{").replace("\\}","}")
			# dbms metrics
			# query list
			result["queryList"] = ""
			if len(query.queryList) > 0:
				result["queryList"] += "\\textbf{Sequence of "+str(len(query.queryList))+" queries}\n\\begin{itemize}"
				for queryElement in query.queryList: # query.queryList:
					queryElementObj = tools.query(self.benchmarker.queries[queryElement-1])					
					result["queryList"] += "\\item \\hyperref[benchmark:{code}-Q{queryNumber2}]{{Q{queryNumber2}: {queryTitle2}}}".format(**result, queryNumber2=queryElement, queryTitle2=queryElementObj.title)
				result["queryList"] += "\\end{itemize}"
			# query list runs
			result['queryRuns'] = ""
			if len(self.benchmarker.protocol['query'][str(numQuery)]['runs']) > 0:
				result['queryRuns'] = '\\subsubsection{{Runs of Query}}\\label{{runs:{code}-Q{queryNumber}}}'.format(**result)
				runs = []
				for i in range(query.numRun):
					queryElement = query.queryList[i % len(query.queryList)]
					queryElementObj = tools.query(self.benchmarker.queries[queryElement-1])
					run = self.benchmarker.protocol['query'][str(numQuery)]['runs'][i]
					runs.append(["\\hyperref[benchmark:{code}-Q{queryNumber2}]{{Q{queryNumber2}}}".format(**result, queryNumber2=queryElement), queryElementObj.title, run])
				df = pd.DataFrame.from_records(runs)
				df.columns = ["Query", "Query Title", "Run"]
				result['queryRuns'] += tabulate(df,headers=df.columns, tablefmt="latex", stralign="right", floatfmt=",.2f", showindex=False).replace("\\textbackslash{}", "\\").replace("\\{", "{").replace("\\}","}")
			# format title of query
			queryName = 'Query %d%s' % (numQuery, ': '+query.title)
			result['queryName'] = queryName
			if len(query.query) > 0:
				if isinstance(query.query,list):
					result['queryString'] = "\\begin{sqlFormat}\n"+"\n".join(query.query)+"\n\\end{sqlFormat}\n"
				elif isinstance(query.query,str):
					result['queryString'] = "\\begin{sqlFormat}\n"+query.query+"\n\\end{sqlFormat}\n"
				else:
					result['queryString'] = ""
			else:
				result['queryString'] = ""
			queryNotes = ""
			for c in query.DBMS:
				for connectionname, connection in self.benchmarker.dbms.items():
					if connectionname.startswith(c) and self.benchmarker.dbms[connectionname].connectiondata['active']:
						queryNotes += self.benchmarker.dbms[connectionname].getName() + ': Different query\n\\begin{sqlFormat}\n' + query.DBMS[c] + '\n\\end{sqlFormat}'
			result['queryNotes'] = queryNotes
			result['warmup']=str(query.warmup)
			result['cooldown']=str(query.cooldown)
			result['delay_run']=str(query.delay_run)
			result['delay_connect']=str(query.delay_connect)
			result['run']=str(query.numRun)
			result['start']=self.benchmarker.protocol['query'][str(numQuery)]['start']
			result['end']=self.benchmarker.protocol['query'][str(numQuery)]['end']
			# hardware metrics
			result['hardwareMetrics'] = ""
			result['hardwareMetricsTable'] = ""
			for connectionname, connection in self.benchmarker.dbms.items():
				if self.benchmarker.dbms[connectionname].hasHardwareMetrics():
					metricsReporter = monitor.metrics(self.benchmarker)
					#result['hardwareMetrics'] = monitor.metrics.latex.format(**result)
					result['hardwareMetrics'] = metricsReporter.generateLatexForQuery(result)
					m = 'total_gpu_util'
					df1 = metricsReporter.dfHardwareMetrics(numQuery, m)
					df2 = evaluator.addStatistics(df1, drop_nan=False)
					result['hardwareMetricsTable'] = '\\newpage'
					result['hardwareMetricsTable'] += tabulate(df2, headers=df2.columns, tablefmt="latex", stralign="right", floatfmt=",.2f", showindex=True)
					break
			# report per timer
			if numTimer > 0:
				result['nameTimer'] = timer.name
				# format times for output
				header = tools.timer.header_stats.copy()
				# add factor column
				header.insert(1,"factor")
				# convert to DataFrame
				dataframe = self.benchmarker.statsToDataFrame(numQuery, timer)
				# test if any rows left
				if dataframe.empty:
					return {}
				# add statistics
				dataframe = tools.dataframehelper.addStatistics(dataframe)
				#if dataframe.isnull().values.any():
				#	print(numQuery)
				#	print(timer.name)
				#	print(dataframe)
				# print transfer table in latex
				table = tabulate(dataframe, headers=header, tablefmt="latex", stralign="right", floatfmt=",.2f", showindex=True)
				# align dbms name to the left
				table = table.replace('\\begin{tabular}{r', '\\begin{tabular}{l')
				result['table'] = table
				result['tableName'] = "Time "+timer.name
				result['timerName'] = timer.name
		#print(result)
		return result
	def generate(self, numQuery, timer):
		"""
		Generates latex report (no front page, given query) for given list of timers.
		Anonymizes dbms if activated.

		:param numQuery: Number of query to collect data from. numQuery=0 for title page
		:param timer: List of timers to collect data from. timer=None for title page
		:return: returns nothing
		"""
		# generate report pages per started query
		for numQuery in range(1, len(self.benchmarker.queries)+1):
			# are there benchmarks for this query?
			if not self.benchmarker.timerExecution.checkForSuccessfulBenchmarks(numQuery):
				continue
			# is this query active?
			query = tools.query(self.benchmarker.queries[numQuery-1])
			if not query.active:
				continue
			parameter = self.prepare(numQuery, 0)
			latex_output = self.useTemplate('query', parameter)
			for numTimer,t in enumerate(timer):
				if not t.checkForSuccessfulBenchmarks(numQuery):
					continue
				# is timer active for this query?
				if not query.timer[t.name]['active']:
					continue
				parameter = self.prepare(numQuery, numTimer+1, t)
				if len(parameter) > 0:
					latex_output += self.useTemplate('timer', parameter)
			# save latex in file, append to existing
			latex_file = open(self.benchmarker.path+"/benchmarks.tex", "a+")
			latex_file.write(latex_output)
			latex_file.close()
		# generate report appendix pages per started query
		parameterGeneral = self.prepare(0, 0)
		latex_output = self.useTemplate('appendix', parameterGeneral)
		latex_file = open(self.benchmarker.path+"/benchmarks.tex", "a+")
		latex_file.write(latex_output)
		latex_file.close()
		for numQuery in range(1, len(self.benchmarker.queries)+1):
			# are there benchmarks for this query?
			if not self.benchmarker.timerExecution.checkForSuccessfulBenchmarks(numQuery):
				continue
			# is this query active?
			query = tools.query(self.benchmarker.queries[numQuery-1])
			if not query.active:
				continue
			parameter = self.prepare(numQuery, 0)
			latex_output = self.useTemplate('appendixquery', parameter)
			for numTimer,t in enumerate(timer):
				if not t.checkForSuccessfulBenchmarks(numQuery):
					continue
				parameter = self.prepare(numQuery, numTimer+1, t)
				if len(parameter) > 0:
					latex_output += self.useTemplate('appendixtimer', parameter) #self.template['timer'].format(**parameter)
			# save latex in file, append to existing
			latex_file = open(self.benchmarker.path+"/benchmarks.tex", "a+")
			latex_file.write(latex_output)
			latex_file.close()
		# add workflow of bexhoma
		parameterGeneral['workflow'] = ""
		if not self.benchmarker.anonymize:
			dfWorkflow = tools.dataframehelper.getWorkflow(self.benchmarker)
			if not dfWorkflow is None and not dfWorkflow.empty:
				#parameterGeneral['workflow'] = "\\subsection{Workflow}\\label{workflow}\n\\Rotatebox{90}{{\\scriptsize{"
				parameterGeneral['workflow'] = "\\subsection{Workflow}\\label{workflow}\n{\\scriptsize{"
				parameterGeneral['workflow'] += tabulate(dfWorkflow, headers=dfWorkflow.columns, tablefmt="latex", stralign="right", showindex=True)
				parameterGeneral['workflow'] += "}}"
		# generate foot of report
		latex_output = self.useTemplate('reportFoot', parameterGeneral)
		# save latex in file, append to existing
		latex_file = open(self.benchmarker.path+"/benchmarks.tex", "a+")
		latex_file.write(latex_output)
		latex_file.close()
	def generateAll(self, timer):
		"""
		Generates complete latex report (front page, all queries and connections) for given list of timers.
		Anonymizes dbms if activated.

		:param timer: List of timer objects
		"""
		self.init()
		if self.e is None:
		#if len(evaluator.evaluator.evaluation) == 0:
			self.e = evaluator.evaluator(self.benchmarker)
		self.generate(1, timer)



class DEPRECATED_latexerPagePerQuery(DEPRECATED_latexer):
	"""
	Class for generating reports.
	This class generates a survey in latex and saves it to disk.
	The survey has one page per timer.
	"""
	def __init__(self, benchmarker, templatefolder):
		latexer.__init__(self, benchmarker, templatefolder="pagePerQuery")



class DEPRECATED_latexerCustom(DEPRECATED_latexer):
	"""
	Class for generating reports.
	This class generates a survey in latex and saves it to disk.
	This is a dummy for generating custom reports.
	Concept: Just change the output templates and possibly the prepare method to append/change the results list.
	"""
	def __init__(self, benchmarker):
		latexer.__init__(self, benchmarker, templatefolder="custom")
	def prepare(self, numQuery=0, numTimer=0, timer=None):
		result = latexer.prepare(numQuery, numTimer, timer)
		return result







