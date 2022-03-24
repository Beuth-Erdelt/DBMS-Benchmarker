"""
    Benchmarking Classes for the Python Package DBMS Benchmarker
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
from timeit import default_timer as timer
from tabulate import tabulate
import pandas as pd
from tqdm import tqdm
import logging
from os import makedirs, path
import os
import ast
from shutil import copyfile
import datetime
import time
import re
import hashlib
import pickle
import sys
import json
import math
from operator import itemgetter
from collections import Counter
import multiprocessing as mp
from timeit import default_timer
import random
from operator import add
from dbmsbenchmarker import tools, reporter, parameter, monitor, evaluator
import pprint
# for query timeout
import jaydebeapi

#activeConnections = []

BENCHMARKER_VERBOSE_QUERIES = False
BENCHMARKER_VERBOSE_STATISTICS = False
BENCHMARKER_VERBOSE_RESULTS = False
BENCHMARKER_VERBOSE_PROCESS = False

class singleRunInput:
	"""
	Class for collecting info about a benchmark run
	"""
	def __init__(self, numRun, queryString, queryConfig):
		self.numRun = numRun
		self.queryString = queryString
		self.queryConfig = queryConfig



class singleRunOutput:
	"""
	Class for collecting info about a benchmark run
	"""
	def __init__(self):
		self.durationConnect = 0.0
		self.durationExecute = None#0.0
		self.durationTransfer = None#0.0
		self.error = ''
		self.data = []
		self.columnnames = []
		self.size = 0
		pass



def singleRun(connectiondata, inputConfig, numRuns, connectionname, numQuery, path=None, activeConnections = [], BENCHMARKER_VERBOSE_QUERIES=False, BENCHMARKER_VERBOSE_RESULTS=False, BENCHMARKER_VERBOSE_PROCESS=True):
	"""
	Function for running an actual benchmark run

	:param connectiondata: Data about the connection, dict format
	:param inputConfig: Data containing info about the benchmark run
	:param numRun: Number of benchmark run
	:param connectionname: Name of the connection
	:param numQuery: Number of the query, 1...
	:param path: Result path, for optional storing received data
	:return: returns object of class singleRunOutput
	"""
	#global activeConnections
	import logging
	#logging.basicConfig(format='%(asctime)s %(message)s')
	logger = logging.getLogger('singleRun')
	logger.setLevel(logging.DEBUG)
	# init list of results
	results = []
	# compute number of (parallel) connection
	# example: 5/6/7/8 yields number 1 (i.e. the second one)
	# this is working, but requires a connection per batch
	numActiveConnection = math.floor(numRuns[0]/len(numRuns))
	numActiveConnection = 0
	#activeConnections = JUnpickler.loads(activeConnections)
	#print(numActiveConnection)
	#print(len(activeConnections))
	if len(activeConnections) > numActiveConnection:
		if BENCHMARKER_VERBOSE_PROCESS:
			logger.info("Found active connection #"+str(numActiveConnection))
		connection = activeConnections[numActiveConnection]
		durationConnect = 0.0
	else:
		# look at first run to determine of there should be sleeping
		query = tools.query(inputConfig[numRuns[0]].queryConfig)
		if query.delay_connect > 0:
			if BENCHMARKER_VERBOSE_PROCESS:
				logger.info("Delay Connection by "+str(query.delay_connect)+" seconds")
			time.sleep(query.delay_connect)
		# connect to dbms
		connection = tools.dbms(connectiondata)
		start = default_timer()
		connection.connect()
		end = default_timer()
		durationConnect = 1000.0*(end - start)
	if BENCHMARKER_VERBOSE_PROCESS:
		logger.info(("singleRun batch size %i: " % len(numRuns)))
		logger.info(("numRun %s: " % ("/".join([str(i+1) for i in numRuns])))+"connection [ms]: "+str(durationConnect))
	# perform runs for this connection
	for numRun in numRuns:
		workername = "numRun %i: " % (numRun+1)
		queryString = inputConfig[numRun].queryString
		#print(workername+queryString)
		query = tools.query(inputConfig[numRun].queryConfig)
		if query.delay_run > 0:
			if BENCHMARKER_VERBOSE_PROCESS:
				logger.info(workername+"Delay Run by "+str(query.delay_run)+" seconds")
			time.sleep(query.delay_run)
		error = ""
		try:
			#start = default_timer()
			if BENCHMARKER_VERBOSE_QUERIES:
				#logger.info(type(queryString))
				if isinstance(queryString, list):
					for queryPart in queryString:
						logger.info(workername+queryPart)
				else:
					logger.info(workername+queryString)
			connection.openCursor()
			#end = default_timer()
			#durationConnect += 1000.0*(end - start)
			start = default_timer()
			# if query is given as list of strings
			if isinstance(queryString, list):
				for queryPart in queryString:
					connection.executeQuery(queryPart)
			else:
				connection.executeQuery(queryString)
			end = default_timer()
			durationExecute = 1000.0*(end - start)
			logger.info(workername+"execution [ms]: "+str(durationExecute))
			# transfer
			data = []
			columnnames = []
			size = 0
			durationTransfer = 0
			if query.withData:
				if len(queryString) != 0:
					start = default_timer()
					if isinstance(queryString, list):
						data = []
						columnnames = []
						size = 0
						durationTransfer = 0
					else:
						data=connection.fetchResult()
						end = default_timer()
						durationTransfer = 1000.0*(end - start)
						logger.info(workername+"transfer [ms]: "+str(durationTransfer))
						data = [[str(item).strip() for item in sublist] for sublist in data]
						size = sys.getsizeof(data)
						logger.info(workername+"Size of result list retrieved: "+str(size)+" bytes")
						#self.logger.debug(data)
						columnnames = [[i[0].upper() for i in connection.cursor.description]]
						if BENCHMARKER_VERBOSE_RESULTS:
							s = columnnames + [[str(e) for e in row] for row in data]
							lens = [max(map(len, col)) for col in zip(*s)]
							fmt = '\t'.join('{{:{}}}'.format(x) for x in lens)
							table = [fmt.format(*row) for row in s]
							logger.info(workername+('\n'.join(table)))
						if not query.storeData:
							logger.info(workername+"Forget result set")
							data = []
							columnnames = []
						#self.logger.debug(columnnames)
		except Exception as e:
			logger.exception(workername+'Caught an error: %s' % str(e))
			error = '{workername}: {exception}'.format(workername=workername, exception=e)
			durationConnect = 0
			durationExecute = 0
			durationTransfer = 0
			data = []
			columnnames = []
			size = 0
		finally:
			#start = default_timer()
			connection.closeCursor()
			#end = default_timer()
			#durationExecute += 1000.0*(end - start)
		result = singleRunOutput()
		# connection time is valid only for first run (making the connection)
		if numRun==numRuns[0] and query.withConnect:
			result.durationConnect = durationConnect
		else:
			result.durationConnect = 0.0
		result.durationExecute = durationExecute
		result.durationTransfer = durationTransfer
		result.error = error
		result.data = data
		result.size = size
		result.columnnames = columnnames
		#result.size = size
		results.append(result)
	if not len(activeConnections) > numActiveConnection:
		#start = default_timer()
		connection.disconnect()
		#end = default_timer()
		#durationConnect += 1000.0*(end - start)
	return results



class singleResultInput:
	"""
	Class for collecting info about a benchmark run
	"""
	def __init__(self, numRun, data, columnnames, queryConfig):
		self.numRun = numRun
		self.data = data
		self.columnnames = columnnames
		self.queryConfig = queryConfig



def singleResult(connectiondata, inputConfig, numRuns, connectionname, numQuery, path=None):
	"""
	Function for treating result sets

	:param connectiondata: Data about the connection, dict format
	:param inputConfig: Data containing info about the benchmark run
	:param numRun: Number of benchmark run
	:param connectionname: Name of the connection
	:param numQuery: Number of the query, 1...
	:param path: Result path, for optional storing received data
	:return: returns object of class singleRunOutput
	"""
	import logging
	logger = logging.getLogger('singleResult')
	logger.setLevel(logging.INFO)
	logger.debug("Processing result sets of {} runs for query {}".format(len(numRuns), numQuery))
	# init list of results
	results = []
	# info about dbms
	connection = tools.dbms(connectiondata)
	# perform runs for this connection
	for numRun in numRuns:
		workername = "numRun %i: " % (numRun+1)
		query = tools.query(inputConfig[numRun].queryConfig)
		error = ""
		try:
			# transfer
			data = inputConfig[numRun].data
			columnnames = inputConfig[numRun].columnnames
			size = 0
			#print(data)
			if query.result:
				data = [[str(item).strip() for item in sublist] for sublist in data]
				if query.restrict_precision is not None:
					data = [[round(float(item), int(query.restrict_precision)) if tools.convertToFloat(item) == float else item for item in sublist] for sublist in data]
				if query.sorted and len(data) > 0:
					logger.debug(workername+"Begin sorting")
					data = sorted(data, key=itemgetter(*list(range(0,len(data[0])))))
					logger.debug(workername+"Finished sorting")
			logger.info(workername+"Size of processed result list retrieved: "+str(sys.getsizeof(data))+" bytes")
			# convert to dataframe
			#columnnames = [[i[0].upper() for i in connection.cursor.description]]
			df = pd.DataFrame.from_records(data)
			logger.debug(workername+'Data {}'.format(data))
			logger.debug(workername+'DataFrame generated')
			if not df.empty:
				df.columns = columnnames
			size = int(df.memory_usage(index=True).sum())
			logger.debug(workername+'DataFrame size: %s' %str(size))
			# store result set for connection and query
			storeResultSet = query.storeResultSet
			if storeResultSet and numRun==0:
				if path is not None:
					if 'dataframe' in query.storeResultSetFormat:
						filename = path+"/query_"+str(numQuery)+"_resultset_"+connectionname+".pickle"
						logger.debug(workername+"Store pickle of result set to "+filename)
						f = open(filename, "wb")
						pickle.dump(df, f)
						f.close()
					if 'csv' in query.storeResultSetFormat:
						filename = path+"/query_"+str(numQuery)+"_resultset_"+connectionname+".csv"
						logger.debug(workername+"Store csv of result set to "+filename)
						f = open(filename, "w")
						f.write(df.to_csv(index_label=False,index=False))
						f.close()
			# store (compressed) data for comparison
			if query.result == 'hash':
				# replace by hash information
				columnnames = [['hash']]
				data = columnnames + [[hashlib.sha224(pickle.dumps(data)).hexdigest()]]
				logger.debug(workername+"Compressed by hash")
			elif query.result == 'size':
				# replace by size information
				columnnames = [['size']]
				data = columnnames + [[size]]
				#data = columnnames + [[sys.getsizeof(data)]]
				logger.debug(workername+"Compressed by size")
			else:
				#columnnames = [[n[0].upper() for n in connection.cursor.description]]
				data = columnnames + data
				logger.debug(workername+"Uncompressed")
			logger.debug(workername+"Size of sorted result list stored: "+str(sys.getsizeof(data))+" bytes")
		except Exception as e:
			logger.exception(workername+'Caught an error: %s' % str(e))
			error = '{workername}: {exception}'.format(workername=workername, exception=e)
			data = []
			size = 0
		finally:
			pass
		logger.debug(workername+'Done processing result')
		result = singleRunOutput()
		result.error = error
		result.data = data
		result.size = size
		results.append(result)
	return results



class benchmarker():
	"""
	Class for running benchmarks
	"""
	def __init__(self, result_path=None, working='query', batch=False, fixedQuery=None, fixedConnection=None, rename_connection='', rename_alias='', fixedAlias='', anonymize=False, unanonymize=[], numProcesses=None, seed=None, code=None, subfolder=None):
		"""
		Construct a new 'benchmarker' object.
		Allocated the reporters store (always called) and printer (if reports are to be generated).
		A result folder is created if not existing already.

		:param result_path: Path for storing result files. If None is given, a folder is created using time.
		:param working: Process benchmarks query-wise or connection-wise
		:param batch: Script is running in batch mode (more protocol-like output)
		:param fixedQuery: Number of only query to be benchmarked
		:param fixedConnection: Name of only connection to be benchmarked
		:param anonymize: Anonymize all dbms
		:param unanonymize: List of names of connections, which should not be anonymized despite of parameter anonymize
		:param numProcesses: Number of parallel client processes. Global setting, can be overwritten by connection or query
		:param seed: -
		:param code: Optional code for result folder
		:return: returns nothing
		"""
		self.logger = logging.getLogger('benchmarker')
		if seed is not None:
			random.seed(seed)
		## connection management:
		self.connectionmanagement = {'numProcesses': numProcesses, 'runsPerConnection': None, 'timeout': None, 'singleConnection': True}
		# set number of parallel client processes
		#self.connectionmanagement['numProcesses'] = numProcesses
		if self.connectionmanagement['numProcesses'] is None:
			self.connectionmanagement['numProcesses'] = 1#math.ceil(mp.cpu_count()/2) #If None, half of all available processes is taken
		else:
			self.connectionmanagement['numProcesses'] = int(self.numProcesses)
		# connection should be renamed (because of copy to subfolder and parallel processing)
		# also rename alias
		self.rename_connection = rename_connection
		self.rename_alias = rename_alias	# what alias is supposed to be renamed to
		self.fixedAlias = fixedAlias		# what alias is in connection file
		# for connections staying active for all benchmarks
		self.activeConnections = []
		#self.runsPerConnection = 4
		#self.timeout = 600
		# there is no general pool
		self.pool = None
		# printer is first and fixed reporter
		self.reporter = [reporter.printer(self)]
		# store is fixed reporter and cannot be removed
		self.reporterStore = reporter.storer(self)
		# dict of dbms
		self.dbms = {}
		# list of queries
		self.queries = []
		# should benchmarks be overwritten
		self.overwrite = False
		# clear all possibly present benchmarks
		self.clearBenchmarks()
		# should result folder be created
		self.continuing = False
		self.path = ""
		if result_path is None:
			if code is None:
				self.code = str(round(time.time()))
			else:
				self.code = str(int(code))
			self.path = self.code
			makedirs(self.path)
		else:
			if path.isdir(result_path):
				if path.isfile(result_path+'/queries.config') and path.isfile(result_path+'/connections.config'):
					# result folder exists and contains results
					self.code = path.basename(path.normpath(result_path))
					self.path = result_path
					if path.isfile(result_path+'/protocol.json'):
						self.continuing = True
				else:
					# result path is not a folder for existing results
					if code is None:
						self.code = str(round(time.time()))
					else:
						self.code = str(int(code))
					#self.code = str(round(time.time()))
					self.path = result_path+"/"+self.code
					if not path.isdir(self.path):
						makedirs(self.path)
			else:
				self.logger.exception("Path does not exist: "+result_path)
			#self.path = str(int(path))
		self.resultfolder_base = None
		self.resultfolder_subfolder = None
		if subfolder is not None:# and fixedConnection is not None:
			self.resultfolder_base = self.path
			self.resultfolder_subfolder = subfolder
			self.path = self.resultfolder_base+"/"+self.resultfolder_subfolder
			if not path.isdir(self.path):
				makedirs(self.path)
			"""
			else:
				# TODO: 'name': 'MemSQL-5' replace by 'MemSQL-5-1' in connections.config? self.rename_connection
				client = 1
				while True:
					self.resultfolder_base = self.path
					self.resultfolder_subfolder = subfolder+'-'+str(client)
					self.path = self.resultfolder_base+"/"+self.resultfolder_subfolder				
					if not path.isdir(self.path):
						makedirs(self.path)
						break
					else:
						client = client + 1
			"""
			if path.isfile(self.resultfolder_base+'/protocol.json'):
				copyfile(self.resultfolder_base+'/protocol.json', self.path+'/protocol.json')
				self.continuing = True
		print("Benchmarking in folder "+self.path)
		# querywise or connectionwise
		self.working = working
		# batch mode, different output
		self.bBatch = batch
		# benchmark only fixed query or connection
		if not fixedQuery is None:
			fixedQuery = int(fixedQuery)
		self.fixedQuery = fixedQuery
		self.fixedConnection = fixedConnection
		# anonymize dbms
		self.anonymize = anonymize# True or False
		self.unanonymize = unanonymize# Name of connection
	def clearBenchmarks(self):
		"""
		Clears all benchmark related protocol data.
		Allocates a timer for execution and a timer for data transfer.

		:return: returns nothing
		"""
		self.protocol = {'query':{}, 'connection':{}}
		self.timerExecution = tools.timer("execution")
		self.timerTransfer = tools.timer("datatransfer")
		self.timerConnect = tools.timer("connection")
		self.timerSession = tools.timer("session")
		self.timerSession.stackable = False
		self.timerSession.perRun = False
		self.timerRun = tools.timer("run")
		self.timerRun.stackable = False
		self.timers = [self.timerSession, self.timerRun, self.timerExecution, self.timerTransfer, self.timerConnect]
	def getConfig(self,configfolder=None, connectionfile=None, queryfile=None):
		"""
		Reads all queries and connections from given config files.
		It's possible to give a name of a folder instead.
		Filenames 'connections.config' and 'query.config' are assumed then.

		:param configfolder: Name of the folder containing config files
		:param connectionfile: Name of the file containing connection data
		:param queryfile: Name of the file containing query data
		:return: returns nothing
		"""
		if connectionfile is None:
			connectionfile = 'connections.config'
		if queryfile is None:
			queryfile = 'queries.config'
		if configfolder is not None:
			self.getConnectionsFromFile(configfolder+'/'+connectionfile)
			self.getQueriesFromFile(configfolder+'/'+queryfile)
		#elif self.resultfolder_base is not None:
		#	self.getConnectionsFromFile(self.resultfolder_base+'/connections.config')
		#	self.getQueriesFromFile(self.resultfolder_base+'/queries.config')
		else:
			self.getConnectionsFromFile(connectionfile)
			self.getQueriesFromFile(queryfile)
	def getQueriesFromFile(self,filename=None):
		"""
		Reads all queries from a given file in json format and stores them for further usage.
		The file is copied to the result folder if not there already.

		:param filename: Name of the file containing query data
		:return: returns nothing
		"""
		# If result folder exists: Read from there
		if path.isfile(self.path+'/queries.config'):
			filename = self.path+'/queries.config'
		# If nothing is given: Try to read from result folder
		if filename is None:
			filename = self.path+'/queries.config'
		# If not read from result folder: Copy to result folder
		if not filename == self.path+'/queries.config':
			if path.isfile(filename):
				copyfile(filename, self.path+'/queries.config')
			else:
				self.logger.exception('Caught an error: Query file not found')
				exit()
		with open(filename,'r') as inp:
			self.queryconfig = ast.literal_eval(inp.read())
			# global setting in a class variable
			# overwrites parts of query file
			if tools.query.template is not None:
				for i,q in enumerate(self.queryconfig['queries']):
					self.queryconfig['queries'][i] = {**q, **tools.query.template}
					with open(self.path+'/queries.config','w') as outp:
						pprint.pprint(self.queryconfig, outp)
			self.queries = self.queryconfig["queries"].copy()
			if not "name" in self.queryconfig:
				self.queryconfig["name"] = "No name"
			if not "intro" in self.queryconfig:
				self.queryconfig["intro"] = ""
			if not "factor" in self.queryconfig:
				self.queryconfig["factor"] = "mean"
			if "connectionmanagement" in self.queryconfig:
				#self.connectionmanagement = self.queryconfig["connectionmanagement"].copy()
				self.connectionmanagement = tools.joinDicts(self.connectionmanagement, self.queryconfig["connectionmanagement"])
			#	if "timeout" in self.queryconfig["connectionmanagement"]:
			#		self.timeout = self.queryconfig["connectionmanagement"]["timeout"]
			#	if "numProcesses" in self.queryconfig["connectionmanagement"]:
			#		self.numProcesses = self.queryconfig["connectionmanagement"]["numProcesses"]
			#	if "runsPerConnection" in self.queryconfig["connectionmanagement"]:
			#		self.runsPerConnection = self.queryconfig["connectionmanagement"]["runsPerConnection"]
			if not "reporting" in self.queryconfig:
				self.queryconfig["reporting"] = {'resultsetPerQuery': False, 'resultsetPerQueryConnection': False, 'queryparameter': False, 'rowsPerResultset': False}
			if len(parameter.defaultParameters) > 0:
				if 'defaultParameters' in self.queryconfig:
					self.queryconfig['defaultParameters'] = {**self.queryconfig['defaultParameters'], **parameter.defaultParameters}
				else:
					self.queryconfig['defaultParameters'] = parameter.defaultParameters.copy()
			if 'defaultParameters' in self.queryconfig:
				parameter.defaultParameters = self.queryconfig['defaultParameters']
		for numQuery in range(1, len(self.queries)+1):
			self.protocol['query'][str(numQuery)] = {'errors':{}, 'warnings':{}, 'durations':{}, 'duration':0.0, 'start':'', 'end':'', 'dataStorage': [], 'resultSets': {}, 'parameter': [], 'sizes': {}, 'starts': {}, 'ends': {}, 'runs': []}
	def cleanProtocol(self, numQuery):
		"""
		Cleans the protocol for an existing query.
		This is helpful for rerunning a benchmark.

		:param numQuery: Number of query to benchmark
		:return: returns nothing
		"""
		self.protocol['query'][str(numQuery)]['errors'] = {}
		self.protocol['query'][str(numQuery)]['dataStorage'] = []
		self.protocol['query'][str(numQuery)]['parameter'] = []
	def getConnectionsFromFile(self,filename=None):
		"""
		Reads all connection data from a given file in json format and stores them for further usage.
		The file is copied to the result folder if not there already.

		:param filename: Name of the file containing connection data
		:return: returns nothing
		"""
		# If result folder exists: Read from there
		if path.isfile(self.path+'/connections.config'):
			filename = self.path+'/connections.config'
		# If nothing is given: Try to read from result folder
		if filename is None:
			filename = self.path+'/connections.config'
		# If not read from result folder: Copy to result folder
		if not filename == self.path+'/connections.config':
			if path.isfile(filename):
				copyfile(filename, self.path+'/connections.config')
				self.logger.debug("copied connection file {} to {}".format(filename, self.path+'/connections.config'))
				# 'name': 'MemSQL-5' replace by 'MemSQL-5-1' in connections.config? self.rename_connection
				if self.fixedConnection is not None and len(self.fixedConnection) > 0 and len(self.rename_connection) > 0:
					with open(self.path+'/connections.config', "r") as connections_file:
						connections_content = connections_file.read()
					#print(connections_content)
					connections_content = connections_content.replace("'name': '{}'".format(self.fixedConnection), "'name': '{}', 'orig_name': '{}'".format(self.rename_connection, self.fixedConnection))
					connections_content = connections_content.replace("'alias': '{}'".format(self.fixedAlias), "'alias': '{}', 'orig_alias': '{}'".format(self.rename_alias, self.fixedAlias))
					#print(connections_content)
					with open(self.path+'/connections.config', "w") as connections_file:
						connections_file.write(connections_content)
					self.logger.debug("Renamed connection {} to {}".format(self.fixedConnection, self.rename_connection))
					self.logger.debug("Renamed alias {} to {}".format(self.fixedAlias, self.rename_alias))
					self.fixedConnection = self.rename_connection
					filename = self.path+'/connections.config'
			else:
				self.logger.exception('Caught an error: Connection file not found')
				exit()
		# read from file
		with open(filename,'r') as inf:
			self.connections = ast.literal_eval(inf.read())
		# add all dbms
		for c in self.connections:
			if self.anonymize and not c['name'] in self.unanonymize:
				# this dbms shoud be anonymized
				anonymous = True
			else:
				anonymous = False
			self.dbms[c['name']] = tools.dbms(c, anonymous)
		#self.connectDBMSAll()
	def connectDBMSAll(self):
		"""
		Connects to all dbms we have collected connection data of.

		:return: returns nothing
		"""
		for c in sorted(self.dbms.keys()):
			self.connectDBMS(c)
	def disconnectDBMSAll(self):
		"""
		Disconnects to all dbms we have connected to.

		:return: returns nothing
		"""
		for c in self.dbms:
			self.disconnectDBMS(c)
	def connectDBMS(self, connectionname):
		"""
		Connects to one single dbms.

		:param connectionname: Name of the connection we want to establish.
		:return: returns nothing
		"""
		print("Connecting to "+connectionname)
		self.dbms[connectionname].connect()
		print("Connected to "+self.dbms[connectionname].connectiondata['name'])
	def disconnectDBMS(self, connectionname):
		"""
		Disconnects from one single dbms.

		:param connectionname: Name of the connection we want to disconnect from.
		:return: returns nothing
		"""
		self.logger.debug("Disconnect from "+connectionname)
		self.dbms[connectionname].disconnect()
	def removeInactiveConnectionsFromDataframe(self, dataframe):
		"""
		Remove inactive dbms from dataframe.
		Connection names are expected in first column named 0.

		:param dataframe: Dataframe, first column containing names of connections
		:param benchmarker: Benchmarker object for access to config (check for active connections)
		:return: Cleaned dataframe
		"""
		newdataframe = dataframe
		for index, row in dataframe.iterrows():
			if not self.dbms[row[0]].connectiondata['active']:
				newdataframe = newdataframe.drop([index], axis=0)
		newdataframe.reset_index(drop=True, inplace=True)
		return newdataframe
	def benchmarksToDataFrame(self, numQuery, timer):
		"""
		Returns benchmarks of a given query and timer as a DataFrame (rows=dbms, cols=benchmark runs).

		:param numQuery: Number of query
		:param timer: Timer object
		:return: DataFrame of benchmark times
		"""
		dataframe = timer.toDataFrame(numQuery)
		if dataframe.empty:
			return dataframe
		# remove inactive connections
		dataframe = self.removeInactiveConnectionsFromDataframe(dataframe)
		# drop rows of only 0
		dataframe = dataframe[(dataframe.T[1:] != 0).any()]
		# first column to index
		dataframe = dataframe.set_index(dataframe[0].map(tools.dbms.anonymizer))
		dataframe.index.name = 'DBMS'
		# drop first column
		dataframe = dataframe.drop(columns=[0])
		return dataframe
	def statsToDataFrame(self, numQuery, timer):
		"""
		Returns statistics of a given query and timer as a DataFrame (rows=dbms, cols=statisticd).

		:param numQuery: Number of query
		:param timer: Timer object
		:return: DataFrame of benchmark statistics
		"""
		dataframe = timer.statsToDataFrame(numQuery)
		# remove inactive connections
		dataframe = self.removeInactiveConnectionsFromDataframe(dataframe)
		# add factor column
		dataframe = tools.dataframehelper.addFactor(dataframe, self.queryconfig['factor'])
		return dataframe
	def generateSortedTotalRanking(self):
		"""
		Returns dataframe (rows=dbms, col=ranking) of total rankings (average ranking all queries and timers).

		:return: DataFrame of rankings
		"""
		totalRank = {}
		numSuccessfulQueries = 0
		numActiveDBMS = len([d for i,d in self.dbms.items() if d.connectiondata['active']])
		for t in self.timers:
			for numQuery in range(1, len(self.queries)+1):
				if t.checkForSuccessfulBenchmarks(numQuery):
					queryObject = tools.query(self.queries[numQuery-1])
					# is timer active for this query?
					if not queryObject.timer[t.name]['active']:
						continue
					if not queryObject.active:
						continue
					numSuccessfulQueries = numSuccessfulQueries + 1
					# convert to DataFrame
					dataframe = self.statsToDataFrame(numQuery, t)
					#print(dataframe)
					# test if any rows left
					if (dataframe[(dataframe.T[1:] != 0).any()]).empty:
						continue
					rank = {}
					for c in self.dbms.keys():
						if self.dbms[c].connectiondata['active']:
							rank[self.dbms[c].getName()] = numActiveDBMS
					i = 1
					for name in dataframe.index:
						#if self.dbms[name].connectiondata['active']:
						rank[name] = i
						i = i + 1
					totalRank = dict(Counter(totalRank)+Counter(rank))
		# generate dict of ranks
		totalRank = {k: (v / numSuccessfulQueries) for k, v in totalRank.items()}
		# generate sorted dict of ranks
		sortedRank = sorted(totalRank.items(), reverse=True, key=lambda kv: kv[1])
		# convert to dataframe
		dataframe = pd.DataFrame.from_records(sortedRank)
		#dataframe = self.removeInactiveConnectionsFromDataframe(dataframe)
		dataframe.columns = ['DBMS', 'Average Position']
		dataframe = dataframe.set_index('DBMS')
		#dataframe.index = dataframe.index.map(tools.dbms.anonymizer)
		return dataframe, numSuccessfulQueries
	def getQueryString(self, numQuery, connectionname=None, numRun=0):
		"""
		Returns query string.
		This might depend on the number of benchmark run and on the connection.

		:param numQuery: Number of query
		:param connectionname: Name of connection
		:param numRun: Number of benchmark run
		:return: String of (SQL) query
		"""
		q = self.queries[numQuery-1]
		query = tools.query(q)
		if len(query.queryList) > 0 and len(self.protocol['query'][str(numQuery)]['runs']) > 0:
			queryString = self.getQueryString(query.queryList[numRun % len(query.queryList)], connectionname, self.protocol['query'][str(numQuery)]['runs'][numRun])
			return queryString
		queryString = query.query
		# overwrite default query string with dialect
		if connectionname is not None and len(query.DBMS) > 0 and 'dialect' in self.dbms[connectionname].connectiondata:
			for c, q in query.DBMS.items():
				if self.dbms[connectionname].connectiondata['dialect'] == c:
					queryString = q
		# overwrite default query string with variant matching the beginning of the name of connection
		if connectionname is not None and len(query.DBMS) > 0:
			for c, q in query.DBMS.items():
				if connectionname.startswith(c):
					queryString = q
		def parametrize(queryTemplate, numQuery, numRun):
			params = self.protocol['query'][str(numQuery)]['parameter'][numRun]
			params['numRun'] = numRun
			params = tools.joinDicts(params, parameter.defaultParameters)
			return queryTemplate.format(**params)
		# if query is given as a list of strings (create view, ..., drop view)
		if isinstance(queryString,list):
			queryPart = []
			for queryTemplate in queryString:
				if len(self.protocol['query'][str(numQuery)]['parameter']) > 0:
					queryPart.append(parametrize(queryTemplate, numQuery, numRun))
			#print(queryPart)
			queryString = queryPart
		else:
			if len(self.protocol['query'][str(numQuery)]['parameter']) > 0:
				queryString = parametrize(queryString, numQuery, numRun)
		# it is a query template
		#if len(self.protocol['query'][str(numQuery)]['parameter']) > 0:
		#	bParametrized = True
		#	#queryTemplate = queryString
		#	#params = self.protocol['query'][str(numQuery)]['parameter'][numRun]
		#	#queryString = queryTemplate.format(**params)
		#	queryString = parametrize(queryString, numQuery, numRun)
		#else:
		#	bParametrized = False
		return queryString
	def getRandomRun(self, numQuery):#, connectionname, numRun=None):
		q = self.queries[numQuery-1]
		query = tools.query(q)
		numRun = math.floor(random.uniform(0,query.numRun))
		#queryString = self.getQueryString(numQuery, connectionname=connectionname, numRun=numRun)
		#print(queryString)
		#return queryString, 
		return numRun
	def getConnectionManager(self, numQuery, connectionname):
		# connection management for parallel connections
		q = self.queries[numQuery-1]
		# prepare query object
		query = tools.query(q)
		#print(self.connectionmanagement)
		numProcesses = self.connectionmanagement['numProcesses']#self.numProcesses
		batchsize = self.connectionmanagement['runsPerConnection']#self.runsPerConnection
		timeout = self.connectionmanagement['timeout']#self.timeout
		singleConnection = self.connectionmanagement['singleConnection']#self.timeout
		# overwrite by benchmark (query file)
		if 'connectionmanagement' in self.queryconfig:
			connectionmanagement = self.queryconfig['connectionmanagement']
			if('numProcesses' in connectionmanagement):# and connectionmanagement['numProcesses'] != 0):
				numProcesses = connectionmanagement['numProcesses']
			if('runsPerConnection' in connectionmanagement):# and connectionmanagement['runsPerConnection'] != 0):
				# 0=unlimited
				batchsize = connectionmanagement['runsPerConnection']
			if('timeout' in connectionmanagement):# and connectionmanagement['timeout'] != 0):
				# 0=unlimited
				timeout = connectionmanagement['timeout']
			if('singleConnection' in connectionmanagement):# and connectionmanagement['timeout'] != 0):
				singleConnection = connectionmanagement['singleConnection']
		# overwrite by connection
		if 'connectionmanagement' in self.dbms[connectionname].connectiondata:
			connectionmanagement = self.dbms[connectionname].connectiondata['connectionmanagement']
			if('numProcesses' in connectionmanagement):# and connectionmanagement['numProcesses'] != 0):
				numProcesses = connectionmanagement['numProcesses']
			if('runsPerConnection' in connectionmanagement):# and connectionmanagement['runsPerConnection'] != 0):
				# 0=unlimited
				batchsize = connectionmanagement['runsPerConnection']
			if('timeout' in connectionmanagement):# and connectionmanagement['timeout'] != 0):
				# 0=unlimited
				timeout = connectionmanagement['timeout']
			if('singleConnection' in connectionmanagement):# and connectionmanagement['timeout'] != 0):
				singleConnection = connectionmanagement['singleConnection']
		# overwrite by query
		if 'connectionmanagement' in q:
			connectionmanagement = q['connectionmanagement']
			if('numProcesses' in connectionmanagement):# and connectionmanagement['numProcesses'] != 0):
				numProcesses = connectionmanagement['numProcesses']
			if('runsPerConnection' in connectionmanagement):# and connectionmanagement['runsPerConnection'] != 0):
				# 0=unlimited
				batchsize = connectionmanagement['runsPerConnection']
			if('timeout' in connectionmanagement):# and connectionmanagement['timeout'] != 0):
				# 0=unlimited
				timeout = connectionmanagement['timeout']
		if numProcesses == 0 or numProcesses is None:
			numProcesses = 1
		if timeout == 0:
			timeout = None
		if batchsize == 0 or batchsize is None:
			batchsize = math.ceil(query.numRun/numProcesses)
		# unless pickling of java objects is possible
		# we cannot have global connections
		#singleConnection = False
		return {'numProcesses': numProcesses, 'runsPerConnection': batchsize, 'timeout': timeout, 'singleConnection': singleConnection}
	def runSingleBenchmarkRun(self, numQuery, connectionname, numRun=0):
		"""
		Runs a single benchmark run.
		This generates the actual query string and sends it to the connected dbms.

		:param numQuery: Number of query
		:param connectionname: Name of connection
		:param numRun: Number of benchmark run
		:return: String of (SQL) query
		"""
		queryString = self.getQueryString(numQuery, connectionname=connectionname, numRun=numRun)
		inputConfig = [singleRunInput(0, queryString, self.queries[numQuery-1])]
		output = singleRun(self.dbms[connectionname].connectiondata, inputConfig, [0], connectionname, numQuery, None, BENCHMARKER_VERBOSE_QUERIES=BENCHMARKER_VERBOSE_QUERIES, BENCHMARKER_VERBOSE_RESULTS=BENCHMARKER_VERBOSE_RESULTS)
		return output[0]
	def runSingleBenchmarkRunMultiple(self, numQuery, connectionname, numRun=0, times=1):
		"""
		Runs a single benchmark run multiple times.
		This generates the actual query string and sends it multiple times to the connected dbms.

		:param numQuery: Number of query
		:param connectionname: Name of connection
		:param numRun: Number of benchmark run
		:param times: Number of times the same query is sent
		:return: String of (SQL) query
		"""
		output = [self.runSingleBenchmarkRun(numQuery, connectionname, numRun) for i in range(times)]
		l_connect, l_execute, l_transfer, l_error, l_data, l_size = self.flattenResult(output)
		result = singleRunOutput()
		result.durationConnect = l_connect
		result.durationExecute = l_execute
		result.durationTransfer = l_transfer
		result.error = l_error
		result.data = l_data
		result.size = l_size
		return result
	def flattenResult(self, lists):
		l_connect = [l.durationConnect for l in lists]
		l_execute = [l.durationExecute for l in lists]
		l_transfer = [l.durationTransfer for l in lists]
		l_error = [l.error for l in lists]
		l_data = [l.data for l in lists]
		l_columnnames = [l.columnnames for l in lists]
		l_size = [l.size for l in lists]
		def output(l):
			#print(l)
			print('Num: '+str(len(l)))
			print('Min: '+str(min(l)))
			print('Max: '+str(max(l)))
			print('Avg: '+str(sum(l)/len(l)))
		if BENCHMARKER_VERBOSE_STATISTICS:
			print("Connect:")
			output(l_connect)
			print("Execute:")
			output(l_execute)
			print("Transfer:")
			output(l_transfer)
			#print("Error:")
			#print(l_error)
			print("Size:")
			#print(l_size)
			output(l_size)
			#print("Data:")
			#print(l_data)
			#print("Column names:")
			#print(l_columnnames)
		return l_connect, l_execute, l_transfer, l_error, l_data, l_columnnames, l_size
	def runBenchmark(self, numQuery, connectionname):
		"""
		Performs a benchmark run (fixed query and connection) and stores results.
		This only happens if we haven't already benchmarked that pair or it is explicitly wished to rerun the benchmark.

		:param numQuery: Number of query to benchmark
		:param connectionname: Name of connection to benchmark
		:return: True if benchmark has been done, False if skipped
		"""
		#global activeConnections
		# check if benchmark should be done
		if self.timerExecution.checkForSuccessfulBenchmarks(numQuery, connectionname):
			# benchmark already done
			if not self.overwrite or (self.fixedQuery is not None and self.fixedQuery != numQuery) or (self.fixedConnection is not None and self.fixedConnection != connectionname):
				# rerun not this benchmark
				self.logger.debug("Benchmarks of Q"+str(numQuery)+" at dbms "+connectionname+" already done")
				return False
			else:
				# rerun specified
				print("Rerun benchmarks of Q"+str(numQuery)+" at dbms "+connectionname)
		else:
			# not been done
			if not (self.fixedQuery is None and self.fixedConnection is None):
				# only run specific benchmark
				if (self.fixedQuery is not None and self.fixedQuery != numQuery) or (self.fixedConnection is not None and self.fixedConnection != connectionname):
					# not this benchmark
					self.logger.debug("Benchmarks of Q"+str(numQuery)+" at dbms "+connectionname+" not wanted right now")
					return False
		# prepare basic setting
		self.logger.info("Starting benchmarks of Q"+str(numQuery)+" at dbms "+connectionname)
		self.startBenchmarkingQuery(numQuery)
		q = self.queries[numQuery-1]
		c = connectionname
		print("Connection: "+connectionname)
		# prepare multiprocessing
		logger = mp.log_to_stderr()
		logger.setLevel(logging.INFO)
		# prepare query object
		query = tools.query(q)
		# connection management for parallel connections
		connectionmanagement = self.getConnectionManager(numQuery, c)
		numProcesses = connectionmanagement['numProcesses']#self.numProcesses
		batchsize = connectionmanagement['runsPerConnection']#self.runsPerConnection
		timeout = connectionmanagement['timeout']#self.timeout
		if timeout is not None:
			jaydebeapi.QUERY_TIMEOUT = timeout
		singleConnection = connectionmanagement['singleConnection']
		# overwrite by connection
		#if 'connectionmanagement' in self.dbms[c].connectiondata:
		#	connectionmanagement = self.dbms[c].connectiondata['connectionmanagement']
		#	if('numProcesses' in connectionmanagement and connectionmanagement['numProcesses'] != 0):
		#		numProcesses = self.dbms[c].connectiondata['connectionmanagement']['numProcesses']
		#	if('runsPerConnection' in connectionmanagement):# and connectionmanagement['runsPerConnection'] != 0):
		#		# 0=unlimited
		#		batchsize = self.dbms[c].connectiondata['connectionmanagement']['runsPerConnection']
		#	if('timeout' in connectionmanagement):# and connectionmanagement['timeout'] != 0):
		#		# 0=unlimited
		#		timeout = self.dbms[c].connectiondata['connectionmanagement']['timeout']
		#if numProcesses == 0:
		#	numProcesses = 1
		#if timeout == 0:
		#	timeout = None
		#if batchsize == 0:
		#	batchsize = math.ceil(query.numRun/numProcesses)
		numBatches = math.ceil(query.numRun/batchsize)
		runs = list(range(0,query.numRun))
		# prepare protocol for result data
		if c not in self.protocol['query'][str(numQuery)]['resultSets']:
			self.protocol['query'][str(numQuery)]['resultSets'][c] = []
		# prepare protocol for errors
		if c not in self.protocol['query'][str(numQuery)]['errors']:
			self.protocol['query'][str(numQuery)]['errors'][c] = ""
		# prepare protocol for warnings
		if c not in self.protocol['query'][str(numQuery)]['warnings']:
			self.protocol['query'][str(numQuery)]['warnings'][c] = ""
		# prepare protocol for duration
		if c not in self.protocol['query'][str(numQuery)]['durations']:
			self.protocol['query'][str(numQuery)]['durations'][c] = 0.0
		# prepare protocol for sizes
		if c not in self.protocol['query'][str(numQuery)]['sizes']:
			self.protocol['query'][str(numQuery)]['sizes'][c] = 0.0
		# skip query if not active
		if not query.active:
			self.logger.info("Benchmarks of Q"+str(numQuery)+" at dbms "+connectionname+" is not active")
			# this stores empty values as placeholder - query list is a "list"
			self.timerExecution.skipTimer(numQuery, query, connectionname)
			self.timerTransfer.skipTimer(numQuery, query, connectionname)
			self.timerConnect.skipTimer(numQuery, query, connectionname)
			self.stopBenchmarkingQuery(numQuery)
			return False
		# skip connection if not active
		if not self.dbms[c].connectiondata['active']:
			self.logger.info("Benchmarks of Q"+str(numQuery)+" at dbms "+connectionname+" is not active")
			# this stores empty values as placeholder
			self.timerExecution.skipTimer(numQuery, query, connectionname)
			self.timerTransfer.skipTimer(numQuery, query, connectionname)
			self.timerConnect.skipTimer(numQuery, query, connectionname)
			self.stopBenchmarkingQuery(numQuery)
			return False
		# always reset parts of protocol
		self.protocol['query'][str(numQuery)]['resultSets'][c] = []
		self.protocol['query'][str(numQuery)]['errors'][c] = ""
		self.protocol['query'][str(numQuery)]['warnings'][c] = ""
		# dump settings
		if BENCHMARKER_VERBOSE_PROCESS:
			self.logger.info("runsPerConnection: "+str(batchsize))
			self.logger.info("numBatches: "+str(numBatches))
			self.logger.info("numProcesses: "+str(numProcesses))
			self.logger.info("timeout: "+str(timeout))
			self.logger.info("singleConnection: "+str(singleConnection))
		# Patch: if singleConnection only with single process
		if singleConnection:
			numProcesses = 1
		if singleConnection and len(self.activeConnections) < numProcesses:
			self.logger.info("More active connections from {} to {}".format(len(self.activeConnections), numProcesses))
			for i in range(len(self.activeConnections), numProcesses):
				self.activeConnections.append(tools.dbms(self.dbms[connectionname].connectiondata))
				self.logger.info("Establish global connection #"+str(i))
				self.activeConnections[i].connect()
		# do we want to keep result sets? (because of mismatch)
		keepResultsets = False
		# do we want to cancel / abort loop over benchmarks?
		breakLoop = False
		try:
			# start connecting
			self.timerExecution.startTimer(numQuery, query, connectionname)
			self.timerTransfer.startTimer(numQuery, query, connectionname)
			if not query.withConnect:
				# we do not benchmark connection time, so we connect directly and once
				self.timerConnect.skipTimer(numQuery, query, connectionname)
				self.connectDBMS(c)
			else:
				self.timerConnect.startTimer(numQuery, query, connectionname)
			#queryString = query.query
			#if c in query.DBMS:
			#	queryString = query.DBMS[c]
			#self.logger.debug(queryString)
			# it is a query template
			if len(self.protocol['query'][str(numQuery)]['parameter']) > 0 or len(self.protocol['query'][str(numQuery)]['runs']) > 0:
				#queryTemplate = queryString
				bParametrized = True
			else:
				bParametrized = False
			self.protocol['query'][str(numQuery)]['errors'][c] = []
			# tqdm does not allow break
			if self.bBatch:
				range_runs = range(0, query.numRun)
			else:
				range_runs = tqdm(range(0, query.numRun))
			# prepare input data for processes
			inputConfig = []
			for i in range(query.numRun):
				#print(self.protocol['query'][str(numQuery)]['runs'])
				# replace parameter in query template
				#if len(self.protocol['query'][str(numQuery)]['runs']) > 0:
				#	queryString = self.getQueryString(query.queryList[i % len(query.queryList)], c, self.protocol['query'][str(numQuery)]['runs'][i])
				#else:
				#	queryString = self.getQueryString(numQuery, c, i)
				queryString = self.getQueryString(numQuery, c, i)
				#print(queryString)
				#self.logger.debug(queryString) # shown by singleRun?
				inputConfig.append(singleRunInput(i, queryString, self.queries[numQuery-1]))
			lists = []
			# perform required number of warmup and benchmark runs of query
			durationBenchmark = 0.0
			start = default_timer()
			# store start time for query / connection
			self.protocol['query'][str(numQuery)]['starts'][c] = str(datetime.datetime.now())
			# pooling
			if not singleConnection:
				if self.pool is not None:
					print("POOL1")
					#multiple_results = [self.pool.apply_async(singleRun, (self.dbms[c].connectiondata, inputConfig, runs[i*batchsize:(i+1)*batchsize], connectionname, numQuery, self.path, JPickler.dumps(self.activeConnections))) for i in range(numBatches)]
					multiple_results = [self.pool.apply_async(singleRun, (self.dbms[c].connectiondata, inputConfig, runs[i*batchsize:(i+1)*batchsize], connectionname, numQuery, self.path, [], BENCHMARKER_VERBOSE_QUERIES, BENCHMARKER_VERBOSE_RESULTS, BENCHMARKER_VERBOSE_PROCESS)) for i in range(numBatches)]
					lists = [res.get(timeout=timeout) for res in multiple_results]
					lists = [i for j in lists for i in j]
				else:
					with mp.Pool(processes=numProcesses) as pool:
						print("POOL2")
						#multiple_results = [pool.apply_async(singleRun, (self.dbms[c].connectiondata, inputConfig, runs[i*batchsize:(i+1)*batchsize], connectionname, numQuery, self.path, JPickler.dumps(self.activeConnections))) for i in range(numBatches)]
						multiple_results = [pool.apply_async(singleRun, (self.dbms[c].connectiondata, inputConfig, runs[i*batchsize:(i+1)*batchsize], connectionname, numQuery, self.path, [], BENCHMARKER_VERBOSE_QUERIES, BENCHMARKER_VERBOSE_RESULTS, BENCHMARKER_VERBOSE_PROCESS)) for i in range(numBatches)]
						lists = [res.get(timeout=timeout) for res in multiple_results]
						lists = [i for j in lists for i in j]
			else:
				# no parallel processes because JVM does not parallize
				# time the queries and stop early if maxTime is reached
				if BENCHMARKER_VERBOSE_PROCESS:
					self.logger.info("We have {} active connections".format(len(self.activeConnections)))
				start_time_queries = default_timer()
				lists = []
				for i in range(numBatches):
					lists_batch = singleRun(self.dbms[c].connectiondata, inputConfig, runs[i*batchsize:(i+1)*batchsize], connectionname, numQuery, self.path, self.activeConnections, BENCHMARKER_VERBOSE_QUERIES, BENCHMARKER_VERBOSE_RESULTS, BENCHMARKER_VERBOSE_PROCESS)
					lists.extend(lists_batch)
					end_time_queries = default_timer()
					duration_queries = (end_time_queries - start_time_queries)
					if query.maxTime is not None and query.maxTime < duration_queries:
						# fill with zero? affects statistics
						self.logger.info("Reached maxTime={}s after {}s".format(query.maxTime, duration_queries))
						self.logger.info("We have received {} query results, so {} are missing and will be filled up".format(len(lists), query.numRun-len(lists)))
						lists_empty = [singleRunOutput() for i in range(query.numRun-len(lists))]
						lists.extend(lists_empty)
						break
				# we do not close connections per query but per workload
				#for con in self.activeConnections:
				#	print("Closed connection")
				#	con.disconnect()
				#self.activeConnections = []
			# store end time for query / connection
			end = default_timer()
			durationBenchmark = 1000.0*(end - start)
			self.protocol['query'][str(numQuery)]['ends'][c] = str(datetime.datetime.now())
			#pool.close()
			#pool.join()
			#lists = [res.get() for res in multiple_results]
			l_connect, l_execute, l_transfer, l_error, l_data, l_columnnames, l_size = self.flattenResult(lists)
			error = ""
			for i in range(len(l_error)):
				if len(l_error[i]) > 0:
					error = l_error[i]
					break
			if len(error):
				self.logger.info(error)
			self.timerConnect.time_c = l_connect
			self.timerExecution.time_c = l_execute
			self.timerTransfer.time_c = l_transfer
			self.protocol['query'][str(numQuery)]['durations'][c] = durationBenchmark
			self.protocol['query'][str(numQuery)]['errors'][c] = error
			# prepare input data for processing result sets
			inputConfig = []
			for i in range(query.numRun):
				inputConfig.append(singleResultInput(i, l_data[i], l_columnnames[i], self.queries[numQuery-1]))
			#print(inputConfig)
			lists = []
			numProcesses_cpu = mp.cpu_count()
			batchsize_data = 1
			numBatches_data = math.ceil(query.numRun/batchsize_data)
			runs_data = list(range(0,query.numRun))
			numProcesses_data = min(numProcesses_cpu, numBatches_data)
			if query.storeData != False:
				if numProcesses_data == 1:
					# single result set
					if BENCHMARKER_VERBOSE_PROCESS:
						self.logger.info("Process {} runs in {} batches of size {} within this processes".format(query.numRun, numBatches_data, batchsize_data, numProcesses_data))
					i = 0
					lists = singleResult(self.dbms[c].connectiondata, inputConfig, runs[i*batchsize_data:(i+1)*batchsize_data], connectionname, numQuery, self.path)
				else:
					# several result sets
					# process sequentially
					if BENCHMARKER_VERBOSE_PROCESS:
						self.logger.info("Process {} runs in {} batches of size {} within this processes sequentially".format(query.numRun, numBatches_data, batchsize_data))
					lists = []
					for i in range(numBatches_data):
						lists_batch = singleResult(self.dbms[c].connectiondata, inputConfig, runs[i*batchsize_data:(i+1)*batchsize_data], connectionname, numQuery, self.path)
						lists.extend(lists_batch)
					# process in parallel
					"""
					self.logger.info("Process {} runs in {} batches of size {} with a pool of {} processes".format(query.numRun, numBatches_data, batchsize_data, numProcesses_data))
					with mp.Pool(processes=numProcesses_data) as pool:
						multiple_results = [pool.apply_async(singleResult, (self.dbms[c].connectiondata, inputConfig, runs[i*batchsize_data:(i+1)*batchsize_data], connectionname, numQuery, self.path)) for i in range(numBatches_data)]
						lists = [res.get(timeout=timeout) for res in multiple_results]
						lists = [i for j in lists for i in j]
					"""
				l_data = [l.data for l in lists]
				l_size = [l.size for l in lists]
			#print("Size:")
			#print(l_size)
			#print("Data:")
			#print(l_data)
			size = int(sum(l_size))
			if BENCHMARKER_VERBOSE_PROCESS:
				self.logger.info("Size of data storage: "+str(size))
			self.protocol['query'][str(numQuery)]['sizes'][c] = size
			# result set of query / connection
			# only for comparion
			# will be dropped if comparison is successful
			self.protocol['query'][str(numQuery)]['resultSets'][c] = l_data
			if len(self.protocol['query'][str(numQuery)]['errors'][c]) == 0:
				if not bParametrized:
					# shall be constant for all runs
					for i in range(len(l_data)):
						if not l_data[i] == l_data[0]:
							#print("Received data %i:" % i)
							#print(l_data[i])
							#print("Received data 0:")
							#print(l_data[0])
							self.protocol['query'][str(numQuery)]['warnings'][c] = 'NumRun '+str(i+1)+': Received inconsistent result set'
							self.logger.debug('Received differing result set')
							keepResultsets = True
							break
			if bParametrized:
				# own data store for each run
				dataIndex = len(l_data)
				data = l_data
			else:
				# all runs have same data store
				dataIndex = 1
				data = [l_data[0]]
			# store result set for query only
			# shall be the same for all connections
			#print(self.protocol['query'][str(numQuery)]['dataStorage'])
			if len(self.protocol['query'][str(numQuery)]['dataStorage']) < dataIndex:
				self.protocol['query'][str(numQuery)]['dataStorage'].extend(data)
			else:
				numRunStorage = len(self.protocol['query'][str(numQuery)]['dataStorage'])
				numRunReceived = len(l_data)
				if BENCHMARKER_VERBOSE_STATISTICS:
					self.logger.info("NumRuns in Storage: "+str(numRunStorage))
					self.logger.info("NumRuns received: "+str(numRunReceived))
				if len(self.protocol['query'][str(numQuery)]['errors'][c]) == 0:
					if BENCHMARKER_VERBOSE_STATISTICS:
						print("NumRuns to compare: "+str(dataIndex))
					for i in range(dataIndex):
						#print("Stored data #%i:" % i)
						#print(self.protocol['query'][str(numQuery)]['dataStorage'][i])#, floatfmt=".10f"))
						#print("Received data #%i:" % i)
						#print(l_data[i])
						if not l_data[i] == self.protocol['query'][str(numQuery)]['dataStorage'][i]:
							self.protocol['query'][str(numQuery)]['warnings'][c] = 'NumRun '+str(i+1)+': Received differing result set'
							self.logger.debug('Received differing result set')
							keepResultsets = True
							break
							#raise ValueError('Received differing result set')
			#if len(self.resultfolder_subfolder) > 0:
			# always store complete resultset for subfolders
			filename = self.path+"/query_"+str(numQuery)+"_resultset_complete_"+connectionname+".pickle"
			if BENCHMARKER_VERBOSE_PROCESS:
				self.logger.info("Store pickle of complete result set to "+filename)
			f = open(filename, "wb")
			pickle.dump(data, f)
			f.close()
		except Exception as e:
			self.logger.exception('Caught an error: %s' % str(e))
			self.protocol['query'][str(numQuery)]['errors'][c] = 'ERROR ({}) - {}'.format(type(e).__name__, e)
			# store end time for query / connection
			self.protocol['query'][str(numQuery)]['ends'][c] = str(datetime.datetime.now())
			# benchmark is 0 due to error
			self.timerExecution.abortTimerRun()
			self.timerTransfer.abortTimerRun()
			if query.withConnect:
				# we do benchmark connection time, so we connect every run
				self.timerConnect.abortTimerRun()
			# this means ignore benchmark for this query/connection due to error
			self.timerExecution.cancelTimer()
			self.timerTransfer.cancelTimer()
			if query.withConnect:
				# we do benchmark connection time, so we connect every run
				self.timerConnect.cancelTimer()
			# this means store results even if error happend
			#self.timerExecution.abortTimer()
			#self.timerTransfer.abortTimer()
			# tqdm does not support break:
			# https://github.com/tqdm/tqdm/issues/613
			breakLoop = True
		finally:
			self.timerExecution.finishTimer()
			self.timerTransfer.finishTimer()
			if query.withConnect:
				# we do benchmark connection time, so we connect every run
				#self.disconnectDBMS(c)
				self.timerConnect.finishTimer()
		if not keepResultsets:
			self.protocol['query'][str(numQuery)]['resultSets'][c] = []
		self.stopBenchmarkingQuery(numQuery)
		#if self.dbms[c].hasHardwareMetrics():
		#	metricsReporter = monitor.metrics(self)
		#	metricsReporter.generatePlotForQuery(numQuery)
		return True
	def generateAllParameters(self):
		for numQuery in range(1, len(self.queries)+1):
			q = self.queries[numQuery-1]
			query = tools.query(q)
			self.logger.debug("generateAllParameters query={}, parameter={}, protocol={}".format(numQuery, query.parameter, self.protocol['query'][str(numQuery)]['parameter']))
			if len(query.parameter) > 0 and len(self.protocol['query'][str(numQuery)]['parameter']) == 0:
				params = parameter.generateParameters(query.parameter, query.numRun)
				self.protocol['query'][str(numQuery)]['parameter'] = params
	def startBenchmarkingQuery(self, numQuery):
		"""
		Starts protocol for that specific query.
		Generates parameters.

		:param numQuery: Number of query to benchmark
		:return: returns nothing
		"""
		if self.protocol['query'][str(numQuery)]['start'] == "":
			self.protocol['query'][str(numQuery)]['start'] = str(datetime.datetime.now())
		self.start_query = timer()
		q = self.queries[numQuery-1]
		query = tools.query(q)
		if len(query.parameter) > 0 and len(self.protocol['query'][str(numQuery)]['parameter']) == 0:
			self.logger.debug("generateParameters query={}, parameter={}, protocol={}".format(numQuery, query.parameter, self.protocol['query'][str(numQuery)]))
			params = parameter.generateParameters(query.parameter, query.numRun)
			self.protocol['query'][str(numQuery)]['parameter'] = params
		if len(query.queryList) > 0:
			numRunList = []
			for queryElement in range(query.numRun): # query.queryList:
				numRun = self.getRandomRun(query.queryList[queryElement % len(query.queryList)])#, connectionname)
				numRunList.append(numRun)
			self.protocol['query'][str(numQuery)]['runs'] = numRunList
		print("Benchmarking Q"+str(numQuery)+': '+query.title)
	def stopBenchmarkingQuery(self, numQuery):
		"""
		Writes collected data to protocol for that specific query.

		:param numQuery: Number of query to benchmark
		:return: returns nothing
		"""
		end_query = timer()
		duration_query = end_query - self.start_query
		# add to protocol
		self.protocol['query'][str(numQuery)]['duration'] += 1000.0*duration_query
		self.protocol['query'][str(numQuery)]['end'] = str(datetime.datetime.now())
	def runBenchmarksQuery(self):
		"""
		Performs querywise benchmark runs.
		Stores results and generates reports immediately after completion of a query (all connections, all runs).

		:return: returns nothing
		"""
		for numQuery in range(1, len(self.queries)+1):
			if self.overwrite and not (self.fixedQuery is not None and self.fixedQuery != numQuery):# or (self.fixedConnection is not None and self.fixedConnection != connectionname):
				# rerun this query
				self.cleanProtocol(numQuery)
			for c in sorted(self.dbms.keys()):
				# run benchmark, current query and connection
				bBenchmarkDoneForThisQuery = self.runBenchmark(numQuery, c)
				# if benchmark has been done: store and generate reports
				if bBenchmarkDoneForThisQuery:
					# store results
					self.reporterStore.generate(numQuery, [self.timerExecution, self.timerTransfer, self.timerConnect])
					if not self.bBatch:
						# generate reports
						for r in self.reporter:
							r.init()
							r.generate(numQuery, [self.timerExecution, self.timerTransfer, self.timerConnect])
	def runBenchmarksConnection(self):
		"""
		Performs connectionwise benchmark runs.
		Stores results and generates reports immediately after completion of a query (all runs).

		:return: returns nothing
		"""
		for c in sorted(self.dbms.keys()):
			# check if we need a global connection
			singleConnection = False
			if 'connectionmanagement' in self.queryconfig:
				connectionmanagement = self.queryconfig['connectionmanagement']
				if('singleConnection' in connectionmanagement):# and connectionmanagement['timeout'] != 0):
					singleConnection = connectionmanagement['singleConnection']
					if singleConnection:
						# we assume all queries should share a connection
						numProcesses = 1
						i = 0
						connectionname = c
						print("More active connections from {} to {}".format(len(self.activeConnections), numProcesses))
						self.activeConnections.append(tools.dbms(self.dbms[connectionname].connectiondata))
						print("Establish global connection #"+str(i))
						self.activeConnections[i].connect()
			# work queries
			for numQuery in range(1, len(self.queries)+1):
				bBenchmarkDone = self.runBenchmark(numQuery, c)
				# if benchmark has been done: store and generate reports
				if bBenchmarkDone:
					# store results
					self.reporterStore.generate(numQuery, [self.timerExecution, self.timerTransfer, self.timerConnect])
					if not self.bBatch:
						# generate reports
						for r in self.reporter:
							r.init()
							r.generate(numQuery, [self.timerExecution, self.timerTransfer, self.timerConnect])
			# close global connection
			if singleConnection:
				print("Closed connection for", connectionname)
				self.activeConnections[i].disconnect()
				self.activeConnections = []
	def runBenchmarks(self):
		"""
		Runs benchmarks or possibly reruns specific benchmarks.
		Generates reports.

		:return: returns nothing
		"""
		# clean evaluation dict
		evaluator.evaluator.evaluation = {}
		if self.working == 'query':
			self.runBenchmarksQuery()
		else:
			self.runBenchmarksConnection()
		if self.bBatch:
			# generate reports at the end only
			self.generateReportsAll()
	def readResultfolder(self):
		"""
		Reads data of previous benchmark from folder.

		:return: returns nothing
		"""
		self.clearBenchmarks()
		# read from stored results
		self.logger.debug("Read from "+self.path)
		self.reporterStore.readProtocol()
		for numQuery,q in enumerate(self.queries):
			query = tools.query(q)
			loaded = self.reporterStore.load(query, numQuery+1, [self.timerExecution, self.timerTransfer, self.timerConnect])
			if not loaded:
				break
		# show finished benchmarks
		"""
		for numQuery,q in enumerate(self.timerExecution.times):
			self.logger.debug("Q"+str(numQuery+1))
			numConnection = 1
			#if len(q) > 0:
			for c, v in q.items():
				self.logger.debug("C"+str(numConnection)+" "+c+"="+str(len(v))+" runs")
				numConnection = numConnection + 1
		"""
	def readBenchmarks(self):
		"""
		Reads data of previous benchmark from folder.
		Generates all reports.

		:return: returns nothing
		"""
		self.readResultfolder()
		# generate reports
		self.generateReportsAll()
	def computeTimerRun(self):
		"""
		Adds a timer for total time per run.

		:return: returns nothing
		"""
		self.timerRun = tools.timer("run")
		self.timerRun.stackable = False
		for q,t in enumerate(self.timerExecution.times):
			#print("Q"+str(q+1))
			query = tools.query(self.queries[q])
			#print(query.timer['sum']['active'])
			self.timerRun.times.append({})
			self.timerRun.stats.append({})
			for c,times in t.items():
				#print(self.timerExecution.times[q][c])
				#print(self.timerTransfer.times[q][c])
				#print(self.timerConnect.times[q][c])
				l = self.timerExecution.times[q][c]
				def addition_no_null(x,y):
					return x+y if x is not None and y is not None else None
				if c in self.timerTransfer.times[q]:
					l = list(map(addition_no_null, l, self.timerTransfer.times[q][c]))
				if c in self.timerConnect.times[q]:
					l = list(map(addition_no_null, l, self.timerConnect.times[q][c]))
				#l = list(map(add, list(map(add, self.timerExecution.times[q][c], self.timerTransfer.times[q][c])), self.timerConnect.times[q][c]))
				#print(l)
				self.timerRun.times[q][c] = l
				self.timerRun.stats[q][c] = self.timerRun.getStats(l[query.numRunBegin:query.numRunEnd])
		#self.timers = [self.timerRun] + self.timers#, self.timerExecution, self.timerTransfer, self.timerConnect]
		self.timers = [self.timerSession, self.timerRun, self.timerExecution, self.timerTransfer, self.timerConnect]
	def computeTimerSession(self):
		"""
		Adds a timer for total time per run.

		:return: returns nothing
		"""
		self.timerSession = tools.timer("session")
		self.timerSession.stackable = False
		self.timerSession.perRun = False
		for q,t in enumerate(self.timerExecution.times):
			#print("Q"+str(q+1))
			query = tools.query(self.queries[q])
			self.timerSession.times.append({})
			self.timerSession.stats.append({})
			#if query.warmup > 0:
			#	continue
			for c,times in t.items():
				#print(self.timerExecution.times[q][c])
				#print(self.timerTransfer.times[q][c])
				#print(self.timerConnect.times[q][c])
				def addition_no_null(x,y):
					return x+y if x is not None and y is not None else None
				l = self.timerExecution.times[q][c]
				if c in self.timerTransfer.times[q]:
					l = list(map(addition_no_null, l, self.timerTransfer.times[q][c]))
				if c in self.timerConnect.times[q]:
					l = list(map(addition_no_null, l, self.timerConnect.times[q][c]))
				#print(l)
				connectionmanagement = self.getConnectionManager(q+1, c)
				batchsize = connectionmanagement['runsPerConnection']#self.runsPerConnection
				numBatches = math.ceil(query.numRun/batchsize)
				singleConnection = connectionmanagement['singleConnection']
				if singleConnection:
					batchsize = len(l)
					numBatches = 1
				#print(l)
				#print(batchsize)
				#print(numBatches)
				# aggregation changes number of results (warmup!)
				l_agg = [sum(filter(None,l[i*batchsize:(i+1)*batchsize])) for i in range(numBatches)]
				self.timerSession.times[q][c] = l_agg
				self.timerSession.stats[q][c] = self.timerSession.getStats(l_agg)
		#self.timers = [self.timerSession] + self.timers#, self.timerExecution, self.timerTransfer, self.timerConnect]
		self.timers = [self.timerSession, self.timerRun, self.timerExecution, self.timerTransfer, self.timerConnect]
	def generateReportsAll(self):
		"""
		Generates all reports.

		:return: returns nothing
		"""
		self.computeTimerRun()
		self.computeTimerSession()
		# forget and recompute hardware metrics
		monitor.metrics.m_avg = None
		for r in self.reporter:
			print("Report "+type(r).__name__)
			r.generateAll(self.timers)#[self.timerExecution, self.timerTransfer, self.timerConnect])
	def continueBenchmarks(self, overwrite = False):
		"""
		Reads data of previous benchmark from folder.
		Continues performing missing benchmarks, if not all queries were treated completely.

		:param overwrite: True if existing benchmarks should be overwritten
		:return: returns nothing
		"""
		self.overwrite = overwrite
		self.readResultfolder()
		self.runBenchmarks()
	def getResultSetCSV(self, query, connection):
		filename=self.path+"/query_"+str(query)+"_resultset_"+connection+".csv"
		if os.path.isfile(filename):
			df = pd.read_csv(filename)
			return df
		else:
			print("No result found")
	def getResultSetDF(self, query, connection):
		filename=self.path+"/query_"+str(query)+"_resultset_"+connection+".pickle"
		if os.path.isfile(filename):
			f = open(filename, "rb")
			result = pickle.load(f)
			f.close()
			return result
		else:
			print("No result found")
	def getBenchmarks(self, query):
		filename=self.path+"/query_"+str(query)+"_execution_dataframe.pickle"
		f = open(filename, "rb")
		result = pickle.load(f)
		f.close()
		return result
	def getBenchmarksCSV(self, query, timer="execution"):
		filename=self.path+"/query_"+str(query)+"_"+timer+".csv"
		df = pd.read_csv(filename)
		df_t = df.transpose()
		return df_t
	def getStatistics(self, query):
		filename=self.path+"/query_"+str(query)+"_execution_statistics.pickle"
		f = open(filename, "rb")
		result = pickle.load(f)
		f.close()
		return result
	def listQueriesSuccess(self):
		# find position of execution timer
		e = [i for i,t in enumerate(self.timers) if t.name=="execution"]
		# list of active queries for timer e[0] = execution
		qs = tools.findSuccessfulQueriesAllDBMS(self, None, self.timers)[e[0]]
		# index +1 for public addressing
		qs = [q+1 for q in qs]
		return qs
	def listQueries(self):
		qs = [i for i,q in enumerate(self.queries) if 'active' in q and q['active']]
		# index +1 for public addressing
		qs = [q+1 for q in qs]
		return qs
	def listConnections(self):
		# list of active dbms
		cs = [i for i,q in self.dbms.items() if q.connectiondata['active']]
		return cs
	def getSumPerTimer(self):
		dataframe, title = tools.dataframehelper.sumPerTimer(self, numQuery=None, timer=self.timers)
		return dataframe, title
	def getProdPerTimer(self):
		dataframe, title = tools.dataframehelper.multiplyPerTimer(self, numQuery=None, timer=self.timers)
		return dataframe, title
	def getTotalTime(self):
		dataframe, title = tools.dataframehelper.totalTimes(self)
		dataframe.loc['Total']= dataframe.sum()
		return dataframe, title
	def getError(self, query, connection=None):
		if connection is None:
			return self.protocol['query'][str(query)]['errors']
		else:
			return self.protocol['query'][str(query)]['errors'][connection]
	def getWarning(self, query, connection=None):
		if connection is None:
			return self.protocol['query'][str(query)]['warnings']
		else:
			return self.protocol['query'][str(query)]['warnings'][connection]
	def printErrors(self):
		for numQuery in range(1, len(self.queries)+1):
			queryObject = tools.query(self.queries[numQuery-1])
			if not queryObject.active:
				continue
			print("Q"+str(numQuery))
			print(self.getError(numQuery))
	def printWarnings(self):
		for numQuery in range(1, len(self.queries)+1):
			queryObject = tools.query(self.queries[numQuery-1])
			if not queryObject.active:
				continue
			print("Q"+str(numQuery))
			print(self.getWarning(numQuery))
	def printDataStorageSizes(self):
		for numQuery in range(1, len(self.queries)+1):
			queryObject = tools.query(self.queries[numQuery-1])
			if not queryObject.active:
				continue
			print("Q"+str(numQuery))
			print(str(sys.getsizeof(self.protocol['query'][str(numQuery)]['dataStorage']))+" bytes")
	def readDataStorage(self, query, numRun=0):
		if str(query) in self.protocol['query'] and len(self.protocol['query'][str(query)]['dataStorage']) > numRun:
			df = pd.DataFrame(self.protocol['query'][str(query)]['dataStorage'][numRun])
			if not df.empty:
				# set column names
				df.columns = df.iloc[0]
				# remove first row
				df = df[1:]
		else:
			df = pd.DataFrame()
		return df
	def readResultSet(self, query, connection, numRun=0):
		df = pd.DataFrame(self.protocol['query'][str(query)]['resultSets'][connection][numRun])
		# set column names
		df.columns = df.iloc[0]
		# remove first row
		df = df[1:]
		return df
	def readResultSetDict(self, query):
		return self.protocol['query'][str(query)]['resultSets']
	def getQueryObject(self, query):
		return tools.query(self.queries[query-1])
	def runIsolatedQueryMultiple(self, connectionname, queryString, times=1):
		output = [self.runIsolatedQuery(connectionname, queryString) for i in range(times)]
		l_connect, l_execute, l_transfer, l_error, l_data, l_size = self.flattenResult(output)
		result = singleRunOutput()
		result.durationConnect = l_connect
		result.durationExecute = l_execute
		result.durationTransfer = l_transfer
		result.error = l_error
		result.data = l_data
		result.size = l_size
		return result
	def runIsolatedQuery(self, connectionname, queryString):
		query = {
			'numRun': 1,
			'withData': True,
			'query': queryString,
			'timer':
			{
				'datatransfer':
				{
					'active': True,
					'sorted': True,
					'compare': 'result',
					'store': 'dataframe',
					'precision': 4,
				},
				'connection':
				{
					'active': True,
				}
			}
		}
		input = singleRunInput(numRun=0, queryString=queryString, queryConfig=query)
		output = singleRun(connectiondata=self.dbms[connectionname].connectiondata,
			inputConfig=[input],
			numRuns=[0],
			connectionname=connectionname,
			numQuery=0,
			path=None,
			BENCHMARKER_VERBOSE_QUERIES=BENCHMARKER_VERBOSE_QUERIES,
			BENCHMARKER_VERBOSE_RESULTS=BENCHMARKER_VERBOSE_RESULTS,
			BENCHMARKER_VERBOSE_PROCESS=BENCHMARKER_VERBOSE_PROCESS)
		output = output[0]
		return output
	def runAndStoreIsolatedQuery(self, connectionname, queryString, queryName=None):
		print(connectionname)
		output = self.runIsolatedQuery(connectionname, queryString)
		df = tools.dataframehelper.resultsetToDataFrame(output.data)
		print(df)
		#print(df)
		if queryName is not None:
			filename = self.path+"/query_resultset_"+connectionname+"_"+queryName+".pickle"
			print("Store pickle of result set to "+filename)
			f = open(filename, "wb")
			pickle.dump(df, f)
			f.close()
		return df
	def getIsolatedResultset(self, connectionname, queryName):
		filename = self.path+"/query_resultset_"+connectionname+"_"+queryName+".pickle"
		f = open(filename, "rb")
		result = pickle.load(f)
		f.close()
		return result
	def getParameterDF(self, numQuery):
		listParameter = self.protocol['query'][str(numQuery)]['parameter']
		dataframeParameter = pd.DataFrame.from_records(listParameter)
		dataframeParameter.index=range(1,len(listParameter)+1)
		return dataframeParameter




class inspector(benchmarker):
	"""
	Class for inspecting done benchmarks
	"""
	def __init__(self, result_path, code, anonymize=False):
		benchmarker.__init__(self,result_path=result_path+"/"+str(code), anonymize=anonymize)
		self.getConfig()
		self.readResultfolder()
		print("Connections:")
		for c in self.connections:
			print(c['name'])
		print("Queries:")
		for i,q in enumerate(self.queries):
			if 'active' in q and q['active']:
				print(str(i)+": Q"+str(i+1)+" = "+q['title'])
