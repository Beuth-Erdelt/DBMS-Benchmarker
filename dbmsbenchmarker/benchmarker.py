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
from dbmsbenchmarker import tools, reporter, parameter, monitor, evaluator, inspector
import dbmsbenchmarker
import pprint
# for query timeout
import jaydebeapi
from types import SimpleNamespace

#activeConnections = []

BENCHMARKER_VERBOSE_QUERIES = False
BENCHMARKER_VERBOSE_STATISTICS = False
BENCHMARKER_VERBOSE_RESULTS = False
BENCHMARKER_VERBOSE_PROCESS = False
BENCHMARKER_VERBOSE_NONE = False

logger = mp.log_to_stderr(logging.WARNING)

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



def singleRun(connectiondata, inputConfig, numRuns, connectionname, numQuery, path=None, activeConnections = [], BENCHMARKER_VERBOSE_QUERIES=False, BENCHMARKER_VERBOSE_RESULTS=False, BENCHMARKER_VERBOSE_PROCESS=True, BENCHMARKER_VERBOSE_NONE=False):
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
    #import logging, multiprocessing
    #logging.basicConfig(format='%(asctime)s %(message)s')
    #logger = multiprocessing.log_to_stderr()
    #logger = multiprocessing.get_logger()
    #logger = logging.getLogger('singleRun')
    #logger.setLevel(logging.INFO)
    # init list of results
    results = []
    #print("HELLO!")
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
            print("Found active connection #"+str(numActiveConnection))
        connection = activeConnections[numActiveConnection]
        durationConnect = 0.0
    else:
        # look at first run to determine if there should be sleeping
        query = tools.query(inputConfig[numRuns[0]].queryConfig)
        if query.delay_connect > 0:
            if BENCHMARKER_VERBOSE_PROCESS:
                print("Delay Connection by "+str(query.delay_connect)+" seconds")
            time.sleep(query.delay_connect)
        # connect to dbms
        connection = tools.dbms(connectiondata)
        start = default_timer()
        connection.connect()
        end = default_timer()
        durationConnect = 1000.0*(end - start)
    if BENCHMARKER_VERBOSE_PROCESS:
        print(("singleRun batch size %i: " % len(numRuns)))
    if durationConnect > 0:
        if not BENCHMARKER_VERBOSE_NONE:
            print(("numRun %s: " % ("/".join([str(i+1) for i in numRuns])))+"connection [ms]: "+str(durationConnect))
    # perform runs for this connection
    for numRun in numRuns:
        workername = "numRun %i: " % (numRun+1)
        queryString = inputConfig[numRun].queryString
        #print(workername+queryString)
        query = tools.query(inputConfig[numRun].queryConfig)
        if query.delay_run > 0:
            if BENCHMARKER_VERBOSE_PROCESS:
                print(workername+"Delay Run by "+str(query.delay_run)+" seconds")
            time.sleep(query.delay_run)
        error = ""
        try:
            #start = default_timer()
            if BENCHMARKER_VERBOSE_QUERIES:
                #logger.info(type(queryString))
                if isinstance(queryString, list):
                    for queryPart in queryString:
                        print(workername+queryPart)
                else:
                    print(workername+queryString)
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
            #print(connection.getName())
            end = default_timer()
            durationExecute = 1000.0*(end - start)
            if not BENCHMARKER_VERBOSE_NONE:
                print(workername+"execution [ms]: "+str(durationExecute))
            # transfer
            data = []
            columnnames = []
            size = 0
            durationTransfer = 0
            if query.withData:
                if len(queryString) != 0:
                    start = default_timer()
                    # CHANGE: List of queries also receives 1 result (last query)
                    if False and isinstance(queryString, list):
                        data = []
                        columnnames = []
                        size = 0
                        durationTransfer = 0
                    else:
                        data=connection.fetchResult()
                        end = default_timer()
                        durationTransfer = 1000.0*(end - start)
                        if not BENCHMARKER_VERBOSE_NONE:
                            print(workername+"transfer [ms]: "+str(durationTransfer))
                        data = [[str(item).strip() for item in sublist] for sublist in data]
                        size = sys.getsizeof(data)
                        if not BENCHMARKER_VERBOSE_NONE:
                            print(workername+"Size of result list retrieved: "+str(size)+" bytes")
                        #self.logger.debug(data)
                        #pprint.pprint(connection.cursor.__dict__)
                        #pprint.pprint(connection.cursor.description)
                        try:
                            # read the column names from meta data labels
                            # for example: MySQL TPC-DS Q3 needs this
                            result_set = connection.cursor._rs
                            meta_data = result_set.getMetaData()
                            column_count = meta_data.getColumnCount()
                            columnnames = [[meta_data.getColumnLabel(i).upper() for i in range(1, column_count + 1)]]
                            #print("Column aliases or names:", columnnames)
                        except Exception as e:
                            # take the column names as provided directly
                            columnnames = [[i[0].upper() for i in connection.cursor.description]]
                        if BENCHMARKER_VERBOSE_RESULTS:
                            s = columnnames + [[str(e) for e in row] for row in data]
                            lens = [max(map(len, col)) for col in zip(*s)]
                            fmt = '\t'.join('{{:{}}}'.format(x) for x in lens)
                            table = [fmt.format(*row) for row in s]
                            if not BENCHMARKER_VERBOSE_NONE:
                                print(workername+'Result set:')
                                print('\n'.join(table))
                        if not query.storeData:
                            if not BENCHMARKER_VERBOSE_NONE:
                                print(workername+"Forget result set")
                            data = []
                            columnnames = []
                        #self.logger.debug(columnnames)
        except Exception as e:
            print(workername+'Caught an error: %s' % str(e))
            error = '{workername}: {exception}'.format(workername=workername, exception=e)
            durationConnect = 0
            durationExecute = 0
            durationTransfer = 0
            data = []
            columnnames = []
            size = 0
        finally:
            #start = default_timer()
            #print("close")
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
        #print("disconnect")
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
                #if query.restrict_precision is not None:
                    #data = [[round(float(item), int(query.restrict_precision)) if tools.convertToFloat(item) == float else item for item in sublist] for sublist in data]
                    #data = [[tools.convert_to_rounded_float(item, int(query.restrict_precision)) for item in sublist] for sublist in data]
                    #data = [[tools.convert_to_rounded_float(item, int(query.restrict_precision)) for item in sublist] for sublist in data]
                if query.restrict_precision is not None:
                    precision = query.restrict_precision
                else:
                    precision = 10
                if query.sorted and len(data) > 0:
                    logger.debug(workername+"Begin sorting")
                    #data = sorted(data, key=itemgetter(*list(range(0,len(data[0])))))
                    data = [[tools.convert_to_rounded_float_2(item, int(precision)) for item in sublist] for sublist in data]
                    data = sorted(data, key=lambda sublist: tools.sort_key_rounded(sublist, precision))
                    #print(data, precision)
                    logger.debug(workername+"Finished sorting")
                logger.debug(workername+"Size of processed result list retrieved: "+str(sys.getsizeof(data))+" bytes")
            # convert to dataframe
            #columnnames = [[i[0].upper() for i in connection.cursor.description]]
            df = pd.DataFrame.from_records(data=data, coerce_float=True)
            logger.debug(workername+'Data {}'.format(data))
            logger.debug(workername+'DataFrame generated')
            if not df.empty:
                df.columns = columnnames
                size = int(df.memory_usage(index=True, deep=True).sum())
            else:
                size = 0
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
            #print(error+"\n", data)
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
    def __init__(self, result_path=None, working='query', batch=False, fixedQuery=None, fixedConnection=None, rename_connection='', rename_alias='', fixedAlias='', anonymize=False, unanonymize=[], numProcesses=None, seed=None, code=None, subfolder=None, stream_id=None, stream_shuffle=False, numStreams=1):
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
        :param numStreams: Number of parallel (independent) streams, i.e. instances of the benchmarker. Noted just for info here, has no effect
        :param seed: -
        :param code: Optional code for result folder
        :return: returns nothing
        """
        self.logger = logging.getLogger('benchmarker')
        if seed is not None:
            random.seed(int(seed))
        self.seed = seed                                    # stores random seed, used before each query parameter generator
        ## connection management:
        if numProcesses is not None:
            # we cannot handle a single connection if there are multiple processes
            singleConnection = False
            numProcesses = int(numProcesses)
        else:
            # default is 1 connection per stream
            singleConnection = True
        self.connectionmanagement = {'numProcesses': numProcesses, 'runsPerConnection': None, 'timeout': None, 'singleConnection': singleConnection, 'numStreams': numStreams}
        # set number of parallel client processes
        #self.connectionmanagement['numProcesses'] = numProcesses
        if self.connectionmanagement['numProcesses'] is None:
            self.connectionmanagement['numProcesses'] = 1#math.ceil(mp.cpu_count()/2) #If None, half of all available processes is taken
        #else:
        #    self.connectionmanagement['numProcesses'] = int(self.numProcesses)
        if not tools.query.template is None and 'numRun' in tools.query.template:
            self.connectionmanagement['numRun'] = tools.query.template['numRun'] # store global setting about number of runs per query for archive purposes
        # connection should be renamed (because of copy to subfolder and parallel processing)
        # also rename alias
        self.rename_connection = rename_connection
        self.rename_alias = rename_alias    # what alias is supposed to be renamed to
        self.fixedAlias = fixedAlias        # what alias is in connection file
        # for connections staying active for all benchmarks
        self.activeConnections = []
        #self.runsPerConnection = 4
        #self.timeout = 600
        # number of stream, in particular for parallel streams
        # None = ignore this
        self.stream_id = stream_id
        self.stream_shuffle = stream_shuffle
        # there is no general pool
        self.pool = None
        # store number of cpu cores
        self.num_cpu = mp.cpu_count()
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
        # overwrite header of workload file
        self.workload = {}
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
                    #print("path", self.pathee)
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
        #print("Results in folder {}".format(self.path))
        self.logger.debug("Results in folder {}".format(self.path))
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
        # log times
        self.time_start = 0
        self.time_end = 0
    def clearBenchmarks(self):
        """
        Clears all benchmark related protocol data.
        Allocates a timer for execution and a timer for data transfer.

        :return: returns nothing
        """
        self.protocol = {'query':{}, 'connection':{}, 'total':{}, 'ordering':{}}
        self.timerExecution = tools.timer("execution")
        self.timerTransfer = tools.timer("datatransfer")
        self.timerConnect = tools.timer("connection")
        self.timerSession = tools.timer("session")
        self.timerSession.stackable = False
        self.timerSession.perRun = False
        self.timerRun = tools.timer("run")
        self.timerRun.stackable = False
        self.timers = [self.timerSession, self.timerRun, self.timerExecution, self.timerTransfer, self.timerConnect]
    def store_ordering(self, ordering):
        #print("store_ordering", ordering)
        if self.stream_id is not None:
            self.protocol['ordering'][self.stream_id] = ordering
        else:
            self.protocol['ordering'] = ordering
    def init_random_seed(self, offset=0):
        if self.seed is not None:
            if offset is not None:
                random.seed(int(self.seed) + int(offset))
            else:
                random.seed(int(self.seed))
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
        #   self.getConnectionsFromFile(self.resultfolder_base+'/connections.config')
        #   self.getQueriesFromFile(self.resultfolder_base+'/queries.config')
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
                exit(1)
        with open(filename,'r') as inp:
            self.queryconfig = ast.literal_eval(inp.read())
            # global setting in a class variable
            # overwrites parts of query file - header
            if len(self.workload) > 0:
                for k,v in self.workload.items():
                    self.queryconfig[k] = v
            # overwrites parts of query file - queries
            if tools.query.template is not None:
                for i,q in enumerate(self.queryconfig['queries']):
                    self.queryconfig['queries'][i] = tools.joinDicts(q, tools.query.template)
                    #self.queryconfig['queries'][i] = {**q, **tools.query.template}
                    with open(self.path+'/queries.config','w') as outp:
                        pprint.pprint(self.queryconfig, outp)
            self.queries = self.queryconfig["queries"].copy()
            # default for all queries is being 'active'
            for numQuery in range(len(self.queries)):
                if not 'active' in self.queries[numQuery]:
                    self.queries[numQuery]['active'] = True
            if not "name" in self.queryconfig:
                self.queryconfig["name"] = "No name"
            if not "intro" in self.queryconfig:
                self.queryconfig["intro"] = ""
            if not "factor" in self.queryconfig:
                self.queryconfig["factor"] = "mean"
            if "connectionmanagement" in self.queryconfig:
                #self.connectionmanagement = self.queryconfig["connectionmanagement"].copy()
                self.connectionmanagement = tools.joinDicts(self.connectionmanagement, self.queryconfig["connectionmanagement"])
            #   if "timeout" in self.queryconfig["connectionmanagement"]:
            #       self.timeout = self.queryconfig["connectionmanagement"]["timeout"]
            #   if "numProcesses" in self.queryconfig["connectionmanagement"]:
            #       self.numProcesses = self.queryconfig["connectionmanagement"]["numProcesses"]
            #   if "runsPerConnection" in self.queryconfig["connectionmanagement"]:
            #       self.runsPerConnection = self.queryconfig["connectionmanagement"]["runsPerConnection"]
            if not "reporting" in self.queryconfig:
                self.queryconfig["reporting"] = {'resultsetPerQuery': False, 'resultsetPerQueryConnection': False, 'queryparameter': False, 'rowsPerResultset': False}
            if len(parameter.defaultParameters) > 0:
                if 'defaultParameters' in self.queryconfig:
                    self.queryconfig['defaultParameters'] = {**self.queryconfig['defaultParameters'], **parameter.defaultParameters}
                else:
                    self.queryconfig['defaultParameters'] = parameter.defaultParameters.copy()
            if 'defaultParameters' in self.queryconfig:
                parameter.defaultParameters = self.queryconfig['defaultParameters']
            self.queryconfig["connectionmanagement"] = self.connectionmanagement
        # store query config again, since it might have been changed
        self.store_querydata()
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
        # CHANGE: Do not force using default connection file if exists
        #if path.isfile(self.path+'/connections.config'):
        #   filename = self.path+'/connections.config'
        # If nothing is given: Try to read from result folder
        #print(filename, self.path+'/connections.config')
        if filename is None:
            filename = self.path+'/connections.config'
        # set to default if connection file cannot be found
        if not path.isfile(filename):
            filename = self.path+'/connections.config'
        # If not read from result folder: Copy to result folder
        #print(filename, self.path+'/connections.config')
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
                exit(1)
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
    def store_connectiondata(self):
        connections_content = []
        for key, dbms in self.dbms.items():
            connections_content.append(dbms.connectiondata)
        #with open(self.path+'/connections_copy.config', "w") as connections_file:
        with open(self.path+'/connections.config', "w") as connections_file:
            connections_file.write(str(connections_content))
    def store_querydata(self):
        #print("store_querydata")
        with open(self.path+'/queries.config', "w") as queries_file:
            queries_file.write(str(self.queryconfig))
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
            if not self.dbms[row.iloc[0]].connectiondata['active']:
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
                    # parametrized
                    queryPart.append(parametrize(queryTemplate, numQuery, numRun))
                else:
                    # not parametrized
                    queryPart.append(queryTemplate)
            #print(queryPart)
            queryString = queryPart
        else:
            if len(self.protocol['query'][str(numQuery)]['parameter']) > 0:
                queryString = parametrize(queryString, numQuery, numRun)
        # it is a query template
        #if len(self.protocol['query'][str(numQuery)]['parameter']) > 0:
        #   bParametrized = True
        #   #queryTemplate = queryString
        #   #params = self.protocol['query'][str(numQuery)]['parameter'][numRun]
        #   #queryString = queryTemplate.format(**params)
        #   queryString = parametrize(queryString, numQuery, numRun)
        #else:
        #   bParametrized = False
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
            if('singleConnection' in connectionmanagement):# and connectionmanagement['timeout'] != 0):
                singleConnection = connectionmanagement['singleConnection']
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
        global BENCHMARKER_VERBOSE_NONE
        # check if benchmark should be done
        if self.timerExecution.checkForSuccessfulBenchmarks(numQuery, connectionname):
            # benchmark already done
            if not self.overwrite or (self.fixedQuery is not None and self.fixedQuery != numQuery) or (self.fixedConnection is not None and self.fixedConnection != connectionname):
                # rerun not this benchmark
                self.logger.debug("Benchmarks of Q"+str(numQuery)+" at dbms "+connectionname+" already done")
                return False
            else:
                # rerun specified
                if not BENCHMARKER_VERBOSE_NONE:
                    print("Rerun benchmarks of Q"+str(numQuery)+" at dbms "+connectionname)
        else:
            # not been done
            if not (self.fixedQuery is None and self.fixedConnection is None):
                # only run specific benchmark
                if (self.fixedQuery is not None and self.fixedQuery != numQuery) or (self.fixedConnection is not None and self.fixedConnection != connectionname):
                    # not this benchmark
                    self.logger.debug("Benchmarks of Q"+str(numQuery)+" at dbms "+connectionname+" not wanted right now")
                    self.timerExecution.skipTimer(numQuery, numQuery, connectionname)
                    self.timerTransfer.skipTimer(numQuery, numQuery, connectionname)
                    self.timerConnect.skipTimer(numQuery, numQuery, connectionname)
                    return False
        # prepare basic setting
        self.logger.debug("Starting benchmarks of Q"+str(numQuery)+" at dbms "+connectionname)
        self.startBenchmarkingQuery(numQuery)
        q = self.queries[numQuery-1]
        c = connectionname
        if not BENCHMARKER_VERBOSE_NONE:
            print("Connection: "+connectionname)
        # prepare multiprocessing
        logger = mp.log_to_stderr()
        logger.setLevel(logging.WARNING)
        # prepare query object
        query = tools.query(q)
        # connection management for parallel connections
        connectionmanagement = self.getConnectionManager(numQuery, c)
        numProcesses = connectionmanagement['numProcesses']#self.numProcesses
        batchsize = connectionmanagement['runsPerConnection']#self.runsPerConnection
        timeout = connectionmanagement['timeout']#self.timeout
        if timeout is not None:
            jaydebeapi.QUERY_TIMEOUT = timeout
        if self.stream_id is not None:
            parameter.defaultParameters['STREAM'] = self.stream_id
        singleConnection = connectionmanagement['singleConnection']
        #print(connectionmanagement)
        # overwrite by connection
        #if 'connectionmanagement' in self.dbms[c].connectiondata:
        #   connectionmanagement = self.dbms[c].connectiondata['connectionmanagement']
        #   if('numProcesses' in connectionmanagement and connectionmanagement['numProcesses'] != 0):
        #       numProcesses = self.dbms[c].connectiondata['connectionmanagement']['numProcesses']
        #   if('runsPerConnection' in connectionmanagement):# and connectionmanagement['runsPerConnection'] != 0):
        #       # 0=unlimited
        #       batchsize = self.dbms[c].connectiondata['connectionmanagement']['runsPerConnection']
        #   if('timeout' in connectionmanagement):# and connectionmanagement['timeout'] != 0):
        #       # 0=unlimited
        #       timeout = self.dbms[c].connectiondata['connectionmanagement']['timeout']
        #if numProcesses == 0:
        #   numProcesses = 1
        #if timeout == 0:
        #   timeout = None
        #if batchsize == 0:
        #   batchsize = math.ceil(query.numRun/numProcesses)
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
            if singleConnection and len(self.activeConnections):
                # we have a global connection
                self.timerConnect.skipTimer(numQuery, query, connectionname)
            elif not query.withConnect:
                # we do not benchmark connection time, so we connect directly and once
                self.timerConnect.skipTimer(numQuery, query, connectionname)
                self.connectDBMS(c)
            else:
                self.timerConnect.startTimer(numQuery, query, connectionname)
            #queryString = query.query
            #if c in query.DBMS:
            #   queryString = query.DBMS[c]
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
                #   queryString = self.getQueryString(query.queryList[i % len(query.queryList)], c, self.protocol['query'][str(numQuery)]['runs'][i])
                #else:
                #   queryString = self.getQueryString(numQuery, c, i)
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
                    self.logger.info("POOL of query senders (global pool)")
                    #multiple_results = [self.pool.apply_async(singleRun, (self.dbms[c].connectiondata, inputConfig, runs[i*batchsize:(i+1)*batchsize], connectionname, numQuery, self.path, JPickler.dumps(self.activeConnections))) for i in range(numBatches)]
                    multiple_results = [self.pool.apply_async(singleRun, (self.dbms[c].connectiondata, inputConfig, runs[i*batchsize:(i+1)*batchsize], connectionname, numQuery, self.path, [], BENCHMARKER_VERBOSE_QUERIES, BENCHMARKER_VERBOSE_RESULTS, BENCHMARKER_VERBOSE_PROCESS, BENCHMARKER_VERBOSE_NONE)) for i in range(numBatches)]
                    lists = [res.get(timeout=timeout) for res in multiple_results]
                    lists = [i for j in lists for i in j]
                else:
                    """
                    with mp.Pool(processes=numProcesses) as pool:
                        self.logger.info("POOL of query senders (local pool)")
                        #multiple_results = [pool.apply_async(singleRun, (self.dbms[c].connectiondata, inputConfig, runs[i*batchsize:(i+1)*batchsize], connectionname, numQuery, self.path, JPickler.dumps(self.activeConnections))) for i in range(numBatches)]
                        multiple_results = [pool.apply_async(singleRun, (self.dbms[c].connectiondata, inputConfig, runs[i*batchsize:(i+1)*batchsize], connectionname, numQuery, self.path, [], BENCHMARKER_VERBOSE_QUERIES, BENCHMARKER_VERBOSE_RESULTS, BENCHMARKER_VERBOSE_PROCESS, BENCHMARKER_VERBOSE_NONE)) for i in range(numBatches)]
                        lists = [res.get(timeout=timeout) for res in multiple_results]
                        lists = [i for j in lists for i in j]
                        pool.close()
                    """
                    with mp.Pool(processes=numProcesses) as pool:
                        self.logger.info("POOL of query senders (local pool starmap {} workers)".format(numProcesses))
                        #multiple_results = [pool.apply_async(singleRun, (self.dbms[c].connectiondata, inputConfig, runs[i*batchsize:(i+1)*batchsize], connectionname, numQuery, self.path, JPickler.dumps(self.activeConnections))) for i in range(numBatches)]
                        args = [(self.dbms[c].connectiondata, inputConfig, runs[i*batchsize:(i+1)*batchsize], connectionname, numQuery, self.path, [], BENCHMARKER_VERBOSE_QUERIES, BENCHMARKER_VERBOSE_RESULTS, BENCHMARKER_VERBOSE_PROCESS, BENCHMARKER_VERBOSE_NONE) for i in range(numBatches)]
                        multiple_results = pool.starmap_async(singleRun, args)
                        lists = multiple_results.get(timeout=timeout)
                        #lists = [res.get(timeout=timeout) for res in multiple_results]
                        lists = [i for j in lists for i in j]
                        pool.close()
                        pool.join()
            else:
                # no parallel processes because JVM does not parallize
                # time the queries and stop early if maxTime is reached
                if BENCHMARKER_VERBOSE_PROCESS:
                    self.logger.info("We have {} active connections".format(len(self.activeConnections)))
                start_time_queries = default_timer()
                lists = []
                for i in range(numBatches):
                    lists_batch = singleRun(self.dbms[c].connectiondata, inputConfig, runs[i*batchsize:(i+1)*batchsize], connectionname, numQuery, self.path, self.activeConnections, BENCHMARKER_VERBOSE_QUERIES, BENCHMARKER_VERBOSE_RESULTS, BENCHMARKER_VERBOSE_PROCESS, BENCHMARKER_VERBOSE_NONE)
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
                #   print("Closed connection")
                #   con.disconnect()
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
            numProcesses_cpu = self.num_cpu# mp.cpu_count()
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
            # TODO: why always store complete resultset for subfolders, even if there is none?
            if not self.resultfolder_subfolder is None and len(self.resultfolder_subfolder) > 0:
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
            if query.withConnect and not (singleConnection and len(self.activeConnections)):
                # we do benchmark connection time, so we connect every run
                #self.disconnectDBMS(c)
                self.timerConnect.finishTimer()
        if not keepResultsets:
            self.protocol['query'][str(numQuery)]['resultSets'][c] = []
        self.stopBenchmarkingQuery(numQuery)
        #if self.dbms[c].hasHardwareMetrics():
        #   metricsReporter = monitor.metrics(self)
        #   metricsReporter.generatePlotForQuery(numQuery)
        return True
    def generateAllParameters(self, overwrite=False):
        for numQuery in range(1, len(self.queries)+1):
            q = self.queries[numQuery-1]
            query = tools.query(q)
            self.logger.debug("generateAllParameters query={}, parameter={}, protocol={}".format(numQuery, query.parameter, self.protocol['query'][str(numQuery)]['parameter']))
            if len(query.parameter) > 0 and (overwrite or len(self.protocol['query'][str(numQuery)]['parameter']) == 0):
                self.init_random_seed(numQuery)
                params = parameter.generateParameters(query.parameter, query.numRun)
                self.protocol['query'][str(numQuery)]['parameter'] = params
    def startBenchmarkingQuery(self, numQuery):
        """
        Starts protocol for that specific query.
        Generates parameters.

        :param numQuery: Number of query to benchmark
        :return: returns nothing
        """
        global BENCHMARKER_VERBOSE_NONE
        if self.protocol['query'][str(numQuery)]['start'] == "":
            self.protocol['query'][str(numQuery)]['start'] = str(datetime.datetime.now())
        self.start_query = timer()
        q = self.queries[numQuery-1]
        query = tools.query(q)
        if len(query.parameter) > 0 and len(self.protocol['query'][str(numQuery)]['parameter']) == 0:
            self.logger.debug("generateParameters query={}, parameter={}, protocol={}".format(numQuery, query.parameter, self.protocol['query'][str(numQuery)]))
            self.init_random_seed(numQuery)
            params = parameter.generateParameters(query.parameter, query.numRun)
            self.protocol['query'][str(numQuery)]['parameter'] = params
        if len(query.queryList) > 0:
            numRunList = []
            for queryElement in range(query.numRun): # query.queryList:
                numRun = self.getRandomRun(query.queryList[queryElement % len(query.queryList)])#, connectionname)
                numRunList.append(numRun)
            self.protocol['query'][str(numQuery)]['runs'] = numRunList
        if not BENCHMARKER_VERBOSE_NONE:
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
        global BENCHMARKER_VERBOSE_NONE
        # generate ordering
        ordered_list_of_queries = list(range(1, len(self.queries)+1))
        if self.stream_shuffle is not None and int(self.stream_shuffle) > 0:
            if not BENCHMARKER_VERBOSE_NONE:
                print("User wants shuffling")
            if 'stream_ordering' in self.queryconfig and len(self.queryconfig['stream_ordering']) > 0 and self.stream_id is not None and int(self.stream_id) > 0:
                if not BENCHMARKER_VERBOSE_NONE:
                    print("Query file provides shuffling")
                    print("We are on stream {}".format(int(self.stream_id)))
                num_total_streams = len(self.queryconfig['stream_ordering'])
                # stream ids start at 1 and are limited by the number of streams in the ordering list
                num_current_stream = (int(self.stream_id)-1)%num_total_streams+1
                ordered_list_of_queries = self.queryconfig['stream_ordering'][num_current_stream]
                if not BENCHMARKER_VERBOSE_NONE:
                    print("Ordering:", ordered_list_of_queries)
            else:
                if not BENCHMARKER_VERBOSE_NONE:
                    print("We shuffle randomly")
                #ordered_list_of_queries = list(ordered_list_of_queries)
                self.init_random_seed(self.stream_id)
                random.shuffle(ordered_list_of_queries)
                if not BENCHMARKER_VERBOSE_NONE:
                    print("Ordering:", ordered_list_of_queries)
        self.store_ordering(ordered_list_of_queries)
        # a dict of connections, each carrying a list of connections to the dbms
        connectionpool = dict()
        for numQuery in ordered_list_of_queries:
            if self.overwrite and not (self.fixedQuery is not None and self.fixedQuery != numQuery):# or (self.fixedConnection is not None and self.fixedConnection != connectionname):
                # rerun this query
                self.cleanProtocol(numQuery)
            for connectionname in sorted(self.dbms.keys()):
                # check if we need a global connection
                singleConnection = self.connectionmanagement['singleConnection'] # Default is (probably) True
                if 'connectionmanagement' in self.queryconfig:
                    connectionmanagement = self.queryconfig['connectionmanagement']
                    if('singleConnection' in connectionmanagement):# and connectionmanagement['timeout'] != 0):
                        singleConnection = connectionmanagement['singleConnection']
                if singleConnection:
                    if not connectionname in connectionpool:
                        connectionpool[connectionname] = []
                    # we assume all queries should share a connection
                    numProcesses = 1
                    i = len(connectionpool[connectionname])
                    #i = 0
                    #connectionname = c
                    if i == 0:
                        if (self.fixedQuery is not None and self.fixedQuery != numQuery) or (self.fixedConnection is not None and self.fixedConnection != connectionname):
                            continue
                        else:
                            if not BENCHMARKER_VERBOSE_NONE:
                                #print("More active connections from {} to {} for {}".format(len(self.activeConnections), numProcesses, connectionname))
                                print("More active connections from {} to {} for {}".format(len(connectionpool[connectionname]), numProcesses, connectionname))
                            #self.activeConnections.append(tools.dbms(self.dbms[connectionname].connectiondata))
                            connectionpool[connectionname].append(tools.dbms(self.dbms[connectionname].connectiondata))
                            if not BENCHMARKER_VERBOSE_NONE:
                                print("Establish global connection #"+str(i))
                            #self.activeConnections[i].connect()
                            connectionpool[connectionname][i].connect()
                    self.activeConnections = connectionpool[connectionname]
                # run benchmark, current query and connection
                bBenchmarkDoneForThisQuery = self.runBenchmark(numQuery, connectionname)
                # close global connection
                if singleConnection:
                    #print("Closed connection for", connectionname)
                    #self.activeConnections[i].disconnect()
                    #self.activeConnections = []
                    #connectionpool[connectionname][i].disconnect()
                    self.activeConnections = []
                # if benchmark has been done: store and generate reports
                if bBenchmarkDoneForThisQuery:
                    # store results
                    self.reporterStore.generate(numQuery, [self.timerExecution, self.timerTransfer, self.timerConnect])
                    if not self.bBatch:
                        # generate reports
                        for r in self.reporter:
                            r.init()
                            r.generate(numQuery, [self.timerExecution, self.timerTransfer, self.timerConnect])
        if len(connectionpool):
            # close all connections in pool
            for connectionname in connectionpool.keys():
                for i in range(len(connectionpool[connectionname])):
                    connectionpool[connectionname][i].disconnect()
    def runBenchmarksConnection(self):
        """
        Performs connectionwise benchmark runs.
        Stores results and generates reports immediately after completion of a query (all runs).

        :return: returns nothing
        """
        global BENCHMARKER_VERBOSE_NONE
        # generate ordering
        ordered_list_of_queries = list(range(1, len(self.queries)+1))
        if self.stream_shuffle is not None and int(self.stream_shuffle) > 0:
            if not BENCHMARKER_VERBOSE_NONE:
                print("User wants shuffling")
            if 'stream_ordering' in self.queryconfig and len(self.queryconfig['stream_ordering']) > 0 and self.stream_id is not None and int(self.stream_id) > 0:
                if not BENCHMARKER_VERBOSE_NONE:
                    print("Query file provides shuffling")
                    print("We are on stream {}".format(int(self.stream_id)))
                num_total_streams = len(self.queryconfig['stream_ordering'])
                # stream ids start at 1 and are limited by the number of streams in the ordering list
                num_current_stream = (int(self.stream_id)-1)%num_total_streams+1
                ordered_list_of_queries = self.queryconfig['stream_ordering'][num_current_stream]
                if not BENCHMARKER_VERBOSE_NONE:
                    print("Ordering:", ordered_list_of_queries)
            else:
                if not BENCHMARKER_VERBOSE_NONE:
                    print("We shuffle randomly")
                #ordered_list_of_queries = list(ordered_list_of_queries)
                self.init_random_seed(self.stream_id)
                random.shuffle(ordered_list_of_queries)
                if not BENCHMARKER_VERBOSE_NONE:
                    print("Ordering:", ordered_list_of_queries)
        self.store_ordering(ordered_list_of_queries)
        # work per connection
        for connectionname in sorted(self.dbms.keys()):
            # check if we need a global connection
            singleConnection = self.connectionmanagement['singleConnection'] # Default is (probably) True
            if 'connectionmanagement' in self.queryconfig:
                connectionmanagement = self.queryconfig['connectionmanagement']
                if('singleConnection' in connectionmanagement):# and connectionmanagement['timeout'] != 0):
                    singleConnection = connectionmanagement['singleConnection']
            if singleConnection:
                # we assume all queries should share a connection
                numProcesses = 1
                i = 0
                #connectionname = c
                if (self.fixedConnection is not None and self.fixedConnection != connectionname):
                    continue
                else:
                    if not BENCHMARKER_VERBOSE_NONE:
                        print("More active connections from {} to {} for {}".format(len(self.activeConnections), numProcesses, connectionname))
                    self.activeConnections.append(tools.dbms(self.dbms[connectionname].connectiondata))
                    if not BENCHMARKER_VERBOSE_NONE:
                        print("Establish global connection #"+str(i))
                    self.activeConnections[i].connect()
            #print(self.activeConnections)
            # work queries
            for numQuery in ordered_list_of_queries:
                bBenchmarkDone = self.runBenchmark(numQuery, connectionname)
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
                if not BENCHMARKER_VERBOSE_NONE:
                    print("Closed connection for", connectionname)
                self.activeConnections[i].disconnect()
                self.activeConnections = []
    def runBenchmarks(self):
        """
        Runs benchmarks or possibly reruns specific benchmarks.
        Generates reports.

        :return: returns nothing
        """
        # log time of starting benchmark
        time_now = str(datetime.datetime.now())
        time_now_int = int(datetime.datetime.timestamp(datetime.datetime.strptime(time_now,'%Y-%m-%d %H:%M:%S.%f')))
        self.time_start = time_now_int
        self.logger.debug("### Time start: "+str(self.time_start))
        if not 'total' in self.protocol:
            self.protocol['total'] = {}
        for connectionname in sorted(self.dbms.keys()):
            if not connectionname in self.protocol['total']:
                self.protocol['total'][connectionname] = {}
            self.protocol['total'][connectionname]['time_start'] = self.time_start
        # clean evaluation dict
        evaluator.evaluator.evaluation = {}
        if self.working == 'query':
            self.runBenchmarksQuery()
        else:
            self.runBenchmarksConnection()
        # log time of ending benchmark
        time_now = str(datetime.datetime.now())
        time_now_int = int(datetime.datetime.timestamp(datetime.datetime.strptime(time_now,'%Y-%m-%d %H:%M:%S.%f')))
        self.time_end = time_now_int
        self.logger.debug("### Time end: "+str(self.time_end))
        for connectionname in sorted(self.dbms.keys()):
            self.protocol['total'][connectionname]['time_end'] = self.time_end
        if self.fixedConnection is None:
            if self.stream_id is None:
                print("DBMSBenchmarker duration: {} [s]".format(self.time_end-self.time_start))
            else:
                print("DBMSBenchmarker duration stream {}: {} [s]".format(self.stream_id, self.time_end-self.time_start))
        else:
            if self.stream_id is None:
                print("DBMSBenchmarker duration of {}: {} [s]".format(self.fixedConnection, self.time_end-self.time_start))
            else:
                print("DBMSBenchmarker duration stream {} of {}: {} [s]".format(self.stream_id, self.fixedConnection, self.time_end-self.time_start))
        # write protocol again
        self.reporterStore.writeProtocol()
        # store connection data again, it may have changed
        self.store_connectiondata()
        if self.bBatch:
            # generate reports at the end only
            self.generateReportsAll()
        # stop logging multiprocessing
        mp.log_to_stderr(logging.ERROR)
    def readResultfolder(self, silent=False):
        """
        Reads data of previous benchmark from folder.

        :param silent: No output of status
        :return: returns nothing
        """
        if not silent:
            print("Read results")
        self.clearBenchmarks()
        # read from stored results
        self.logger.debug("Read from "+self.path)
        self.reporterStore.readProtocol(silent)
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
        # stop logging multiprocessing
        mp.log_to_stderr(logging.ERROR)
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
            #   continue
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
        global BENCHMARKER_VERBOSE_NONE
        self.computeTimerRun()
        self.computeTimerSession()
        # forget and recompute hardware metrics
        monitor.metrics.m_avg = None
        for r in self.reporter:
            if not BENCHMARKER_VERBOSE_NONE:
                print("Report "+type(r).__name__)
            r.generateAll(self.timers)#[self.timerExecution, self.timerTransfer, self.timerConnect])
    def continueBenchmarks(self, overwrite=False, recreate_parameter=None):
        """
        Reads data of previous benchmark from folder.
        Continues performing missing benchmarks, if not all queries were treated completely.

        :param overwrite: True if existing benchmarks should be overwritten
        :return: returns nothing
        """
        print("Continue Benchmarks")
        self.overwrite = overwrite
        self.readResultfolder()
        # (Re)create all parameters?
        if recreate_parameter is not None and int(recreate_parameter) == 1:
            print("(Re)create all parameters")
            self.generateAllParameters(overwrite)
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
            BENCHMARKER_VERBOSE_PROCESS=BENCHMARKER_VERBOSE_PROCESS,
            BENCHMARKER_VERBOSE_NONE=BENCHMARKER_VERBOSE_NONE)
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
    def __init__(self, result_path, code, anonymize=False, silent=False):
        path = (result_path+"/"+str(code)).replace("//", "/")
        benchmarker.__init__(self,result_path=path, anonymize=anonymize)
        self.getConfig()
        self.readResultfolder(silent=silent)
        if not silent:
            print("Connections:")
            for c in self.connections:
                print(c['name'])
            print("Queries:")
            for i,q in enumerate(self.queries):
                if 'active' in q and q['active']:
                    print(str(i)+": Q"+str(i+1)+" = "+q['title'])





def run_cli(parameter):
    # argparse
    """
    parser = argparse.ArgumentParser(description='A benchmark tool for RDBMS. It connects to a given list of RDBMS via JDBC and runs a given list benchmark queries. Optionally some reports are generated.')
    parser.add_argument('mode', help='run benchmarks and save results, or just read benchmark results from folder, or continue with missing benchmarks only', choices=['run', 'read', 'continue'])
    parser.add_argument('-d', '--debug', help='dump debug informations', action='store_true')
    parser.add_argument('-b', '--batch', help='batch mode (more protocol-like output), automatically on for debug mode', action='store_true')
    parser.add_argument('-qf', '--query-file', help='name of query config file', default='queries.config')
    parser.add_argument('-cf', '--connection-file', help='name of connection config file', default='connections.config')
    parser.add_argument('-q', '--query', help='number of query to benchmark', default=None)
    parser.add_argument('-c', '--connection', help='name of connection to benchmark', default=None)
    parser.add_argument('-ca', '--connection-alias', help='alias of connection to benchmark', default='')
    parser.add_argument('-f', '--config-folder', help='folder containing query and connection config files. If set, the names connections.config and queries.config are assumed automatically.', default=None)
    parser.add_argument('-r', '--result-folder', help='folder for storing benchmark result files, default is given by timestamp', default=None)
    parser.add_argument('-e', '--generate-evaluation', help='generate new evaluation file', default='no', choices=['no','yes'])
    parser.add_argument('-w', '--working', help='working per query or connection', default='connection', choices=['query','connection'])
    #parser.add_argument('-a', '--anonymize', help='anonymize all dbms', action='store_true', default=False)
    #parser.add_argument('-u', '--unanonymize', help='unanonymize some dbms, only sensible in combination with anonymize', nargs='*', default=[])
    parser.add_argument('-p', '--numProcesses', help='Number of parallel client processes. Global setting, can be overwritten by connection. Default is 1.', default=None)
    parser.add_argument('-s', '--seed', help='random seed', default=None)
    parser.add_argument('-cs', '--copy-subfolder', help='copy subfolder of result folder', action='store_true')
    parser.add_argument('-ms', '--max-subfolders', help='maximum number of subfolders of result folder', default=None)
    parser.add_argument('-sl', '--sleep', help='sleep SLEEP seconds before going to work', default=0)
    parser.add_argument('-st', '--start-time', help='sleep until START-TIME before beginning benchmarking', default=None)
    parser.add_argument('-sf', '--subfolder', help='stores results in a SUBFOLDER of the result folder', default=None)
    parser.add_argument('-sd', '--store-data', help='store result of first execution of each query', default=None, choices=[None, 'csv', 'pandas'])
    parser.add_argument('-vq', '--verbose-queries', help='print every query that is sent', action='store_true', default=False)
    parser.add_argument('-vs', '--verbose-statistics', help='print statistics about queries that have been sent', action='store_true', default=False)
    parser.add_argument('-vr', '--verbose-results', help='print result sets of every query that has been sent', action='store_true', default=False)
    parser.add_argument('-vp', '--verbose-process', help='print result sets of every query that has been sent', action='store_true', default=False)
    parser.add_argument('-pn', '--num-run', help='Parameter: Number of executions per query', default=0)
    parser.add_argument('-m', '--metrics', help='collect hardware metrics per query', action='store_true', default=False)
    parser.add_argument('-mps', '--metrics-per-stream', help='collect hardware metrics per stream', action='store_true', default=False)
    parser.add_argument('-sid', '--stream-id', help='id of a stream in parallel execution of streams', default=None)
    parser.add_argument('-ssh', '--stream-shuffle', help='shuffle query execution based on id of stream', default=None)
    parser.add_argument('-wli', '--workload-intro', help='meta data: intro text for workload description', default='')
    parser.add_argument('-wln', '--workload-name', help='meta data: name of workload', default='')
    #parser.add_argument('-pt', '--timeout', help='Parameter: Timeout in seconds', default=0)
    args = parser.parse_args()
    """
    global BENCHMARKER_VERBOSE_QUERIES, BENCHMARKER_VERBOSE_STATISTICS, BENCHMARKER_VERBOSE_RESULTS, BENCHMARKER_VERBOSE_PROCESS, BENCHMARKER_VERBOSE_NONE
    #print(parameter)
    args = SimpleNamespace(**parameter)
    #print(args)
    #exit()
    logger = logging.getLogger('dbmsbenchmarker')
    # evaluate args
    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
        bBatch = True
    else:
        logging.basicConfig(level=logging.INFO)
        bBatch = args.batch
    # set verbose level
    if args.verbose_queries:
        BENCHMARKER_VERBOSE_QUERIES = True
    if args.verbose_statistics:
        BENCHMARKER_VERBOSE_STATISTICS = True
    if args.verbose_results:
        BENCHMARKER_VERBOSE_RESULTS = True
    if args.verbose_process:
        BENCHMARKER_VERBOSE_PROCESS = True
    if args.verbose_none:
        BENCHMARKER_VERBOSE_NONE = True
        logger = logging.getLogger('dbmsbenchmarker')
        logger.setLevel(logging.ERROR)
        logging.basicConfig(level=logging.ERROR)
    subfolder = None
    rename_connection = ''
    rename_alias = ''
    if args.mode != 'read' and args.parallel_processes and args.numProcesses is not None:
        numProcesses = int(args.numProcesses)
        if not args.verbose_none:
            print("Start {} independent processes".format(numProcesses))
        code = str(round(time.time()))
        # make a copy of result folder
        #if not args.result_folder is None and not path.isdir(args.result_folder):
        if args.result_folder is None:
            result_folder = './'
        else:
            result_folder = args.result_folder
        command_args = vars(args).copy()
        makedirs(result_folder+"/"+code)
        copyfile(args.config_folder+'/connections.config', result_folder+"/"+code+'/connections.config')#args.connection_file)
        copyfile(args.config_folder+'/queries.config', result_folder+"/"+code+'/queries.config')#args.query_file)
        if not args.connection is None:
            connections = [args.connection]
        else:
            #connection = args.connection
            #print("Parallel execution must be limited to single DBMS")
            with open(result_folder+"/"+code+'/connections.config', "r") as connections_file:
                connections_content = ast.literal_eval(connections_file.read())
            #print(connections_content)
            connections = [c['name'] for c in connections_content]
            if not args.verbose_none:
                print(connections)
            #exit()
        for connection in connections:
            # only neccessary after merge
            #copyfile(args.config_folder+'/connections.config', result_folder+'/connections.config')#args.connection_file)
            #copyfile(args.config_folder+'/queries.config', result_folder+'/queries.config')#args.query_file)
            #del command_args['parallel_processes']
            command_args['parallel_processes'] = False
            command_args['numProcesses'] = None
            command_args['numStreams'] = numProcesses
            command_args['result_folder'] = result_folder+"/"+code
            command_args['copy_subfolder'] = True
            command_args['subfolder'] = connection
            command_args['connection'] = connection
            #if 'generate_evaluation' in command_args:
            #    del command_args['generate_evaluation']
            command_args['generate_evaluation'] = 'no'
            # Get the current UTC time
            current_time = datetime.datetime.utcnow()
            # Add 5 seconds to the current time
            seconds = 5 if 5 > numProcesses else numProcesses
            start_time = current_time + datetime.timedelta(seconds=seconds)
            command_args['start_time'] = start_time.strftime('%Y-%m-%d %H:%M:%S')
            #command_args['stream_id'] = 1
            pool_args = []#(dict(command_args),)]*numProcesses
            for i in range(numProcesses):
                command_args['stream_id'] = i+1
                pool_args.append((dict(command_args),))
                #pool_args[i][0]['stream_id'] = i+1
            #print(pool_args[0][0]['stream_id'])
            #exit()
            #print(command_args)
            #print(pool_args)
            #exit()
            # Create a pool of subprocesses
            #with Pool(processes=4) as pool:  # Adjust the number of processes as needed
            with mp.Pool(processes=int(numProcesses)) as pool:
                # Map the arguments to the subprocess function
                #results = pool.map("scripts.cli", [command_args]*4)  # Run the same args in 4 subprocesses
                multiple_results = pool.starmap_async(run_cli, pool_args)
                #multiple_results = pool.starmap_async(benchmarker.run_cli, [(k:v) for k,d in command_args.items()])
                lists = multiple_results.get()#timeout=timeout)
                #lists = [res.get(timeout=timeout) for res in multiple_results]
                #lists = [i for j in lists for i in j]
                #print(lists)
                pool.close()
                pool.join()
            # Print results
            #for stdout, stderr in multiple_results:
            #    print("STDOUT:", stdout)
            #    print("STDERR:", stderr)
        tools.merge_partial_results(result_folder+"/", code)
        if args.generate_evaluation == 'yes':
            #evaluator.evaluation = {}
            #command_args['mode'] = 'read'
            #command_args['result_folder'] = code
            #experiments = benchmarker.run_cli(command_args)
            experiments = benchmarker(
                result_path=result_folder,#args.result_folder,
                code=code,
                #working=args.working,
                batch=bBatch,
                #subfolder=subfolder,#args.subfolder,
                fixedQuery=args.query,
                fixedConnection=args.connection,
                fixedAlias=args.connection_alias,
                #rename_connection=rename_connection,
                #rename_alias=rename_alias,
                #anonymize=args.anonymize,
                #unanonymize=args.unanonymize,
                #numProcesses=args.numProcesses,
                #stream_id=stream_id,
                #stream_shuffle=stream_shuffle,
                #seed=args.seed
            )
            experiments.getConfig()
            experiments.readBenchmarks()
            evaluate = run_evaluation(experiments, show_query_statistics=True)
            #print("Experiment {} has been finished".format(experiments.code))
            #print(evaluate)
            #list_connections = evaluate.get_experiment_list_connections()
            #print(list_connections)
            """
            benchmarker_times = evaluate.get_experiment_connection_properties(list_connections[0])['times']['total']
            # compute min of start and max of end for timespan
            times_start=[]
            times_end=[]
            for t in benchmarker_times:
                times_start.append(benchmarker_times[t]['time_start'])
                times_end.append(benchmarker_times[t]['time_end'])
            time_start = min(times_start)
            time_end = max(times_end)
            print(time_start, time_end, time_end-time_start)
            """
            return experiments
        return None
    else:
        if args.mode != 'read':
            # sleep before going to work
            if int(args.sleep) > 0:
                logger.debug("Sleeping {} seconds before going to work".format(int(args.sleep)))
                time.sleep(int(args.sleep))
            # make a copy of result folder
            if not args.result_folder is None and not path.isdir(args.result_folder):
                makedirs(args.result_folder)
                copyfile(args.config_folder+'/connections.config', args.result_folder+'/connections.config')#args.connection_file)
                copyfile(args.config_folder+'/queries.config', args.result_folder+'/queries.config')#args.query_file)
            subfolder = args.subfolder
            if args.copy_subfolder and len(subfolder) > 0:
                if args.result_folder is None:
                    print("Subfolder logic only makes sense for existing result folders or for parallel processes. Sorry!")
                    print("Hint: 1) add -pp, or 2) add -r, or 3) remove -cs and -sf")
                    exit()
                if args.stream_id is not None:
                    client = int(args.stream_id)
                else:
                    client = 1
                while True:
                    if args.max_subfolders is not None and client > int(args.max_subfolders):
                        exit()
                    resultpath = args.result_folder+'/'+subfolder+'-'+str(client)
                    if not args.verbose_none:
                        print("Checking if {} is suitable folder for free job number".format(resultpath))
                    if path.isdir(resultpath):
                        client = client + 1
                        waiting = random.randint(1, 10)
                        if not args.verbose_none:
                            print("Sleeping {} seconds before checking for next free job number".format(waiting))
                        time.sleep(waiting)
                    else:
                        if not args.verbose_none:
                            print("{} is a suitable folder for free job number".format(resultpath))
                        makedirs(resultpath)
                        break
                subfolder = subfolder+'-'+str(client)
                rename_connection = args.connection+'-'+str(client)
                if not args.verbose_none:
                    print("Rename connection {} to {}".format(args.connection, rename_connection))
                rename_alias = args.connection_alias+'-'+str(client)
                if not args.verbose_none:
                    print("Rename alias {} to {}".format(args.connection_alias, rename_alias))
            # sleep before going to work
            if args.start_time is not None:
                #logger.debug(args.start_time)
                now = datetime.datetime.utcnow()
                try:
                    start = datetime.datetime.strptime(args.start_time, '%Y-%m-%d %H:%M:%S')
                    if start > now:
                        wait = (start-now).seconds
                        now_string = now.strftime('%Y-%m-%d %H:%M:%S')
                        if not args.verbose_none:
                            print("Sleeping until {} before going to work ({} seconds, it is {} now)".format(args.start_time, wait, now_string))
                        time.sleep(int(wait))
                except Exception as e:
                    print("Invalid format: {}".format(args.start_time))
        # handle parallel streams
        stream_id = args.stream_id
        stream_shuffle = args.stream_shuffle
        #if stream_shuffle is not None and stream_shuffle:
        #    print("User wants shuffled queries")
        #if stream_id is not None and stream_id:
        #    print("This is stream {}".format(stream_id))
        # overwrite parameters of workload queries
        if int(args.num_run) > 0:
            #querymanagement = {
            #     'numRun': int(args.num_run),
            #     'timer': {'datatransfer': {'store': 'csv'}},
            #}
            #tools.query.template = querymanagement
            if not isinstance(tools.query.template, dict):
                tools.query.template = {}
            tools.query.template['numRun'] = int(args.num_run)
        if args.store_data is not None:
            if not isinstance(tools.query.template, dict):
                tools.query.template = {}
            tools.query.template['timer'] = {'datatransfer': {'store': args.store_data}}
        if args.discard_data:
            if not isinstance(tools.query.template, dict):
                tools.query.template = {}
            tools.query.template['timer'] = {'datatransfer': {'active': 'False'}}
        if hasattr(args, 'numStreams'):
            numStreams = args.numStreams
        else:
            numStreams = 1
        # dbmsbenchmarker with reporter
        experiments = benchmarker(
            result_path=args.result_folder,
            working=args.working,
            batch=bBatch,
            subfolder=subfolder,#args.subfolder,
            fixedQuery=args.query,
            fixedConnection=args.connection,
            fixedAlias=args.connection_alias,
            rename_connection=rename_connection,
            rename_alias=rename_alias,
            #anonymize=args.anonymize,
            #unanonymize=args.unanonymize,
            numProcesses=args.numProcesses,
            numStreams=numStreams,
            stream_id=stream_id,
            stream_shuffle=stream_shuffle,
            seed=args.seed)
        # overwrite parameters of workload header
        if len(args.workload_intro):
            experiments.workload['intro'] = args.workload_intro
        if len(args.workload_name):
            experiments.workload['name'] = args.workload_name
        # why?
        #if args.result_folder is not None:
        #    config_folder = args.result_folder
        #else:
        #    config_folder = args.config_folder
        experiments.getConfig(args.config_folder, args.connection_file, args.query_file)
        # switch for args.mode
        if args.mode == 'read':
            experiments.readBenchmarks()
        elif args.mode == 'run':
            if experiments.continuing:
                #experiments.generateAllParameters()
                experiments.continueBenchmarks(overwrite = True)
            else:
                #experiments.generateAllParameters()
                experiments.runBenchmarks()
            if args.connection is None:
                if stream_id is None:
                    print('Experiment {} has been finished'.format(experiments.code))
                else:
                    print('Experiment {} stream {} has been finished'.format(experiments.code, stream_id))
            else:
                if stream_id is None:
                    print('Experiment {} for {} has been finished'.format(experiments.code, args.connection))
                else:
                    print('Experiment {} stream {} for {} has been finished'.format(experiments.code, stream_id, args.connection))
        elif args.mode == 'continue':
            if experiments.continuing:
                experiments.continueBenchmarks(overwrite = False)
            else:
                print("Continue needs result folder")
        if args.metrics:
            # collect hardware metrics
            experiments.reporter.append(reporter.metricer(experiments))
            experiments.generateReportsAll()
        if args.metrics_per_stream:
            # collect hardware metrics
            experiments.reporter.append(reporter.metricer(experiments, per_stream=True))
            experiments.generateReportsAll()
        if args.generate_evaluation == 'yes':
            # generate evaluation cube
            experiments.overwrite = True
            evaluator.evaluator(experiments, load=False, force=True)
            run_evaluation(experiments, True)
        return experiments

def run_evaluation(experiments, show_query_statistics=False):
        # generate evaluation cube
        experiments.overwrite = True
        # show some evaluations
        evaluator.evaluator(experiments, load=False, force=True)
        result_folder = experiments.path #args.result_folder if not args.result_folder is None else "./"
        #num_processes = min(float(args.numProcesses if not args.numProcesses is None else 1), float(args.num_run) if int(args.num_run) > 0 else 1)
        evaluate = dbmsbenchmarker.inspector.inspector(result_folder)
        evaluate.load_experiment("")#experiments.code)
        print("Show Evaluation")
        print("===============")
        # show aggregated statistics per query
        #evaluate = inspector.inspector(resultfolder)
        #evaluate.load_experiment(code)
        #evaluate = inspector.inspector(resultfolder)
        #evaluate.load_experiment(code)
        list_connections = evaluate.get_experiment_list_connections()
        list_queries = evaluate.get_experiment_list_queries()
        query_properties = evaluate.get_experiment_query_properties()
        def map_index_to_queryname_simple(numQuery):
            if numQuery in query_properties and 'config' in query_properties[numQuery] and 'title' in query_properties[numQuery]['config']:
                return query_properties[numQuery]['config']['title']
            else:
                return numQuery
        if show_query_statistics:
            for numQuery in list_queries:
                #df1, df2 = evaluate.get_measures_and_statistics(numQuery)
                #print(df2.round(2))
                df1, df2 = evaluate.get_measures_and_statistics_merged(numQuery)
                header = df2.columns
                if not df2.empty:
                    print("Q{}: {}".format(numQuery, map_index_to_queryname_simple(str(numQuery))))
                    print(tabulate(df2,headers=header, tablefmt="grid", floatfmt=".2f"))
                #print(df2.round(2))
                #break        # get workload properties
        workload_properties = evaluate.get_experiment_workload_properties()
        #query_properties = evaluate.get_experiment_query_properties()
        def map_index_to_queryname(numQuery):
            if numQuery[1:] in query_properties and 'config' in query_properties[numQuery[1:]] and 'title' in query_properties[numQuery[1:]]['config']:
                return query_properties[numQuery[1:]]['config']['title']
            else:
                return numQuery
        #query_properties['1']['config']
        print(workload_properties['name'], ":", workload_properties['intro'])
        # get queries and dbms
        list_queries_all = evaluate.get_experiment_list_queries()
        #print(list_queries_all)
        dbms_filter = []
        #if not args.connection is None:
        #    dbms_filter = [args.connection]
        for q in list_queries_all:
            df = evaluate.get_timer(q, "execution")
            if len(list(df.index)) > 0:
                dbms_filter = list(df.index)
                print("First successful query: Q{}".format(q))
                break
        #print(dbms_filter)
        #list_queries = evaluate.get_experiment_queries_successful() # evaluate.get_experiment_list_queries()
        list_queries = evaluate.get_survey_successful(timername='run', dbms_filter=dbms_filter)
        #print(list_queries, len(list_queries))
        if 'numRun' in experiments.connectionmanagement:
            num_run = experiments.connectionmanagement['numRun']
        else:
            num_run = 1
        if 'numProcesses' in experiments.connectionmanagement:
            num_processes = experiments.connectionmanagement['numProcesses']
        else:
            num_processes = 1
        if 'numStreams' in experiments.connectionmanagement:
            num_streams = experiments.connectionmanagement['numStreams']
        else:
            num_streams = 1
        #####################
        if len(dbms_filter) > 0:
            print("Limited to:", dbms_filter)
        print("Number of successful queries:", len(list_queries))
        print("Number of runs per query:", num_run)
        print("Number of max. parallel clients per stream:", int(num_processes))
        print("Number of parallel independent streams:", int(num_streams))
        if 'ordering' in evaluate.benchmarks.protocol:
            print("Ordering of queries:")
            #print(evaluate.benchmarks.protocol['ordering'])
            if isinstance(evaluate.benchmarks.protocol['ordering'], list):
                print(evaluate.benchmarks.protocol['ordering'])
            else:
                for k,v in evaluate.benchmarks.protocol['ordering'].items():
                    print("Stream {}: {}".format(k, v))
        #####################
        df = evaluate.get_total_errors(dbms_filter=dbms_filter).T
        pd.set_option("display.max_rows", None)
        pd.set_option('display.max_colwidth', None)
        pd.set_option('display.max_rows', 500)
        pd.set_option('display.max_columns', 500)
        pd.set_option('display.width', 1000)
        #print(df)
        num_errors = df.sum().sum()
        print("\n### Errors (failed queries): {}".format(num_errors))
        if num_errors > 0:
            df.index = df.index.map(map_index_to_queryname)
            all_false_rows = df.apply(lambda row: all(row == False), axis=1)
            df_filtered = df[~all_false_rows]
            print(df_filtered)
        else:
            print("No errors")
        #####################
        df = evaluate.get_total_warnings(dbms_filter=dbms_filter).T
        num_warnings = df.sum().sum()
        print("\n### Warnings (result mismatch): {}".format(num_warnings))
        if num_warnings > 0:
            df.index = df.index.map(map_index_to_queryname)
            all_false_rows = df.apply(lambda row: all(row == False), axis=1)
            df_filtered = df[~all_false_rows]
            print(df_filtered)
        else:
            print("No warnings")
        #####################
        #df = evaluate.get_aggregated_query_statistics(type='timer', name='connection', query_aggregate='Median', dbms_filter=dbms_filter)
        df = evaluate.get_aggregated_experiment_statistics(type='timer', name='connection', query_aggregate='Median', total_aggregate='Geo', dbms_filter=dbms_filter)
        df = (df/1000.0).sort_index()
        if not df.empty:
            print("\n### Geometric Mean of Medians of Connection Times (only successful) [s]")
            df.columns = ['average connection time [s]']
            df = df.reindex(index=tools.natural_sort(df.index))
            print(df.round(2))
            #print("### Statistics of Timer Connection (only successful) [s]")
            #df_stat = evaluator.addStatistics(df, drop_nan=False, drop_measures=True)
            #print(df_stat.round(2))
        #####################
        #df = evaluate.get_aggregated_query_statistics(type='timer', name='connection', query_aggregate='Median', dbms_filter=dbms_filter)
        df = evaluate.get_aggregated_experiment_statistics(type='timer', name='connection', query_aggregate='Max', total_aggregate='Max', dbms_filter=dbms_filter)
        df = (df/1000.0).sort_index()
        if not df.empty:
            print("\n### Max of Connection Times (only successful) [s]")
            df.columns = ['max connection time [s]']
            df = df.reindex(index=tools.natural_sort(df.index))
            print(df.round(2))
            #print("### Statistics of Timer Connection (only successful) [s]")
            #df_stat = evaluator.addStatistics(df, drop_nan=False, drop_measures=True)
            #print(df_stat.round(2))
        #####################
        df = evaluate.get_aggregated_experiment_statistics(type='timer', name='run', query_aggregate='Median', total_aggregate='Geo', dbms_filter=dbms_filter)
        df = (df/1000.0).sort_index()
        if not df.empty:
            print("\n### Geometric Mean of Medians of Run Times (only successful) [s]")
            df.columns = ['average run time [s]']
            df = df.reindex(index=tools.natural_sort(df.index))
            print(df.round(2))
        #####################
        df = evaluate.get_aggregated_experiment_statistics(type='timer', name='run', query_aggregate='Max', total_aggregate='Sum', dbms_filter=dbms_filter).astype('float')/1000.
        if not df.empty:
            print("### Sum of Maximum Run Times per Query (only successful) [s]")
            df.columns = ['sum of max run times [s]']
            df = df.reindex(index=tools.natural_sort(df.index))
            print(df.round(2))
        #####################
        df = num_processes*float(len(list_queries))*3600./df
        if not df.empty:
            print("### Queries per Hour (only successful) [QpH] - {}*{}*3600/(sum of max run times)".format(int(num_processes), int(len(list_queries))))
            df.columns = ['queries per hour [Qph]']
            df = df.reindex(index=tools.natural_sort(df.index))
            print(df.round(2))
        df_tpx = df.copy()
        #####################
        times_start = dict()
        times_end = dict()
        times_numbers = dict()
        tpx_sum = dict()
        #times = experiments.protocol['query']
        #print(times)
        for connection_nr, connection in evaluate.benchmarks.dbms.items():
            df_time = pd.DataFrame()
            c = connection.connectiondata
            #print(c)
            #connection_name = c['name']
            if 'orig_name' in c:
                orig_name = c['orig_name']
            else:
                orig_name = c['name']
            benchmarker_times_dict = evaluate.get_experiment_connection_properties(c['name'])
            if len(benchmarker_times_dict) > 0:
                benchmarker_times = benchmarker_times_dict['times']['total']
            #print(benchmarker_times)
            if len(orig_name) > 0: #'orig_name' in c:
                #print(c['name'], orig_name)
                if not orig_name in tpx_sum:
                    tpx_sum[orig_name] = 0
                #print(tpx_sum)
                #print(orig_name)
                if not orig_name in times_start:
                    times_start[orig_name] = []
                    times_end[orig_name] = []
                    times_numbers[orig_name] = 0
                if c['name'] in df_tpx.index:
                    tpx_sum[orig_name] = tpx_sum[orig_name] + df_tpx.loc[c['name']]['queries per hour [Qph]']
                    times_numbers[orig_name] = times_numbers[orig_name] + 1
                #else:
                #    print("Del", orig_name)
                #    del tpx_sum[orig_name]
                #times_numbers[orig_name] = times_numbers[orig_name] + 1
                #print(times_start)
                #print(times_end)
                for q in experiments.protocol['query']:
                    #print(experiments.protocol['query'][q])
                    #print(q, experiments.protocol['query'][q]['starts'], experiments.protocol['query'][q]['ends'])
                    times = experiments.protocol['query'][q]
                    if "ends" in times and c['name'] in times["ends"]:
                        time_start = int(datetime.datetime.timestamp(datetime.datetime.strptime(times["starts"][c['name']],'%Y-%m-%d %H:%M:%S.%f')))
                        time_end = int(datetime.datetime.timestamp(datetime.datetime.strptime(times["ends"][c['name']],'%Y-%m-%d %H:%M:%S.%f')))
                        times_start[orig_name].append(time_start)
                        times_end[orig_name].append(time_end)
                        #times_start[time_start] = i
                        #times_end[time_end] = i
                #print(times_start)
                #print(times_end)
                #for t in benchmarker_times:
                #    times_start[orig_name].append(benchmarker_times[t]['time_start'])
                #    times_end[orig_name].append(benchmarker_times[t]['time_end'])
            if tpx_sum[orig_name] == 0:
                del tpx_sum[orig_name]
        if len(tpx_sum) > 0:
            print("### Queries per Hour (only successful) [QpH] - Sum per DBMS")
            df = pd.DataFrame.from_dict(tpx_sum, orient='index', columns=['queries per hour [Qph]'])
            df.index.name = 'DBMS'
            df = df.reindex(index=tools.natural_sort(df.index))
            print(df.round(2))
        tpx_total = dict()
        if len(times_start) > 0:
            for c in times_start:
                #print(c, times_start[c], times_end[c])
                if len(times_start[c]) > 0:
                    time_start = min(times_start[c])
                    time_end = max(times_end[c])
                    time_span = time_end-time_start
                    if time_span == 0:
                        # if it is very fast (by error?), we still need a positive number
                        time_span = 1
                    num_results = times_numbers[c]
                    tpx = round(num_run*num_results*float(len(list_queries))*3600./(time_span), 2)
                    #print(c, time_start, time_end, time_span, tpx)
                    #print("{}: {}*{}*3600/{} = {}".format(c, num_results, int(len(list_queries)), time_span, tpx))
                    tpx_total[c] = {'queries per hour [Qph]': tpx, 'formula': "{}*{}*{}*3600/{}".format(num_run, num_results, int(len(list_queries)), time_span)}
            #print(tpx_total)
            if len(tpx_total) > 0:
                print("### Queries per Hour (only successful) [QpH] - (max end - min start)")
                df = pd.DataFrame.from_dict(tpx_total, orient='index')#, columns=['queries per hour [Qph]'])
                df.index.name = 'DBMS'
                df = df.reindex(index=tools.natural_sort(df.index))
                print(df)
        print("Experiment {} has been finished".format(experiments.code))
        return evaluate



