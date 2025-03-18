"""
    Helper classes and functions for benchmarking, timing, usage of JDBC, result DataFrame for the Python Package DBMS Benchmarker
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
from statistics import *
import numpy as np
import jaydebeapi
from timeit import default_timer #as timer
import pandas as pd
import logging
import math
import re
import ast
from os import path
import matplotlib.pyplot as plt
import pickle
import traceback
import warnings

from dbmsbenchmarker import inspector, benchmarker

# Set query timeout
jaydebeapi.QUERY_TIMEOUT = 0

def _set_stmt_parms(self, prep_stmt, parameters):
        for i in range(len(parameters)):
            prep_stmt.setObject(i + 1, parameters[i])
        #print("jaydebeapi.QUERY_TIMEOUT", jaydebeapi.QUERY_TIMEOUT)
        prep_stmt.setQueryTimeout(jaydebeapi.QUERY_TIMEOUT)

jaydebeapi.Cursor._set_stmt_parms = _set_stmt_parms


class timer():
    """
    Container for storing benchmarks (times).
    This is a list (queries) of lists (connections) of lists (runs).
    Use this by
    - loop over queries q
    -- loop over connections c
    --- startTimer()
    --- making n benchmarks for q and c
    ---- startTimerRun() and abortTimerRun() / finishTimerRun()
    --- abortTimer() or finishTimer()
    """
    header_stats = ["DBMS [ms]", "n", "mean", "stdev", "cv %", "qcod %", "iqr", "median", "min", "max"]
    def __init__(self, name):
        """
        Stores name of benchmark container (e.g. execution or data transfer)

        :param name: Name of benchmark times container
        :return: returns nothing
        """
        self.name = name
        self.start = 0
        self.end = 0
        self.currentQuery = 0
        self.stackable = True # this timer will be considered in stacked bar charts (sums up parts of benchmarks times)
        self.perRun = True # this timer measures per run (respects warmup/cooldown)
        self.clearBenchmarks()
    def clearBenchmarks(self):
        """
        Clears all benchmark related data

        :return: returns nothing
        """
        self.times = []
        self.stats = []
    @staticmethod
    def getStats(data):
        """
        Computes statistics to list of data.
        This is: mean, median, stdev, cv (coefficient of variation), qcod (Quartile coefficient of dispersion), iqr (Interquartile range), min and max.

        :param data: List of numbers
        :return: returns 6 statistical numbers as a list
        """
        # remove Nones for all statistics
        data = [x for x in data if x is not None]
        # remove zeros for some statistics
        data_no_zeros = list(filter((0.0).__ne__, data))
        if len(data_no_zeros) == 0:
            # we do not want to have an empty list
            data_no_zeros = data
        result = []
        numRun = len(data)
        if numRun == 0:
            return [0, 0, 0, 0, 0, 0, 0, 0, 0]
        t_mean = mean(data)
        #print("statistics for n runs: "+str(numRun))
        if numRun > 1:
            t_stdev = stdev(data)
        else:
            t_stdev = 0
        if t_mean > 0 and t_stdev > 0:
            t_cv = t_stdev / t_mean * 100.0
        else:
            t_cv = 0
        t_median = median(data_no_zeros)
        t_min = min(data_no_zeros)
        t_max = max(data_no_zeros)
        Q1 = np.percentile(data_no_zeros,25)
        Q3 = np.percentile(data_no_zeros,75)
        if Q3+Q1 > 0:
            t_qcod = 100.0*(Q3-Q1)/(Q3+Q1)
        else:
            t_qcod = 0
        if Q3-Q1 > 0:
            t_iqr = (Q3-Q1)
        else:
            t_iqr = 0
        result = [numRun, t_mean, t_stdev, t_cv, t_qcod, t_iqr, t_median, t_min, t_max]
        return result
    def startTimer(self, numQuery, query, nameConnection):
        """
        Stores number of warmup runs and benchmark runs.
        Also clears data of current query.
        This is a dict (connections) of lists (benchmark runs)
        Starts benchmark of one single connection with fixed query.
        Clears list of numbers of runs of current connection.

        :param query: Query object
        :param numRun: Number of benchmarking runs
        :return: returns nothing
        """
        self.nameConnection = nameConnection
        self.startTimerQuery(numQuery, query)#numWarmup, numRun)
        self.startTimerConnection()
        while len(self.times) <= self.currentQuery:
            self.times.append({})
            self.stats.append({})
        if len(self.times) >= self.currentQuery and self.nameConnection in self.times[self.currentQuery]:
            logging.debug("Benchmark "+self.name+" has already been done")
    def abortTimer(self):
        """
        Augments list of benchmarks of current connection by filling missing values with 0.

        :return: returns nothing
        """
        self.abortTimerConnection()
    def cancelTimer(self):
        """
        All benchmarks are set to 0. Timer will be ignored

        :return: returns nothing
        """
        self.cancelTimerConnection()
    def finishTimer(self):
        """
        Appends completed benchmarks to storage.
        This is a list of benchmarks (runs).
        Appends completed benchmarks of one single query to storage.
        This is a dict (connections) of lists (benchmark runs).

        :return: returns nothing
        """
        self.finishTimerConnection()
        self.finishTimerQuery()
        self.times[self.currentQuery][self.nameConnection] = self.time_c
        self.stats[self.currentQuery][self.nameConnection] = self.stat_c
        #if benchmarker.BENCHMARKER_VERBOSE_STATISTICS:
        #    print("Benchmark "+self.name+" has been stored for "+self.nameConnection+" mean: "+str(self.stats[self.currentQuery][self.nameConnection][0]))
    def skipTimer(self, numQuery, query, nameConnection):
        """
        Skips the timer, that is and empty list of measures is appended.

        :param numQuery: Number of query
        :param query: Query object of the query
        :param nameConnection: Name of the connection
        :return: returns nothing
        """
        self.nameConnection = nameConnection
        self.startTimerQuery(numQuery, query)#numWarmup, numRun)
        while len(self.times) <= self.currentQuery:
            self.times.append({})
            self.stats.append({})
        self.finishTimerQuery()
    def startTimerQuery(self, numQuery, query):# numWarmup, numRun):
        """
        Starts new (current) query and stores the query object of it.

        :param numQuery: Number of query
        :param query: Query object of the query
        :return: returns nothing
        """
        self.currentQuery = numQuery-1
        self.query = query
    def finishTimerQuery(self):
        """
        Purpose is to stop recording timer of a query.
        Does nothing currently.

        :return: returns nothing
        """
        pass
    def startTimerConnection(self):
        """
        Starts recording measures of one single connection with fixed query.
        Clears list of numbers of runs of current connection.

        :return: returns nothing
        """
        self.time_c = []
        self.stat_c = []
    def abortTimerConnection(self):
        """
        Augments list of benchmarks of current connection by filling missing values with 0.

        :return: returns nothing
        """
        # fill missing runs with 0
        #self.time_c.extend([0]*(self.numWarmup+self.numRun-len(self.time_c)))
        self.time_c.extend([0]*(self.query.numRun-len(self.time_c)))
    def cancelTimerConnection(self):
        """
        Augments list of benchmarks of current connection by filling missing values with 0.

        :return: returns nothing
        """
        # fill missing runs with 0
        self.time_c = [0]*(self.query.numRun)
    def finishTimerConnection(self):
        """
        Appends completed benchmarks to storage.
        This is a list of benchmarks (runs).

        :return: returns nothing
        """
        # compute statistics, ignore warmup
        if self.perRun:
            self.stat_c = timer.getStats(self.time_c[self.query.numRunBegin:self.query.numRunEnd])
        else:
            self.stat_c = timer.getStats(self.time_c)
    def startTimerRun(self):
        """
        Starts benchmark of one single run.
        This starts a timer.

        :return: returns nothing
        """
        self.start = default_timer()
    def finishTimerRun(self):
        """
        Ends benchmark of one single run.
        Computes duration in ms.
        Benchmark is stored in list (fixed query, one connection)

        :return: returns duration of current benchmark
        """
        self.end = default_timer()
        duration = self.end - self.start
        self.time_c.append(1000.0*duration)
        return self.end - self.start
    def abortTimerRun(self):
        """
        Ends benchmark of one single run.
        Same as finishTimerRun(), but time is set to 0 due to error.

        :return: returns 0
        """
        # same as finishTimerRun(), but time is 0
        self.end = self.start
        duration = self.end - self.start
        self.time_c.append(1000.0*duration)
        return self.end - self.start
    def appendTimes(self, times, query):# numWarmup):
        """
        Appends results of one single query.
        This is a list (connections) of benchmarks (runs).
        It also computes statistics and thereby ignores warmup runs.

        :param numWarmup: Number of warmup runs
        :return: returns nothing
        """
        if len(times)>0:
            if self.perRun:
                stats = {k: timer.getStats(v[query.numRunBegin:query.numRunEnd]) for k,v in times.items()}
            else:
                stats = {k: timer.getStats(v) for k,v in times.items()}
            #stats = {k: self.getStats(v[numWarmup:]) for k,v in times.items()}
        else:
            stats = {}
        self.times.append(times)
        self.stats.append(stats)
    def checkForBenchmarks(self, numQuery, nameConnection = None):
        """
        Checks if there is a list of benchmark runs for a given query and connection.

        :param numQuery: Number of query
        :param nameConnection: Name of connection
        :return: True if benchmark results are present
        """
        if nameConnection is None:
            return (len(self.times) >= numQuery)
        else:
            return (len(self.times) >= numQuery and nameConnection in self.times[numQuery-1])
    def checkForSuccessfulBenchmarks(self, numQuery, nameConnection = None):
        """
        Checks if there is a list of benchmark runs for a given query and connection and not all times are zero.

        :param numQuery: Number of query
        :param nameConnection: Name of connection
        :return: True if benchmark results are present
        """
        existing = self.checkForBenchmarks(numQuery, nameConnection)
        if nameConnection is not None:
            return(existing and not all(v == 0 for v in self.times[numQuery-1][nameConnection]))
        else:
            return(existing and not all(v == 0 for k,c in self.times[numQuery-1].items() for v in c))
    def tolist(self, numQuery):
        """
        Returns benchmarks of a given query as a list of lists.

        :param numQuery: Number of query
        :return: List of lists of benchmark times
        """
        l = [v for k,v in self.times[numQuery-1].items()]
        return l
    def toDataFrame(self, numQuery):
        """
        Returns benchmarks of a given query as a DataFrame (rows=dbms, cols=benchmarks).

        :param numQuery: Number of query
        :return: DataFrame of benchmark times
        """
        data =list(zip([[k] for k,v in self.times[numQuery-1].items()],[v for k,v in self.times[numQuery-1].items()]))
        # correct nesting 
        data2 = [[item for item in sublist] for sublist in data]
        l = [[item for sublist2 in sublist for item in sublist2] for sublist in data2]
        # convert times to DataFrame
        df = pd.DataFrame.from_records(l)
        return df
    def statsToDataFrame(self, numQuery):
        """
        Returns statistics of a given query as a DataFrame (rows=dbms, cols=statisticd).

        :param numQuery: Number of query
        :return: DataFrame of benchmark statistics
        """
        data =list(zip([[k] for k,v in self.stats[numQuery-1].items()],[v for k,v in self.stats[numQuery-1].items()]))
        # correct nesting 
        data2 = [[item for item in sublist] for sublist in data]
        l = [[item for sublist2 in sublist for item in sublist2] for sublist in data2]
        # convert times to DataFrame
        df = pd.DataFrame.from_records(l)
        if df.empty:
            logging.debug("no values")
            return df
        # format times for output
        header = timer.header_stats.copy()
        df.columns = header
        return df




class query():
    template = None
    """
    Container for storing queries.
    This converts a dict read from json to an object.
    It also checks values and sets defaults.
    """
    def __init__(self, querydict):
        """
        Creates object and stores data given in dict into object.

        :param query: Dict containing query infos - query, numRun, numParallel, withData, warmup, cooldown, title
        :return: returns nothing
        """
        self.maxTime = None
        self.numRunStd = 1
        self.numRun = 0
        self.numParallel = 1
        self.warmup = 0
        self.cooldown = 0
        self.active = True
        self.title = ''
        self.DBMS = {}
        self.parameter = {}
        self.withData = False
        self.storeData = False
        self.result = ""
        self.restrict_precision = None
        self.sorted = False
        self.storeResultSet = False
        self.storeResultSetFormat = []
        self.queryList = []
        self.query = ""
        self.withConnect = False
        self.timer = {}
        self.timer['connection'] = {}
        self.timer['connection']['active'] = False
        self.timer['datatransfer'] = {}
        self.timer['datatransfer']['active'] = False
        self.delay_connect = 0
        self.delay_run = 0
        # legacy naming
        #self.timer['transfer'] = {}
        #self.timer['transfer']['active'] = self.timer['datatransfer']['active']#False
        self.dictToObject(querydict)
        if query.template is not None:
            self.dictToObject(query.template)
        if self.numRun == 0:
            self.numRun = self.numRunStd
        self.numRunBegin = self.warmup
        self.numRunEnd = self.numRun-self.cooldown
        self.timer['run'] = {'active': True}
        self.timer['session'] = {'active': True}
    def dictToObject(self, query):
        """
        Converts dict into object.
        Sets missing values to defaults.

        :param query: Dict containing query infos - query, numRun, numParallel, withData, warmup, cooldown, title
        :return: returns nothing
        """
        if 'query' in query:
            self.query = query['query']
        if 'maxTime' in query:
            self.maxTime = float(query['maxTime'])
        if 'numRun' in query:
            self.numRun = int(query['numRun'])
        if 'numParallel' in query:
            self.numParallel = int(query['numParallel'])
        if 'active' in query:
            self.active = query['active']
        if 'numWarmup' in query:
            self.warmup = int(query['numWarmup'])
        if 'numCooldown' in query:
            self.cooldown = int(query['numCooldown'])
        if 'delay' in query:
            self.delay_run = float(query['delay'])
        if 'title' in query:
            self.title = query['title']
        if 'DBMS' in query:
            self.DBMS = query['DBMS']
        if 'parameter' in query:
            self.parameter = query['parameter']
        # timerExecution
        if 'timer' in query:
            self.timer = joinDicts(self.timer, query['timer'])
            self.timer['execution'] = {}
            self.timer['execution']['active'] = True
        # timerTransfer
        if 'datatransfer' in self.timer:
            if self.timer['datatransfer']['active']:
                self.withData = True
            #if 'store' in self.timer['datatransfer'] and self.timer['datatransfer']['store'] == True:
            #    self.storeData = True
            if 'compare' in self.timer['datatransfer'] and self.timer['datatransfer']['compare']:
                self.result = self.timer['datatransfer']['compare']
                self.storeData = True
                self.storeResultSet = True
                self.storeResultSetFormat = 'dataframe'
            if 'precision' in self.timer['datatransfer']:
                self.restrict_precision = self.timer['datatransfer']['precision']
                self.storeData = True
                self.storeResultSet = True
                self.storeResultSetFormat = 'dataframe'
            if 'sorted' in self.timer['datatransfer']:
                self.sorted = self.timer['datatransfer']['sorted']
                self.storeData = True
                self.storeResultSet = True
                self.storeResultSetFormat = 'dataframe'
            if 'store' in self.timer['datatransfer']:
                if self.timer['datatransfer']['store'] == False:
                    self.storeResultSet = False
                    self.storeData = False
                    self.storeResultSetFormat = ''
                else:
                    self.storeResultSet = True
                    self.storeData = True
                    if not self.timer['datatransfer']['store'] == True:
                        if isinstance(self.timer['datatransfer']['store'], str):
                            self.storeResultSetFormat = [self.timer['datatransfer']['store']]
                        else:
                            self.storeResultSetFormat = self.timer['datatransfer']['store']
        # timerConnect
        if 'connection' in self.timer:
            if self.timer['connection']['active']:
                self.withConnect = True
            if 'delay' in self.timer['connection']:
                self.delay_connect = float(self.timer['connection']['delay'])
        # we do not have a query string, but a list of (other) queries
        if 'queryList' in query:
            self.queryList = query['queryList']



def formatDuration(ms):
    """
    Formats duration given in ms to HH:ii:ss and using "," for 1000s

    :param ms: Time given in ms
    :return: returns formatted string
    """
    # truncate version
    #seconds = int((ms/1000)%60)
    #minutes = int((ms/(1000*60))%60)
    #hours = int((ms/(1000*60*60))%24)
    #s = "{:,.2f}ms = {:0>2d}:{:0>2d}:{:0>2d}".format(ms,hours,minutes,seconds)
    # ceil() version:
    seconds = int(math.ceil(ms/1000)%60)
    minutes = int(math.ceil((ms-1000*seconds)/(1000*60))%60)
    hours = int(math.ceil((ms-1000*seconds-1000*60*minutes)/(1000*60*60)))#%24)
    s = "{:,.2f}ms = {:0>2d}:{:0>2d}:{:0>2d}".format(ms,hours,minutes,seconds)
    return s



class dbms():
    """
    Container for storing queries.
    This converts a dict read from json to an object.
    It also checks values and sets defaults.
    """
    jars = []
    currentAnonymChar = 65
    anonymizer = {}
    deanonymizer = {}
    dbmscolors = {}
    def __init__(self, connectiondata, anonymous = False):
        """
        Converts dict into object.
        Anonymizes dbms if activated.

        :param connectiondata: Dict containing connection infos
        :return: returns nothing
        """
        self.connectiondata = connectiondata
        self.connection = None
        self.cursor = None
        self.product = "unknown"
        self.version = "unknown"
        self.driver = "unknown"
        self.driverversion = "unknown"
        if 'JDBC' in connectiondata:
            if not connectiondata['JDBC']['jar'] in dbms.jars:
                if isinstance(connectiondata['JDBC']['jar'], list):
                    # accept list of jars
                    dbms.jars.extend(connectiondata['JDBC']['jar'])
                else:
                    # append single jar
                    dbms.jars.append(connectiondata['JDBC']['jar'])
        if not 'version' in self.connectiondata:
            self.connectiondata['version'] = "-"
        if not 'info' in self.connectiondata:
            self.connectiondata['info'] = ""
        if not 'active' in self.connectiondata:
            self.connectiondata['active'] = True
        self.anonymous = anonymous
        # anonymous dbms get ascending char as name
        if self.anonymous:
            if 'alias' in self.connectiondata and len(self.connectiondata['alias']) > 0:
                # rename using docker alias names
                self.name = connectiondata['name'].replace(connectiondata['docker'], connectiondata['docker_alias'])
                print("Alias for "+self.connectiondata['name']+" is "+self.name)
                """
                if self.connectiondata['alias'] in dbms.anonymizer.values():
                    # rename first occurance of alias
                    old_origin = dbms.deanonymizer[self.connectiondata['alias']]
                    old_alias = self.connectiondata['alias']+" "+chr(dbms.currentAnonymChar)
                    dbms.currentAnonymChar = dbms.currentAnonymChar + 1
                    dbms.anonymizer[old_origin] = old_alias
                    dbms.deanonymizer[old_alias] = old_origin
                    print("Alias for "+old_origin+" became "+old_alias)
                if self.connectiondata['alias'] in dbms.anonymizer.values() or self.connectiondata['alias']+" A" in dbms.anonymizer.values():
                    # rename this occurance
                    self.name = self.connectiondata['alias']+" "+chr(dbms.currentAnonymChar)
                    dbms.currentAnonymChar = dbms.currentAnonymChar + 1
                else:
                    self.name = self.connectiondata['alias']
                """
            else:
                self.name = "DBMS "+chr(dbms.currentAnonymChar)
                dbms.currentAnonymChar = dbms.currentAnonymChar + 1
            print("Alias for "+self.connectiondata['name']+" is "+self.name)
        else:
            self.name = self.connectiondata['name']
        dbms.anonymizer[self.connectiondata['name']] = self.name
        dbms.deanonymizer[self.name] = self.connectiondata['name']
        #print(dbms.anonymizer)
        # is there a limit for parallel processes?
        # =1 means: not parallel
        # =0 means: take global setting
        if not 'numProcesses' in self.connectiondata:
            self.connectiondata['numProcesses'] = 0
        # color of dbms
        colors = plt.rcParams['axes.prop_cycle'].by_key()['color']
        if self.connectiondata['active']:
            dbms.dbmscolors[self.name] = colors[len(dbms.dbmscolors) % len(plt.rcParams['axes.prop_cycle'].by_key()['color'])]
    def connect(self):
        """
        Connects to one single dbms.
        Currently only JDBC is supported.

        :return: returns nothing
        """
        if 'JDBC' in self.connectiondata:
            # Set JVM options
            """
            jvm_options = [
                "-Xms128m",  # Initial heap size
                "-Xmx256m",  # Maximum heap size
                "-XX:+PrintGCDetails",  # Print GC details
                "-XX:+PrintGCTimeStamps",  # Print GC timestamps
                "-verbose:gc"  # Enable GC verbose output
            ]
            """
            if 'options' in self.connectiondata['JDBC']:
                jvm_options = self.connectiondata['JDBC']['options']
                if not benchmarker.BENCHMARKER_VERBOSE_NONE:
                    print("JVM options:", jvm_options)
            else:
                jvm_options = []
            self.connection = jaydebeapi.connect(
                self.connectiondata['JDBC']['driver'],
                self.connectiondata['JDBC']['url'],
                self.connectiondata['JDBC']['auth'],
                dbms.jars, jvm_options)#["-Xms1g", "-Xmx1g", "-XX:+PrintFlagsFinal"])
            try:
                self.metadata = self.connection.jconn.getMetaData()
                self.product = self.metadata.getDatabaseProductName()
                self.version = self.metadata.getDatabaseProductVersion()
                self.driver = self.metadata.getDriverName()
                self.driverversion = self.metadata.getDriverVersion()
                self.connectiondata['product'] = self.product
                self.connectiondata['version'] = self.version
                self.connectiondata['driver'] = self.driver
                self.connectiondata['driverversion'] = self.driverversion
                if not benchmarker.BENCHMARKER_VERBOSE_NONE:
                    print("Connected to {} version {} using {} version {}".format(self.product, self.version, self.driver, self.driverversion))
            except Exception as e:
                print("Product and version not implemented in JDBC driver {}".format(self.connectiondata['JDBC']['jar']))
            else:
                pass
            if 'init_SQL' in self.connectiondata:
                try:
                    query_init = self.connectiondata['init_SQL']
                    if not benchmarker.BENCHMARKER_VERBOSE_NONE:
                        print('init_SQL:', query_init)
                    self.openCursor()
                    if isinstance(query_init, list):
                        for command in query_init:
                            self.executeQuery(command)
                    else:
                        self.executeQuery(query_init)
                    #init_result = self.fetchResult()
                    self.closeCursor()
                except Exception as e:
                    print("Error when running init_SQL:", query_init, e)
                    #print(init_result)
            #self.connection.jconn.setAutoCommit(True)
        else:
            raise ValueError('No connection data for '+self.getName())
    def openCursor(self):
        """
        Opens cursor for current connection.

        :return: returns nothing
        """
        if self.connection is not None:
            self.cursor = self.connection.cursor()
    def closeCursor(self):
        """
        Closes cursor for current connection.

        :return: returns nothing
        """
        if self.cursor is not None:
            self.cursor.close()
            self.cursor = None
    def executeQuery(self, queryString):
        """
        Executes a query for current connection and cursor.

        :param queryString: SQL query to be executed
        :return: returns nothing
        """
        if self.cursor is not None:
            self.cursor.execute(queryString)
    def fetchResult(self):
        """
        Fetches result from current cursor.

        :return: returns result set
        """
        if self.cursor is not None:
            return self.cursor.fetchall()
        else:
            return []
    def disconnect(self):
        """
        Disconnects from one single dbms.

        :return: returns nothing
        """
        if self.connection is not None:
            self.connection.close()
            self.connection = None
    def getName(self):
        """
        Returns name of dbms, or alias if anonymous.

        :return: returns (anonymized) name
        """
        return self.name
    def hasHardwareMetrics(self):
        """
        Returns if monitoring service parameter are present in connection information.

        :return: returns nothing
        """
        # should hardware metrics be fetched from grafana
        if 'monitoring' in self.connectiondata and 'grafanatoken' in self.connectiondata['monitoring'] and 'grafanaurl' in self.connectiondata['monitoring'] and self.connectiondata['monitoring']['grafanatoken'] and self.connectiondata['monitoring']['grafanaurl']:
            return True
        # should hardware metrics be fetched from prometheus
        elif 'monitoring' in self.connectiondata and 'prometheus_url' in self.connectiondata['monitoring'] and len(self.connectiondata['monitoring']['prometheus_url']):
            return True
        else:
            return False
    def isActive(self):
        return self.connection is not None and self.cursor is not None


class dataframehelper():
    """
    Class for some handy DataFrame manipulations
    """
    @staticmethod
    def addFactor(dataframe, factor):
        """
        Adds factor column to DataFrame of benchmark statistics.
        This is a normalized version of the mean or median column.
        Also moves first column (dbms) to index.

        :param dataframe: Report data given as a pandas DataFrame
        :param factor: Column to take as basis for factor
        :return: returns converted dataframe
        """
        if dataframe.empty:
            return dataframe
        # select column 0 = connections
        connections = dataframe.iloc[0:,0].values.tolist()
        # only consider not 0, starting after dbms and n
        dataframe_non_zero = dataframe[(dataframe.T[3:] != 0).any()]
        # select column for factor and find minimum in cleaned dataframe
        factorlist = dataframe[factor]
        minimum = dataframe_non_zero[factor].min()
        # norm list to mean = 1
        if minimum > 0:
            mean_list_normed = [round(float(item/minimum),2) for item in factorlist]
        else:
            #print(dataframe_non_zero)
            mean_list_normed = [round(float(item),2) for item in factorlist]
        # transpose for conversion to dict
        dft = dataframe.transpose()
        # set column names
        dft.columns = dft.iloc[0]
        # remove first row
        df_transposed = dft[1:]
        # transform to dict
        d = df_transposed.to_dict(orient="list")
        # correct nesting, (format numbers?)
        stats_output = {k: [sublist for sublist in [stat_q]] for k,stat_q in d.items()}
        # convert times to DataFrame
        data = []
        for c in connections:
            if c in stats_output:
                l = list([c])
                l.extend(*stats_output[c])
                data.append(l)
        dataframe = pd.DataFrame.from_records(data)
        header = timer.header_stats.copy()
        dataframe.columns = header
        # insert factor column
        dataframe.insert(loc=1, column='factor', value=mean_list_normed)
        # sort by factor
        dataframe = dataframe.sort_values(dataframe.columns[1], ascending = True)
        # replace float by string
        dataframe = dataframe.replace(0.00, "0.00")
        # drop rows of only 0 (starting after factor and n)
        dataframe = dataframe[(dataframe.T[3:] != "0.00").any()]
        # replace string by float, except for first column (dbms names)
        dataframe = dataframe.astype({col: float for col in dataframe.columns[1:]})
        #dataframe = dataframe.replace("0.00", 0.00)
        # anonymize dbms
        dataframe.iloc[0:,0] = dataframe.iloc[0:,0].map(dbms.anonymizer)
        dataframe = dataframe.set_index(dataframe.columns[0])
        return dataframe
    @staticmethod
    def sumPerTimer(benchmarker, numQuery, timer):
        """
        Generates a dataframe (for bar charts) of the time series of a benchmarker.
        Rows=dbms, cols=timer, values=sum of times
        Anonymizes dbms if activated.

        :param benchmarker: Benchmarker object that contains all information about the benchmark, connections and queries
        :param numQuery: Number of query to generate dataframe of (None means all)
        :param timer: Timer containing benchmark results
        :return: returns dataframe and title
        """
        sums = list(range(0,len(timer)))
        timerNames = [t.name for t in timer]
        bValuesFound = False
        numQueriesEvaluated = 0
        numBenchmarks = 0
        validQueries = findSuccessfulQueriesAllDBMS(benchmarker, numQuery, timer)
        for numTimer,t in enumerate(timer):
            numFactors = 0
            logging.debug("sumPerTimer: Check timer "+t.name)
            sums[numTimer] = {}
            if not t.stackable:
                logging.debug(t.name+" is not stackable")
                continue
            # are there benchmarks for this query?
            for i,q in enumerate(t.times):
                # does this timer contribute?
                if not i in validQueries[numTimer]:
                    continue
                logging.debug("timer "+str(numTimer)+" is valid for query Q"+str(i+1))
                df = benchmarker.statsToDataFrame(i+1, t)
                #print(df)
                queryObject = query(benchmarker.queries[i])
                # no active dbms missing for this timer and query
                numQueriesEvaluated = numQueriesEvaluated + 1
                if numQuery is None:
                    logging.debug(str(numQueriesEvaluated)+"=Q"+str(i+1)+" in total bar chart of timer "+t.name+" - all active dbms contribute")
                bValuesFound = True
                numFactors = numFactors + 1
                for c,values in q.items():
                    if benchmarker.dbms[c].connectiondata['active']:
                        dbmsname = benchmarker.dbms[c].getName()
                        if dbmsname in df.index:
                            value_to_add = float(df.loc[dbmsname].loc[benchmarker.queryconfig['factor']])
                            numBenchmarks += len(values[queryObject.numRunBegin:queryObject.numRunEnd])
                            logging.debug("Added "+dbmsname+": "+str(value_to_add))
                            if dbmsname in sums[numTimer]:
                                sums[numTimer][dbmsname] = sums[numTimer][dbmsname] + value_to_add
                            else:
                                sums[numTimer][dbmsname] = value_to_add
            sums[numTimer] = {c: x/numFactors for c,x in sums[numTimer].items()}
        if not bValuesFound:
            return None, ''
        df = pd.DataFrame(sums, index=timerNames)
        df=df.fillna(0.0)
        # remove zero rows (timer missing)
        df = df[(df.T[0:] != 0).any()]
        d = df.transpose()
        # anonymize dbms
        #d.index = d.index.map(dbms.anonymizer)
        # add column total timer
        d['total']=d.sum(axis=1)
        # remove zero rows (dbms missing)
        d = d[(d.T[0:] != 0).any()]
        # remove zero columns (timer missing)
        d = d.loc[:, (d != 0).any(axis=0)]
        if d.empty:
            logging.debug("no values")
            return None, ''
        # sort by total
        d = d.sort_values('total', ascending = True)
        # drop total
        d = d.drop('total',axis=1)
        # add unit to columns
        d.columns = d.columns.map(lambda s: s+' [ms]')
        # label chart
        if benchmarker.queryconfig['factor'] == 'mean':
            chartlabel = 'Arithmetic mean of mean times'
        elif benchmarker.queryconfig['factor'] == 'median':
            chartlabel = 'Arithmetic mean of median times'
        if numQuery is None:
            title = chartlabel+" in "+str(numQueriesEvaluated)+" benchmarks ("+str(numBenchmarks)+" measurements) [ms]"
        else:
            title = "Q"+str(numQuery)+": "+chartlabel+" [ms] in "+str(queryObject.numRun-queryObject.warmup-queryObject.cooldown)+" benchmark test runs"
        return d, title
    @staticmethod
    def multiplyPerTimer(benchmarker, numQuery, timer):
        """
        Generates a dataframe (for bar charts) of the time series of a benchmarker.
        Rows=dbms, cols=timer, values=product of factors of times
        Anonymizes dbms if activated.

        :param benchmarker: Benchmarker object that contains all information about the benchmark, connections and queries
        :param numQuery: Number of query to generate dataframe of (None means all)
        :param timer: Timer containing benchmark results
        :return: returns dataframe and title
        """
        #logging.basicConfig(level=logging.DEBUG)
        sums = list(range(0,len(timer)))
        timerNames = [t.name for t in timer]
        bValuesFound = False
        numQueriesEvaluated = 0
        numBenchmarks = 0
        validQueries = findSuccessfulQueriesAllDBMS(benchmarker, numQuery, timer)
        for numTimer,t in enumerate(timer):
            # factors per dbms
            numFactors = {}
            logging.debug("multiplyPerTimer: Check timer "+t.name)
            sums[numTimer] = {}
            # we want to keep not stackable for table chart
            #if not t.stackable:
            #    logging.debug(t.name+" is not stackable")
            #    continue
            # are there benchmarks for this query?
            for i,q in enumerate(t.times):
                # does this timer contribute?
                if not i in validQueries[numTimer]:
                    continue
                logging.debug("timer "+str(numTimer)+" is valid for query Q"+str(i+1))
                df = benchmarker.statsToDataFrame(i+1, t)
                queryObject = query(benchmarker.queries[i])
                # at least one DBMS does not contribute (because of zero value)
                bMissingFound = False
                # mean value (i.e. sum of all values)
                for c,values in q.items():
                    #print(values)
                    if benchmarker.dbms[c].connectiondata['active']:
                        dbmsname = benchmarker.dbms[c].getName()
                        if dbmsname in df.index:
                            #print(df.loc[dbmsname].loc['factor'])
                            #print(df)
                            value_to_multiply = float(df.loc[dbmsname].loc['factor'])
                            #print(value_to_multiply)
                            if value_to_multiply == 0:
                                # we have some values, but none counting because of warmup
                                #print("Remove")
                                #print(dbmsname)
                                bMissingFound = True
                                break
                #print(t.name)
                #print(str(i+1))
                if bMissingFound or df.empty:
                    #numQueriesEvaluated = numQueriesEvaluated - 1
                    continue
                #print("OK")
                # no active dbms missing for this timer and query
                numQueriesEvaluated = numQueriesEvaluated + 1
                if numQuery is None:
                    logging.debug(str(numQueriesEvaluated)+"=Q"+str(i+1)+" in total bar chart of timer "+t.name+" - all active dbms contribute")
                bValuesFound = True
                for c,values in q.items():
                    if benchmarker.dbms[c].connectiondata['active']:
                        dbmsname = benchmarker.dbms[c].getName()
                        if dbmsname in df.index:
                            value_to_multiply = float(df.loc[dbmsname].loc['factor'])
                            if value_to_multiply == 0:
                                # we have some values, but none counting because of warmup
                                # this should not happen here
                                # bMissingFound = True
                                continue
                            if t.perRun:
                                numBenchmarks += len(values[queryObject.numRunBegin:queryObject.numRunEnd])
                            if not dbmsname in numFactors:
                                numFactors[dbmsname] = 0
                            numFactors[dbmsname] = numFactors[dbmsname] + 1
                            logging.debug("Multiplied "+dbmsname+": "+str(value_to_multiply))
                            if dbmsname in sums[numTimer]:
                                sums[numTimer][dbmsname] = sums[numTimer][dbmsname] * value_to_multiply
                            else:
                                sums[numTimer][dbmsname] = value_to_multiply
            #logging.debug(str(numFactors[dbmsname])+" factors")
            sums[numTimer] = {c: x ** (1/numFactors[c]) for c,x in sums[numTimer].items()}
        if not bValuesFound:
            return None, ''
        df = pd.DataFrame(sums, index=timerNames)
        df=df.fillna(0.0)
        # remove zero rows (timer missing)
        df = df[(df.T[0:] != 0).any()]
        d = df.transpose()
        # anonymize dbms
        #d.index = d.index.map(dbms.anonymizer)
        # add column total timer
        d['total']=d.sum(axis=1)
        # remove zero rows (dbms missing)
        d = d[(d.T[0:] != 0).any()]
        # remove zero columns (timer missing)
        d = d.loc[:, (d != 0).any(axis=0)]
        if d.empty:
            logging.debug("no values")
            return None, ''
        # sort by total
        d = d.sort_values('total', ascending = True)
        # drop total
        d = d.drop('total',axis=1)
        # label chart
        if benchmarker.queryconfig['factor'] == 'mean':
            chartlabel = 'Geometric mean of factors of mean times'
        elif benchmarker.queryconfig['factor'] == 'median':
            chartlabel = 'Geometric mean of factors of median times'
        if numQuery is None:
            title = chartlabel+" in "+str(numQueriesEvaluated)+" benchmarks"# ("+str(numBenchmarks)+" runs)"
        else:
            title = "Q"+str(numQuery)+": "+chartlabel+" in "+str(queryObject.numRun-queryObject.warmup-queryObject.cooldown)+" benchmark test runs"
        #logging.basicConfig(level=logging.ERROR)
        return d, title
    @staticmethod
    def totalTimes(benchmarker, dbms_filter=[]):
        """
        Generates a dataframe (for bar charts) of the time series of a benchmarker.
        Rows=queries, cols=dbms, values=sum of times
        Anonymizes dbms if activated.

        :param benchmarker: Benchmarker object that contains all information about the benchmark, connections and queries
        :param dbms_filter: List of connections to keep
        :return: returns nothing
        """
        # find position of execution timer
        e = [i for i,t in enumerate(benchmarker.timers) if t.name=="execution"]
        # list of active queries for timer e[0] = execution
        qs = findSuccessfulQueriesAllDBMS(benchmarker, None, benchmarker.timers, dbms_filter)[e[0]]
        if len(qs) == 0:
            return None, ""
        # list of active dbms
        cs = [i for i,q in benchmarker.dbms.items() if q.connectiondata['active']]
        times1 = dict.fromkeys(cs, list())
        times = {c:[] for c,l in times1.items()}
        for i in range(len(qs)):
            for q,c in enumerate(cs):
                if c in benchmarker.protocol['query'][str(qs[i]+1)]['durations']:
                    times[c].append(benchmarker.protocol['query'][str(qs[i]+1)]['durations'][c])
                else:
                    times[c].append(0.0)
        dataframe = pd.DataFrame.from_records(times)
        dataframe.index = qs
        dataframe.index = dataframe.index.map(lambda r: "Q"+str(r+1))
        dataframe.columns = dataframe.columns.map(dbms.anonymizer)
        title = 'Total times of '+str(len(times[c]))+" queries"
        return dataframe, title
    @staticmethod
    def DEPRECATED_timesToStatsDataFrame(times):
        l = timer.getStats(times)
        # convert statistics to DataFrame
        df = pd.DataFrame.from_records([l])
        header = timer.header_stats.copy()
        df.columns = header[1:]
        return df
    @staticmethod
    def resultsetToDataFrame(data):
        """
        Generates a dataframe from a result set.

        :param data: Result set as a list
        :return: dataframe of result set
        """
        df = pd.DataFrame.from_records(data)
        # set column names
        df.columns = df.iloc[0]
        # remove first row
        df = df[1:]
        return df
    @staticmethod
    def evaluateHardwareToDataFrame(evaluation):
        """
        Generates a dataframe from system parameters.

        :param evaluation: Evaluation object
        :return: dataframe of system (hardware) parameters
        """
        df1=pd.DataFrame.from_dict({c:d['hardwaremetrics'] for c,d in evaluation['dbms'].items()}).transpose()
        df2=pd.DataFrame.from_dict({c:d['hostsystem'] for c,d in evaluation['dbms'].items()}).transpose()
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
        df.index = df.index.map(dbms.anonymizer)
        df = dataframehelper.addStatistics(df)
        df = df.applymap(lambda x: x if not np.isnan(x) else 0.0)
        return df
    @staticmethod
    def DEPRECATED_addStatistics(df):
        #print("Compute statistics")
        #print(df)
        #with_nan = False
        #print(df)
        if df.isnull().any().any():
            #print("Missing!")
            with_nan = True
            df = df.dropna()
        stat_mean = df.mean()
        #print(stat_mean)
        stat_std = df.std()
        stat_q1 = df.quantile(0.25)
        stat_q2 = df.quantile(0.5)
        stat_q3 = df.quantile(0.75)
        stat_min = df.min()
        stat_max = df.max()
        stat_n = len(df.index)
        #print(stat_q1)
        #print(stat_q3)
        df.loc['n']=stat_n
        df.loc['Mean']= stat_mean
        df.loc['Std Dev']= stat_std
        df.loc['Std Dev'] = stat_std.map(lambda x: x if not np.isnan(x) else 0.0)
        df.loc['cv [%]']= df.loc['Std Dev']/df.loc['Mean']*100.0
        df.loc['Median']= stat_q2
        df.loc['iqr']=stat_q3-stat_q1
        df.loc['qcod [%]']=(stat_q3-stat_q1)/(stat_q3+stat_q1)*100.0
        df.loc['Min']=stat_min
        df.loc['Max']=stat_max
        #if with_nan:
        #    print(df)
        return df
    @staticmethod
    def evaluateMonitoringToDataFrame(evaluation):
        df = pd.DataFrame.from_dict({c:d['hardwaremetrics'] for c,d in evaluation['dbms'].items() if 'hardwaremetrics' in d}).transpose()
        df.index = df.index.map(dbms.anonymizer)
        #df = pd.DataFrame.from_dict({c:d['hardwaremetrics'] if 'hardwaremetrics' in d else [] for c,d in evaluation['dbms'].items()}).transpose()
        df = df.astype(float)
        df = dataframehelper.addStatistics(df)
        df = df.applymap(lambda x: x if not np.isnan(x) else 0.0)
        return df
    def evaluateHostToDataFrame(evaluation):
        df1 = pd.DataFrame.from_dict({c:d['hostsystem'] for c,d in evaluation['dbms'].items() if 'hostsystem' in d}).transpose()
        #print(df1)
        if df1.empty:
            return df1
        #print({c:d['prices']['benchmark_usd'] for c,d in evaluation['dbms'].items()})
        df2 = pd.DataFrame.from_dict({c:{'benchmark_usd':d['prices']['benchmark_usd'],'benchmark_time_s':d['times']['benchmark_ms']/1000.0,'total_time_s':(d['times']['load_ms']+d['times']['benchmark_ms'])/1000.0} for c,d in evaluation['dbms'].items() if 'prices' in d and 'benchmark_usd' in d['prices']}).transpose()
        if df2.empty:
            return df2
        df = df1.merge(df2,left_index=True,right_index=True)
        df.index = df.index.map(dbms.anonymizer)
        if 'CUDA' in df.columns:
            df = df.drop(['CUDA'],axis=1)
        if 'node' in df.columns:
            df = df.drop(['node'],axis=1)
        if 'instance' in df.columns:
            df = df.drop(['instance'],axis=1)
        if 'GPUIDs' in df.columns:
            df = df.drop(['GPUIDs'],axis=1)
        if 'requests_cpu' in df.columns:
            df = df.drop(['requests_cpu'],axis=1)
        if 'requests_memory' in df.columns:
            df = df.drop(['requests_memory'],axis=1)
        if 'limits_cpu' in df.columns:
            df = df.drop(['limits_cpu'],axis=1)
        if 'limits_memory' in df.columns:
            df = df.drop(['limits_memory'],axis=1)
        df = df.drop(['host','CPU','GPU'],axis=1)#,'RAM','Cores'
        df = df.astype(float)
        if 'RAM' in df:
            df['RAM'] = df['RAM']/1024/1024
        if 'disk' in df:
            df['disk'] = df['disk']/1024
        if 'datadisk' in df:
            df['datadisk'] = df['datadisk']/1024
        df = dataframehelper.addStatistics(df)
        df = df.applymap(lambda x: x if not np.isnan(x) else 0.0)
        return df
    @staticmethod
    def evaluateTimerfactorsToDataFrame(evaluation, timer):
        #l=e.evaluation['query'][1]['benchmarks']['execution']['statistics']
        factors = {}
        rows = []
        #print(timer.name)
        connections = [c for c, v in evaluation['dbms'].items()]
        #print(connections)
        for i,q in evaluation['query'].items():
            #print(q)
            if q['config']['active']:
                #print(i)
                rows.append('Q'+str(i))
                if 'benchmarks' in q and 'statistics' in q['benchmarks'][timer.name]:
                    # there are (some) results for this query
                    stats = q['benchmarks'][timer.name]['statistics']
                    for j, c in enumerate(connections):
                        if not c in factors:
                            factors[c] = []
                        if c in stats:
                            factors[c].append(stats[c]['factor'])
                        elif c in dbms.anonymizer and dbms.anonymizer[c] in stats:
                            factors[c].append(stats[dbms.anonymizer[c]]['factor'])
                        else:
                            factors[c].append(None)
                else:
                    # there are no results for this (active) query
                    for j, c in enumerate(connections):
                        if not c in factors:
                            factors[c] = []
                        else:
                            factors[c].append(None)
        #l={c:len(k) for c,k in factors.items()}
        #print(l)
        df = pd.DataFrame(factors)
        df.columns = df.columns.map(dbms.anonymizer)
        df = df.reindex(sorted(df.columns), axis=1)
        df.index = rows
        #print(df)
        return df
    @staticmethod
    def evaluateTPSToDataFrame(evaluation):
        factors = {}
        rows = []
        for i,q in evaluation['query'].items():
            if q['config']['active']:
                rows.append('Q'+str(i))
                for c,d in q['dbms'].items():
                    if c in evaluation['dbms']:
                        # dbms is active
                        if not c in factors:
                            factors[c] = []
                        if 'metrics' in d and 'throughput_run_mean_ps' in d['metrics']:
                            factors[c].append(d['metrics']['throughput_run_mean_ps'])
                        else:
                            factors[c].append(None)
                        #print(c)
                        #print(d['factor'])
        #print(factors)
        df = pd.DataFrame(factors)
        df.columns = df.columns.map(dbms.anonymizer)
        df = df.reindex(sorted(df.columns), axis=1)
        #df.index = df.index.map(lambda x: 'Q'+str(x+1))
        df.index = rows
        #print(df)
        return df
    @staticmethod
    def evaluateLatToDataFrame(evaluation):
        factors = {}
        rows = []
        for i,q in evaluation['query'].items():
            if q['config']['active']:
                #print(q)
                rows.append('Q'+str(i))
                for c,d in q['dbms'].items():
                    if c in evaluation['dbms']:
                        if 'metrics' in d and 'latency_run_mean_ms' in d['metrics']:
                            if not c in factors:
                                factors[c] = []
                            factors[c].append(d['metrics']['latency_run_mean_ms']/1000.0)
                            #print(c)
                            #print(d['factor'])
                        else:
                            if not c in factors:
                                factors[c] = []
                            factors[c].append(None)
                            #print(i)
                            #print(c)
        #print(factors)
        df = pd.DataFrame(factors)
        df.columns = df.columns.map(dbms.anonymizer)
        df = df.reindex(sorted(df.columns), axis=1)
        #df.index = df.index.map(lambda x: 'Q'+str(x+1))
        df.index = rows
        #print(df)
        return df
    @staticmethod
    def evaluateQueuesizeToDataFrame(evaluation):
        factors = {}
        rows = []
        for i,q in evaluation['query'].items():
            if q['config']['active']:
                #print(q)
                rows.append('Q'+str(i))
                for c,d in q['dbms'].items():
                    if c in evaluation['dbms']:
                        if 'metrics' in d and 'queuesize_run' in d['metrics']:
                            if not c in factors:
                                factors[c] = []
                            factors[c].append(d['metrics']['queuesize_run']/d['connectionmanagement']['numProcesses']*100.0)
                            #print(c)
                            #print(d['factor'])
                        else:
                            if not c in factors:
                                factors[c] = []
                            factors[c].append(None)
                            #print(i)
                            #print(c)
        #print(factors)
        df = pd.DataFrame(factors)
        df.columns = df.columns.map(dbms.anonymizer)
        df = df.reindex(sorted(df.columns), axis=1)
        #df.index = df.index.map(lambda x: 'Q'+str(x+1))
        df.index = rows
        #print(df)
        return df
    @staticmethod
    def evaluateResultsizeToDataFrame(evaluation):
        # heatmap of resultsize
        l = {i: {c: d['received_size_byte'] if 'received_size_byte' in d else 0 for c,d in q['dbms'].items() if c in evaluation['dbms']} for i,q in evaluation['query'].items() if q['active']}
        df = pd.DataFrame(l)
        d = df.replace(0, np.nan).min()
        df = df.T
        df.index = ['Q'+j for i,j in enumerate(df.index)]
        # find first active query
        df.columns = list(l[list(l.keys())[0]].keys())
        df.columns = df.columns.map(dbms.anonymizer)
        df = df.reindex(sorted(df.columns), axis=1)
        return df
    @staticmethod
    def evaluateNormalizedResultsizeToDataFrame(evaluation):
        # heatmap of (normalized) resultsize
        l = {i: {c: d['received_size_byte'] if 'received_size_byte' in d else 0 for c,d in q['dbms'].items() if c in evaluation['dbms']} for i,q in evaluation['query'].items() if q['active']}
        df = pd.DataFrame(l)
        d = df.replace(0, np.nan).min()
        # normalization
        df = df.div(d).replace(0,np.nan)
        df = df.T
        df.index = ['Q'+j for i,j in enumerate(df.index)]
        # find first active query
        df.columns = list(l[list(l.keys())[0]].keys())
        df.columns = df.columns.map(dbms.anonymizer)
        df = df.reindex(sorted(df.columns), axis=1)
        return df
    def evaluateErrorsToDataFrame(evaluation):
        # heatmap of errors
        l = {i: {c: True if 'error' in d else False for c,d in q['dbms'].items() if c in evaluation['dbms']} for i,q in evaluation['query'].items() if q['active']}
        df = pd.DataFrame(l)
        df = df.T
        df.index = ['Q'+j for i,j in enumerate(df.index)]
        # find first active query
        df.columns = list(l[list(l.keys())[0]].keys())
        df.columns = df.columns.map(dbms.anonymizer)
        df = df.reindex(sorted(df.columns), axis=1)
        return df
    def evaluateWarningsToDataFrame(evaluation):
        # heatmap of errors
        l = {i: {c: True if 'warning' in d else False for c,d in q['dbms'].items() if c in evaluation['dbms']} for i,q in evaluation['query'].items() if q['active']}
        df = pd.DataFrame(l)
        df = df.T
        df.index = ['Q'+j for i,j in enumerate(df.index)]
        # find first active query
        df.columns = list(l[list(l.keys())[0]].keys())
        df.columns = df.columns.map(dbms.anonymizer)
        df = df.reindex(sorted(df.columns), axis=1)
        return df
    @staticmethod
    def merge(*args):
        df = args[0]
        for i, df2 in enumerate(args):
            if i > 0:
                df = df.merge(df2,left_index=True,right_index=True)
        return df#df1.merge(df2,left_index=True,right_index=True)
    @staticmethod
    def collect(dataframe, column, alias, dataframe_collect=None):
        if not column in dataframe.columns:
            # nothing to add
            if dataframe_collect is not None:
                return dataframe_collect
            else:
                return pd.DataFrame()
        df = pd.DataFrame(dataframe[column]).rename(columns = {column:alias})
        if dataframe_collect is not None:
            return dataframehelper.merge(dataframe_collect, df)
        else:
            return df
    @staticmethod
    def getWorkflow(benchmarker):
        #print("getWorkflow")
        filename = benchmarker.path+'/experiments.config'
        if path.isfile(filename):
            print("config found")
            with open(filename, 'r') as f:
                d = ast.literal_eval(f.read())
            workflow = {}
            instance = ''
            volume = ''
            docker = ''
            script = ''
            clients = ''
            rpc = ''
            for i,step in enumerate(d):
                if 'connection' in step:
                    connection = step['connection']
                else:
                    connection = ''
                if 'delay' in step:
                    delay = step['delay']
                else:
                    delay = ''
                if 'instance' in step:
                    instance = step['instance']
                if 'docker' in step:
                    dbms = [k for k,d in step['docker'].items()]
                    docker = dbms[0]
                if 'initscript' in step:
                    scripts = [k for k,s in step['initscript'].items()]
                    script = scripts[0]
                if 'volume' in step:
                    volume = step['volume']
                if 'connectionmanagement' in step:
                    if 'numProcesses' in step['connectionmanagement']:
                        clients = step['connectionmanagement']['numProcesses']
                    if 'runsPerConnection' in step['connectionmanagement']:
                        rpc = step['connectionmanagement']['runsPerConnection']
                workflow[i] = [step['step'], instance, volume, docker, script, connection, delay, clients, rpc]
            df = pd.DataFrame.from_dict(workflow, orient='index', columns=['step', 'instance', 'volume', 'dbms', 'script', 'connection', 'delay', 'clients', 'rpc'])
            #print(df)
            return df
        else:
            return None


def findSuccessfulQueriesAllDBMS(benchmarker, numQuery, timer, dbms_filter=[]):
    """
    Find all queries where all dbms retrieved results successfully for a given list of timers.
    These may be taken into account for comparisons and a total bar chart.
    Anonymizes dbms if activated.

    :param numQuery: Number of query to inspect (optional)
    :param timer: Timer containing benchmark results
    :return: returns list of successful queries per timer
    """
    validQueries = list(range(0,len(timer)))
    for numTimer,t in enumerate(timer):
        logging.debug("Bar chart: Check timer "+t.name)
        validQueries[numTimer] = []
        # are there benchmarks for this query?
        if numQuery is not None and not t.checkForBenchmarks(numQuery):
            continue
        for i,q in enumerate(t.times):
            # does this timer contribute?
            if not t.checkForBenchmarks(i+1):
                continue
            queryObject = query(benchmarker.queries[i])
            # is timer active for this query?
            if not t.name in queryObject.timer or not queryObject.timer[t.name]['active']:
                continue
            bIgnoreQuery = False
            if numQuery is None or (numQuery > 0 and numQuery-1 == i):
                # use all queries (total) or this query is requested
                if numQuery is None:
                    for connectionname, c in benchmarker.dbms.items():
                        # for total: ignore dbms not in dbms_filter
                        if len(dbms_filter) > 0 and connectionname not in dbms_filter:
                            logging.debug("Total bar: Ignore connection "+str(connectionname)+" - filter")
                            continue
                        # ignore queries not active
                        if not queryObject.active:
                            logging.debug("Total bar: Ignore query "+str(i+1)+" - query inactive")
                            bIgnoreQuery = True
                        # for total: only consider queries completed by all active dbms
                        elif not connectionname in q and c.connectiondata['active']:
                            logging.debug("Total bar: Ignore query "+str(i+1)+" - missing dbms "+connectionname)
                            bIgnoreQuery = True
                        # for total: only consider active dbms without error
                        elif c.connectiondata['active'] and all(v == 0 for v in q[connectionname]):
                            logging.debug("Total bar: Ignore query "+str(i+1)+" - data 0")
                            bIgnoreQuery = True
                        if bIgnoreQuery:
                            break
                if not bIgnoreQuery:
                    # no active dbms missing for this timer and query
                    validQueries[numTimer].append(i)
                    logging.debug("Query is successful: {}".format(i))
    return validQueries



import ast
def convertToFloat(var):
    """
    Converts variable to float.

    :param var: Some variable
    :return: returns float converted variable
    """
    #print(var)
    #print(type(var))
    try:
        if isinstance(var, float):
            #print(var)
            #print("is float")
            return float
        return type(ast.literal_eval(var))
    except Exception as e:
        #print(str(e))
        #print("Not convertible")
        #print(var)
        return str

# Custom key function to handle conversion for sorting
def convert_to_rounded_float(var, decimals=2):
    try:
        # Try to convert to float and return a tuple (0, float_value)
        return (0, round(float(var), decimals))
    except ValueError:
        # If conversion fails, return a tuple (1, string_value)
        return (1, str(var))

# Create a key function for sorting based on all elements
def sort_key_rounded(sublist, decimal=2):
    return [convert_to_rounded_float(item, decimal) for item in sublist]

def convertToInt(var):
    """
    Converts variable to float.

    :param var: Some variable
    :return: returns float converted variable
    """
    #print(var)
    #print(type(var))
    try:
        return int(var)
    except Exception as e:
        #print(str(e))
        #print("Not convertible")
        #print(var)
        return var


def convert_to_rounded_float_2(var, decimals=2):
    """
    Converts a variable to a rounded float if possible, otherwise returns the original value.

    :param var: The variable to be converted.
    :param decimals: The number of decimal places to round to.
    :return: The rounded float or the original value if conversion is not possible.
    """
    def safe_literal_eval(var):
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always", SyntaxWarning)  # Catch all SyntaxWarnings
            try:
                result = ast.literal_eval(var)
                # Check if a SyntaxWarning was issued
                if len(w) > 0 and issubclass(w[-1].category, SyntaxWarning):
                    raise ValueError("SyntaxWarning encountered")
                return result
            except (ValueError, SyntaxError):
                raise
    try:
        # If var is already a float, just round and return it
        if isinstance(var, float):
            return round(var, decimals)
        # If var is "None"", just return it
        if var == "None":
            return var
        # Try to sanitize and convert the variable to a float
        if isinstance(var, str):
            var = var.replace("_", "")  # Remove any underscores
        evaluated_var = safe_literal_eval(var)
        if not evaluated_var is None:
            # Convert the evaluated variable to a float and round it
            rounded_float = round(float(evaluated_var), decimals)
        else:
            rounded_float = var
        #print("rounded", var, rounded_float)
        return rounded_float
    except (ValueError, SyntaxError):
        # Return the original value if conversion is not possible
        #print("not rounded", var)
        return var


def sizeof_fmt(num, suffix='B'):
    """
    Formats data size into human readable format.
    https://stackoverflow.com/questions/1094841/reusable-library-to-get-human-readable-version-of-file-size

    :param num: Data size
    :param suffix: 'B'
    :return: returns human readable data size
    """
    for unit in ['','Ki','Mi','Gi','Ti','Pi','Ei','Zi']:
        if abs(num) < 1024.0:
            return "%3.1f %s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f %s%s" % (num, 'Yi', suffix)



def tex_escape(text):
    """
    Escapes string so it's latex compatible.
    https://stackoverflow.com/questions/16259923/how-can-i-escape-latex-special-characters-inside-django-templates

    :param text: a plain text message
    :return: the message escaped to appear correctly in LaTeX
    """
    conv = {
        '&': r'\&',
        '%': r'\%',
        '$': r'\$',
        '#': r'\#',
        '_': r'\_',
        '{': r'\{',
        '}': r'\}',
        '~': r'\textasciitilde{}',
        '^': r'\^{}',
        '\\': r'\textbackslash{}',
        '<': r'\textless{}',
        '>': r'\textgreater{}',
    }
    regex = re.compile('|'.join(re.escape(str(key)) for key in sorted(conv.keys(), key = lambda item: - len(item))))
    return regex.sub(lambda match: conv[match.group()], text)


def joinDicts(d1, d2):
    result = d1.copy()
    for k, v in d2.items():
        if (k in d1 and isinstance(d1[k], dict)):
            result[k] = joinDicts(d1[k], d2[k])
        else:
            result[k] = d2[k]
    return result

def anonymize_dbms(dbms_names):
    if type(dbms_names) == list:
        return anonymize_list(dbms_names)
    elif type(dbms_names) == dict:
        return anonymize_dict(dbms_names)
    elif type(dbms_names) == pd.core.frame.DataFrame:
         anonymize_dataframe(dbms_names)
    elif type(dbms_names) == str:
        return dbms.anonymizer[dbms_names]
 
def anonymize_list(list_dbms):
    list_mapped = [dbms.anonymizer[l] if l in dbms.anonymizer else l for l in list_dbms]
    return list_mapped

def anonymize_dict(dict_dbms):
    list_mapped = {dbms.anonymizer[k]:v for k,v in dict_dbms.items()}
    return list_mapped

def anonymize_dataframe(dataframe_dbms):
    if not dataframe_dbms.empty:
        if dataframe_dbms.index[0] in dbms.anonymizer:
            dataframe_dbms.index = dataframe_dbms.index.map(dbms.anonymizer)
            #dataframe_dbms.index = anonymize_list(dataframe_dbms.index)
        elif dataframe_dbms.columns[0] in dbms.anonymizer:
            dataframe_dbms.columns = dataframe_dbms.columns.map(dbms.anonymizer)
            #dataframe_dbms.columns = anonymize_list(dataframe_dbms.columns)

def natural_sort(l): 
    convert = lambda text: int(text) if text.isdigit() else text.lower()
    alphanum_key = lambda key: [convert(c) for c in re.split('([0-9]+)', key)]
    return sorted(l, key=alphanum_key)


import json
from os.path import isdir, isfile, join
from os import listdir, stat
import pandas as pd
from shutil import copyfile
from operator import itemgetter

#result_path = '/results/'
#code = '1613110870'

def merge_partial_results(result_path, code):
    logger = logging.getLogger('dbmsbenchmarker')
    # result folder
    folder = result_path+code
    # connection subfolders 
    list_connections = [f for f in listdir(folder) if isdir(join(folder, f))]
    def joinDicts(d1, d2):
        result = d1.copy()
        for k, v in d2.items():
            if (k in d1 and isinstance(d1[k], dict)):
                result[k] = joinDicts(d1[k], d2[k])
            else:
                result[k] = d2[k]
        return result
    # merging connection configs
    # list of content of connection config files
    connections = []
    for connection in list_connections:
        filename = '{folder}/{connection}/connections.config'.format(folder=folder, connection=connection)
        #print(filename)
        try:
            if isfile(filename):
                with open(filename,'r') as inf:
                    content=ast.literal_eval(inf.read())
                    #print(content)
                    connections.append(content)
            filename = '{folder}/{connection}/queries.config'.format(folder=folder, connection=connection)
            copyfile(filename, folder+'/queries.config')
        except Exception as e:
            print("Exception when merging connections: {}".format(e))
    # join to single list
    # no connection name must be doubled
    connection_config = []
    connection_names = []
    for connection in connections:
        #print(len(connection))
        for c in connection:
            #print(c['name'])
            if not c['name'] in connection_names:
                connection_config.append(c)
                connection_names.append(c['name'])
    if not benchmarker.BENCHMARKER_VERBOSE_NONE:
        for connection in connection_config:
            print("Merged connection: ", connection['name'])
    # store merged config
    filename = folder+'/connections.config'
    with open(filename,'w') as inf:
        inf.write(str(connection_config))
    # merging protocols
    if not benchmarker.BENCHMARKER_VERBOSE_NONE:
        print("Merge protocols")
    # load partial protocols
    protocols = []
    for connection in list_connections:
        filename = '{folder}/{connection}/protocol.json'.format(folder=folder, connection=connection)
        with open(filename, 'r') as f:
            protocols.append(json.load(f))
    # merged protocol
    protocol = {}
    protocol['query'] = {}
    protocol['connection'] = {}
    protocol['total'] = {}
    protocol['ordering'] = {}
    for k,v in protocols[0]['query'].items():
        if isinstance(v, dict):
            protocol['query'][k] = {}
            for p in protocols:
                protocol['query'][k] = joinDicts(protocol['query'][k], p['query'][k])
    for p in protocols:
        protocol['total'] = joinDicts(protocol['total'], p['total'])
    for p in protocols:
        if 'ordering' in p:
            protocol['ordering'] = joinDicts(protocol['ordering'], p['ordering'])
    filename_protocol = '{folder}/protocol.json'.format(folder=folder)
    with open(filename_protocol, 'w') as f:
        json.dump(protocol, f)
    # compare result sets
    if not benchmarker.BENCHMARKER_VERBOSE_NONE:
        print("Merge result sets")
    for numQuery, query in protocol['query'].items():
        #if int(numQuery) > 3:
        #    exit()
        #print(query)
        data_first = None
        df_first = None
        connection_first = None
        titles_result = []
        for connection in list_connections:
            try:
                filename = '{folder}/{connection}/query_{numQuery}_resultset_complete_{connection}.pickle'.format(folder=folder, connection=connection, numQuery=numQuery)
                logger.debug("Looking for {}".format(filename))
                if isfile(filename):
                    # result set of all runs
                    #print(connection+": ", end='')#, df)
                    with open(filename, 'r') as f:
                        data = pickle.load( open( filename, "rb" ) )
                        if data_first is None:
                            protocol['query'][numQuery]['dataStorage'] = data.copy()
                            protocol['query'][numQuery]['warnings'][connection] = ''
                            if len(protocol['query'][numQuery]['dataStorage'][0]) > 0:
                                titles_result = protocol['query'][numQuery]['dataStorage'][0][0]
                            #print(protocol['query'][numQuery]['dataStorage'][0])
                            data_first = data.copy()
                            #print(data_first)
                            connection_first = connection
                        else:
                            different = False
                            for numRun, resultset in enumerate(data):
                                #print("numRun {}".format(numRun), end='')
                                result = data[numRun].copy()
                                # remove titles
                                if len(data[numRun]) > 0:
                                    titles_result = data[numRun][0]#list(range(len(result[0])))
                                else:
                                    continue
                                #print(titles_result)
                                #print("####", data[numRun])
                                result.pop(0)
                                #print("****1", result)
                                # convert datatypes
                                #precision = query.restrict_precision
                                precision = 2
                                #result = [[round(float(item), int(precision)) if convertToFloat(item) == float else convertToInt(item) if convertToInt(item) == item else item for item in sublist] for sublist in result]
                                result = [[convert_to_rounded_float(item, int(precision)) for item in sublist] for sublist in result]
                                #print("****2", result)
                                if len(result) > 0:
                                    df = pd.DataFrame(sorted(result, key=itemgetter(*list(range(0,len(result[0]))))), columns=titles_result)
                                else:
                                    df = pd.DataFrame([], columns=titles_result)
                                #df = pd.DataFrame(result)
                                #new_header = df.iloc[0] #grab the first row for the header
                                #df = df[1:] #take the data less the header row
                                #df.columns = new_header #set the header row as the df header
                                #print(df)
                                #df.reset_index(inplace=True, drop=True)
                                #print(df)
                                storage = data_first[numRun].copy()
                                # remove titles
                                titles_storage = data_first[numRun][0]#list(range(len(storage[0])))
                                #print(titles_storage)
                                storage.pop(0)
                                # convert datatypes
                                #precision = query.restrict_precision
                                precision = 2
                                #storage = [[round(float(item), int(precision)) if convertToFloat(item) == float else convertToInt(item) if convertToInt(item) == item else item for item in sublist] for sublist in storage]
                                storage = [[convert_to_rounded_float(item, int(precision)) for item in sublist] for sublist in storage]
                                if len(storage) > 0:
                                    df_first = pd.DataFrame(sorted(storage, key=itemgetter(*list(range(0,len(storage[0]))))), columns=titles_storage)
                                else:
                                    df_first = pd.DataFrame([], columns=titles_storage)
                                #if df.empty and df_first.empty:
                                #    logger.debug("all empty")
                                #    continue
                                #df_first = pd.DataFrame(data_first[numRun])
                                #new_header = df_first.iloc[0] #grab the first row for the header
                                #df_first = df_first[1:] #take the data less the header row
                                #df_first.columns = new_header #set the header row as the df header
                                #df_first.reset_index(inplace=True, drop=True)
                                #print(numQuery, connection, df_first, df)
                                # Check for duplicate column names
                                duplicates = df.columns[df.columns.duplicated()]
                                if not duplicates.empty:
                                    logger.debug("Duplicate column names found in Q{} (cannot compare results uniquely): {}".format(numQuery, duplicates.tolist()))
                                    #print("Duplicate column names found in Q{} (cannot compare results uniquely): {}".format(numQuery, duplicates.tolist()))
                                    protocol['query'][numQuery]['warnings'][connection] = 'Not sortable at run #'+str(numRun+1)
                                    protocol['query'][numQuery]['resultSets'][connection] = data
                                    protocol['query'][numQuery]['resultSets'][connection_first] = data_first
                                    different = True
                                    break
                                else:
                                    duplicates = df_first.columns[df_first.columns.duplicated()]
                                    if not duplicates.empty:
                                        logger.debug("Duplicate column names found in Q{} (cannot compare results uniquely): {}".format(numQuery, duplicates.tolist()))
                                        #print("Duplicate column names found in Q{} (cannot compare results uniquely): {}".format(numQuery, duplicates.tolist()))
                                        protocol['query'][numQuery]['warnings'][connection] = 'Not sortable at run #'+str(numRun+1)
                                        protocol['query'][numQuery]['resultSets'][connection] = data
                                        protocol['query'][numQuery]['resultSets'][connection_first] = data_first
                                        different = True
                                        break
                                df_1 = inspector.getDifference12(df_first, df)
                                df_2 = inspector.getDifference12(df, df_first)
                                #print("result", result)
                                #print("storage", storage)
                                if result == storage:
                                    logger.debug("same")
                                    pass
                                #    #exit()
                                #if numQuery=='3':
                                #    print(df_first)
                                #    print(df)
                                if not df_1.empty or not df_2.empty:
                                    logger.debug("different")#, df_1, df_2)
                                    #print("result", result)
                                    #print("storage", storage)
                                    #exit()
                                    protocol['query'][numQuery]['warnings'][connection] = 'Different at run #'+str(numRun+1)
                                    #result_as_list = [[i for i in list(df.columns)]]
                                    #result_as_list.extend(df.values.tolist())
                                    #print(result_as_list)
                                    #exit()
                                    protocol['query'][numQuery]['resultSets'][connection] = data
                                    protocol['query'][numQuery]['resultSets'][connection_first] = data_first
                                    different = True
                                    break
                            if not different:
                                #print("OK")
                                protocol['query'][numQuery]['resultSets'][connection] = []
                                protocol['query'][numQuery]['warnings'][connection] = ""
                else:
                    # result set of first run only
                    filename = '{folder}/{connection}/query_{numQuery}_resultset_{connection}.pickle'.format(folder=folder, connection=connection, numQuery=numQuery)
                    #print(connection+": ", end='')#, df)
                    if isfile(filename):
                        with open(filename, 'r') as f:
                            df = pd.read_pickle(filename)
                            #print(connection)#, df)
                            if df_first is None:
                                df_first = df.copy()
                                #print("first\n", df_first)
                                result_as_list = [[i[0] for i in list(df_first.columns)]]
                                result_as_list.extend(df_first.values.tolist())
                                protocol['query'][numQuery]['dataStorage'] = [result_as_list] # list, because this is (only) first run
                                protocol['query'][numQuery]['warnings'][connection] = ""
                            else:
                                df_1 = inspector.getDifference12(df_first, df)
                                df_2 = inspector.getDifference12(df, df_first)
                                if not df_1.empty or not df_2.empty:
                                    #print("different\n", df)
                                    protocol['query'][numQuery]['warnings'][connection] = 'Different'
                                    result_as_list = [[i[0] for i in list(df.columns)]]
                                    result_as_list.extend(df.values.tolist())
                                    protocol['query'][numQuery]['resultSets'][connection] = [result_as_list] # list, because this is (only) first run
                                else:
                                    #print("OK")
                                    protocol['query'][numQuery]['resultSets'][connection] = []
                                    protocol['query'][numQuery]['warnings'][connection] = ""
            except Exception as e:
                print("Exception when merging result sets: {}".format(e))
                #print("missing")
                protocol['query'][numQuery]['warnings'][connection] = 'Missing'
                traceback.print_exc()
            finally:
                pass
    #print("warnings", protocol['query']['3']['warnings'])
    #print("storage", protocol['query']['3']['dataStorage'])
    #print("result", protocol['query']['3']['resultSets']['MySQL'])
    #print("result", protocol['query']['3']['resultSets']['MonetDBNew'])
    with open(filename_protocol, 'w') as f:
        json.dump(protocol, f)
    # merge timers
    if not benchmarker.BENCHMARKER_VERBOSE_NONE:
        print("Merge timers")
    # load partial timers, join and save
    timer = ['connection', 'execution', 'datatransfer']
    numQuery = 1
    for numQuery, query in protocols[0]['query'].items():
        for t in timer:
            connection = list_connections[0]
            d = {}
            for connection in list_connections:
                # load execution benchmarks
                filename = '{folder}/{connection}/query_{numQuery}_{timer}.csv'.format(folder=folder, connection=connection, numQuery=numQuery, timer=t)
                if isfile(filename):
                    df1 = pd.read_csv(filename)
                    df1_t = df1.transpose()
                    d1 = df1.to_dict(orient="list")
                else:
                    continue
                d = joinDicts(d,d1)
            if len(d) > 0:
                df = pd.DataFrame(d)
                # convert to csv
                csv = df.to_csv(index_label=False,index=False,lineterminator='\n')
                # save
                filename = '{folder}/query_{numQuery}_{timer}.csv'.format(folder=folder, numQuery=numQuery, timer=t)
                csv_file = open(filename, "w")
                csv_file.write(csv)
                csv_file.close()
                logger.debug("Merged timer {}".format(filename))
    # merge metrics
    # copy partial metrics
    for connection in list_connections:
        folder_connection = folder+'/'+connection
        files_metrics = [f for f in listdir(folder_connection) if isfile(join(folder_connection, f)) and 'metric' in f]
        #print(folder_connection, files_metrics)
        if not benchmarker.BENCHMARKER_VERBOSE_NONE:
            print("Copy Metrics", folder_connection)
        for file in files_metrics:
            copyfile(folder_connection+'/'+file, folder+'/'+file)
