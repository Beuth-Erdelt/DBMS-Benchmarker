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
from dbmsbenchmarker import tools, monitor, evaluator, benchmarker
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
    def readProtocol(self, silent=False):
        """
        Loads procol of benchmarker in JSON format.

        :param silent: No output of status
        :return: returns nothing
        """
        try:
            filename = self.benchmarker.path+'/protocol.json'
            with open(filename, 'r') as f:
                self.benchmarker.protocol = json.load(f)
        except Exception as e:
            logging.debug("No protocol found")
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
        if not benchmarker.BENCHMARKER_VERBOSE_NONE:
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
        #    query = tools.query(self.benchmarker.queries[int(q)-1])
        #    # is query active?
        #    if not query.active:
        #        continue
        #    self.generate(q, [])
        # per stream
        if self.per_stream:
            number_of_queries = len(self.benchmarker.protocol['query'].items())
            for c, connection in self.benchmarker.dbms.items():
                if connection.hasHardwareMetrics():
                    logging.info("Hardware metrics for stream of connection {}".format(c))
                    # find lowest start time
                    times_start = dict()
                    times_end = dict()
                    for i in range(number_of_queries, 0, -1):
                        times = self.benchmarker.protocol['query'][str(i)]
                        if "ends" in times and c in times["ends"]:
                            time_start = int(datetime.timestamp(datetime.strptime(times["starts"][c],'%Y-%m-%d %H:%M:%S.%f')))
                            time_end = int(datetime.timestamp(datetime.strptime(times["ends"][c],'%Y-%m-%d %H:%M:%S.%f')))
                            times_start[time_start] = i
                            times_end[time_end] = i
                            #logging.debug("Last active query is {}".format(i))
                            #break
                    logging.debug("Start times: {}".format(times_start))
                    if len(times_start) == 0:
                        logging.info("No successful query")
                        return
                    time_start = min(times_start.keys())
                    print("First query Q{} at {}".format(times_start[time_start], time_start))
                    logging.debug("End times: {}".format(times_end))
                    time_end = max(times_end.keys())
                    print("Last query Q{} at {}".format(times_end[time_end], time_end))
                    # find highest end time
                    #times = self.benchmarker.protocol['query'][str(1)]
                    #time_start = int(datetime.timestamp(datetime.strptime(times["starts"][c],'%Y-%m-%d %H:%M:%S.%f')))
                    #time_end = time_start
                    ## find the last active query (with end time)
                    #for i in range(number_of_queries, 0, -1):
                    #    times = self.benchmarker.protocol['query'][str(i)]
                    #    if "ends" in times and c in times["ends"]:
                    #        time_end = int(datetime.timestamp(datetime.strptime(times["ends"][c],'%Y-%m-%d %H:%M:%S.%f')))
                    #        logging.debug("Last active query is {}".format(i))
                    #        break
                    #logging.debug(connection.connectiondata['monitoring']['prometheus_url'])
                    query='stream'
                    if 'metrics' in connection.connectiondata['monitoring']:
                        metrics_dict = connection.connectiondata['monitoring']['metrics']
                    else:
                        metrics_dict = monitor.metrics.metrics
                    for m, metric in metrics_dict.items():
                        logging.debug("Metric {}".format(m))
                        monitor.metrics.fetchMetric(query, m, c, connection.connectiondata, time_start, time_end, '{result_path}/'.format(result_path=self.benchmarker.path))




