"""
    Command line interface for the Python Package DBMS Benchmarker
    This tranforms csv files of metrics per connection to a csv file per metric.
    It so collects metrics from several dbms connections into a single file (per metric).
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
import pandas as pd
import os
import re
import matplotlib.pyplot as plt
pd.set_option("display.max_rows", None)
pd.set_option('display.max_colwidth', None)
# Some nice output
from IPython.display import display, Markdown
import pickle
import json
import traceback
import ast

from dbmsbenchmarker import monitor

import logging
import urllib3
import argparse
import time


urllib3.disable_warnings()


class evaluator:
    """
    Basis class for evaluating an experiment.
    Constructor sets

      1. `path`: path to result folders
      1. `code`: Id of the experiment (name of result folder)
    """
    def __init__(self, code, path):
        """
        Initializes object by setting code and path to result folder.

        :param path: path to result folders
        :param code: Id of the experiment (name of result folder)
        """
        self.path = path+"/"+code
        self.code = code
    def transform_monitoring_results(self, component="loading"):
        """
        Transforms csv files (per connection and per metric) to a single csv
        file (per metric), one column per SUT.

        A connection's identity is its `orig_name` when it has one, its own
        `name` otherwise - unchanged from before. What's new: once an
        identity has been merged in, any other connection resolving to the
        same identity is skipped, instead of being merged in again. Without
        that guard, several connections sharing one `orig_name` (how
        DBMSBenchmarker represents parallel loaders/clients against a single
        physical SUT) each load and merge the *same* file under the *same*
        column name, and pandas' merge silently renames the second one to
        `_x`/`_y` instead of erroring, corrupting the combined metric. If the
        job-level file named after the identity doesn't exist (some
        components, e.g. `loading`, only ever write that one; others only
        write per-pod files), a connection's own per-pod file is tried as a
        fallback and stands in for the whole identity.

        Loading any pre-existing combined file first keeps repeated
        invocations of this script for the same component idempotent instead
        of compounding.

        :param component: Metrics of component loading or benchmark
        """
        connections_sorted = self.get_connection_config()
        list_metrics = self.get_monitoring_metrics()
        #print(c['name'], list_metrics)
        for m in list_metrics:
            out_filename = '/query_{component}_metric_{metric}.csv'.format(component=component, metric=m)
            df_all = monitor.metrics.loadMetricsDataframe(self.path+out_filename)
            processed_names = set()
            for connection in connections_sorted:
                if 'orig_name' in connection:
                    connectionname = connection['orig_name']
                else:
                    connectionname = connection['name']
                if connectionname in processed_names:
                    continue
                filename = "query_{component}_metric_{metric}_{connection}.csv".format(component=component, metric=m, connection=connectionname)
                #print(self.path++"/"+filename)
                df = monitor.metrics.loadMetricsDataframe(self.path+"/"+filename)
                if df is None and connection['name'] != connectionname:
                    # job-level file doesn't exist; fall back to this connection's own per-pod file
                    filename = "query_{component}_metric_{metric}_{connection}.csv".format(component=component, metric=m, connection=connection['name'])
                    df = monitor.metrics.loadMetricsDataframe(self.path+"/"+filename)
                if df is None:
                    continue
                #print(df)
                processed_names.add(connectionname)
                df.columns=[connectionname]
                if df_all is None:
                    df_all = df
                else:
                    if connectionname in df_all.columns:
                        df_all = df_all.drop(columns=[connectionname])
                    df_all = df_all.merge(df, how='outer', left_index=True,right_index=True)
            #print(df_all)
            #print(self.path+filename)
            print("Generated", self.path+out_filename)
            monitor.metrics.saveMetricsDataframe(self.path+out_filename, df_all)
    def get_monitoring_metric(self, metric, component="loading"):
        """
        Returns list of names of metrics using during monitoring.

        :return: List of monitoring metrics
        """
        filename = '/query_{component}_metric_{metric}.csv'.format(component=component, metric=metric)
        return pd.read_csv(self.path+"/"+filename)
    def get_monitoring_metrics(self):
        """
        Returns list of names of metrics using during monitoring.

        :return: List of monitoring metrics
        """
        connections_sorted = self.get_connection_config()
        for c in connections_sorted:
            list_metrics = list(c['monitoring']['metrics'].keys())
            break
        return list_metrics
    def get_connection_config(self):
        """
        Returns connection.config as Python dict.
        Items are sorted by connection name.

        :return: Python dict of all connection informations
        """
        with open(self.path+"/connections.config",'r') as inf:
            connections = ast.literal_eval(inf.read())
        connections_sorted = sorted(connections, key=lambda c: c['name']) 
        return connections_sorted
    def print_monitoring_results(self, component="loading"):
        """
        Prints the collected metric files as DataFrames.

        :param component: Metrics of component loading or benchmark
        """
        list_metrics = self.get_monitoring_metrics()
        for m in list_metrics:
            df = self.get_monitoring_metric(m, component)
            print(df.T)




if __name__ == '__main__':
    # argparse
    parser = argparse.ArgumentParser(description='A benchmark tool for RDBMS. Transforms loading or benchmarking metrics.')
    parser.add_argument('-r', '--result-folder', help='folder for storing benchmark result folders', default=None)
    parser.add_argument('-e', '--experiment-code', help='folder for storing benchmark result files, default is given by timestamp', default=None)
    #parser.add_argument('-c', '--connection', help='Name of the connection (dbms) to use', default=None)
    parser.add_argument('-ct', '--component-type', help='Type of the component (loading or benchmarking)', default='loading')
    parser.add_argument('-cf', '--connection-file', help='name of connection config file', default='connections.config')
    parser.add_argument('-db', '--debug', help='dump debug informations', action='store_true')
    args = parser.parse_args()
    # evaluate args
    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.ERROR)
    result_path = args.result_folder#'/results'
    code = args.experiment_code#'1616083097'
    #connection = args.connection
    query = args.component_type
    evaluation = evaluator(code=code, path=result_path)
    evaluation.transform_monitoring_results(component=query)
    if args.debug:
        evaluation.print_monitoring_results(component=query)
