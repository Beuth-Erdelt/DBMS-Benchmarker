"""
    Command line interface for the Python Package DBMS Benchmarker
    This fetches some metrics.
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
#from bexhoma import *
from dbmsbenchmarker import *
#import experiments
import logging
import urllib3
import logging
import argparse
import time


urllib3.disable_warnings()

if __name__ == '__main__':
    # argparse
    parser = argparse.ArgumentParser(description='A benchmark tool for RDBMS. Fetches loading or stream metrics.')
    parser.add_argument('-r', '--result-folder', help='folder for storing benchmark result folders', default=None)
    parser.add_argument('-e', '--experiment-code', help='folder for storing benchmark result files, default is given by timestamp', default=None)
    parser.add_argument('-f', '--config-folder', help='folder containing query and connection config files. If set, the names connections.config and queries.config are assumed automatically.', default=None)
    parser.add_argument('-c', '--connection', help='Name of the connection (dbms) to use', default=None)
    parser.add_argument('-mt', '--metrics-type', help='Type of metrics definitions (metrics, metrics_special, metrics_custom)', default='')
    #parser.add_argument('-co', '--component', help='Component name', default='sut')
    parser.add_argument('-ct', '--component-type', help='Type of the component (loading or stream)', default='loading')
    parser.add_argument('-cn', '--container-name', help='Name of the container (if not dbms for sut/workers: datagenerator, sensor, dbmsbenchmarker)', default=None)
    parser.add_argument('-cf', '--connection-file', help='name of connection config file', default='connections.config')
    parser.add_argument('-ts', '--time-start', help='Time loading has started', default=None)
    parser.add_argument('-te', '--time-end', help='Time loading has ended', default=None)
    parser.add_argument('-db', '--debug', help='dump debug informations', action='store_true')
    args = parser.parse_args()
    # evaluate args
    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.ERROR)
    result_path = args.result_folder#'/results'
    code = args.experiment_code#'1616083097'
    connection = args.connection
    query = args.component_type
    metrics_type = args.metrics_type
    #component = args.component
    container_name = args.container_name
    time_start = int(args.time_start)#1616083225
    time_end = int(args.time_end)#1616083310
    print("Interval length {}s".format(time_end-time_start))
    #url = 'http://bexhoma-monitoring-omnisci-{code}.perdelt.svc.cluster.local:9090/api/v1/'.format(code=code)
    #metric = {'query': 'container_memory_working_set_bytes{job="monitor-node", container_label_io_kubernetes_container_name="dbms"}/1024/1024', 'title': 'CPU Memory [MiB]'}
    #monitor.metrics.getMetrics(url, metric, time_start, time_end, 1)
    experiments = benchmarker.benchmarker(
            result_path='{result_path}/{code}/'.format(result_path=result_path, code=code))#args.result_folder,
            #working=args.working,
            #batch=bBatch,
            #subfolder=subfolder,#args.subfolder,
            #fixedQuery=args.query,
            #fixedConnection=args.connection,
            #rename_connection=rename_connection,
            #anonymize=args.anonymize,
            #unanonymize=args.unanonymize,
            #numProcesses=args.numProcesses,
            #seed=args.seed)
    #experiments.getConfig()
    experiments.getConfig(args.config_folder, connectionfile=args.connection_file)
    for connection_name, connection_data in experiments.dbms.items():
        if connection is not None:
            if connection_name != connection:
                print("Found {}, but we only query for {}".format(connection_name, connection))
                continue
        print("URL", connection_data.connectiondata['monitoring']['prometheus_url'])
        #query='loading'
        #query='stream'
        # metrics_special means there arwe special promql queries for special dbms, e.g., systems not managed by bexhoma
        #if 'metrics_special' in connection_data.connectiondata['monitoring'] and (container_name is None or container_name == 'dbms') and (query == 'loading' or query == 'stream'):
        #if metrics_type == 'metrics_custom' and 'metrics_custom' in connection_data.connectiondata['monitoring'] and component in connection_data.connectiondata['monitoring']['metrics_custom']:
        if len(metrics_type) > 0 and metrics_type in connection_data.connectiondata['monitoring']:
            print("Use custom metrics: metrics type = {}".format(metrics_type))
            for m, metric in connection_data.connectiondata['monitoring'][metrics_type].items():
                print("Metric", m)
                path = '{result_path}/{code}/'.format(result_path=result_path, code=code)
                monitor.metrics.fetchMetric(query, m, connection_name, connection_data.connectiondata, time_start, time_end, path, container=container_name, metrics_query_path=metrics_type)
        # do not use replacement anymore (SUT deployment replaced by SUT worker)
        #elif 'metrics_special' in connection_data.connectiondata['monitoring'] and (query == 'loading' or query == 'stream'):
        #    print("Use special metrics for SUT not managed by bexhoma: component type = {}".format(query))
        #    for m, metric in connection_data.connectiondata['monitoring']['metrics_special'].items():
        #        print("Metric", m)
        #        path = '{result_path}/{code}/'.format(result_path=result_path, code=code)
        #        monitor.metrics.fetchMetric(query, m, connection_name, connection_data.connectiondata, time_start, time_end, path, container=container_name, metrics_query_path='metrics_special')
        else:
            print("Use default metrics for components managed by bexhoma: component type = {}".format(query))
            for m, metric in connection_data.connectiondata['monitoring']['metrics'].items():
                print("Metric", m)
                path = '{result_path}/{code}/'.format(result_path=result_path, code=code)
                monitor.metrics.fetchMetric(query, m, connection_name, connection_data.connectiondata, time_start, time_end, path, container=container_name, metrics_query_path='metrics')
                #metrics = monitor.metrics(experiments)
                #df = metrics.dfHardwareMetricsLoading(m)
                #print(df)

