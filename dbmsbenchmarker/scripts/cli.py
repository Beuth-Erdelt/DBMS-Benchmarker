"""
    Command line interface for the Python Package DBMS Benchmarker
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
import logging
import argparse
import time
from os import makedirs, path
import random
from datetime import datetime, timedelta
import shutil

from dbmsbenchmarker import *


def run_benchmarker():
    # argparse
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
    parser.add_argument('-pp', '--parallel-processes', help='if parallel execution should be organized as independent processes', action='store_true')
    parser.add_argument('-s', '--seed', help='random seed', default=None)
    parser.add_argument('-cs', '--copy-subfolder', help='copy subfolder of result folder', action='store_true')
    parser.add_argument('-ms', '--max-subfolders', help='maximum number of subfolders of result folder', default=None)
    parser.add_argument('-sl', '--sleep', help='sleep SLEEP seconds before going to work', default=0)
    parser.add_argument('-st', '--start-time', help='sleep until START-TIME before beginning benchmarking', default=None)
    parser.add_argument('-sf', '--subfolder', help='stores results in a SUBFOLDER of the result folder', default=None)
    parser.add_argument('-sd', '--store-data', help='store result of first execution of each query', default=None, choices=[None, 'csv', 'pandas'])
    parser.add_argument('-dd', '--discard-data', help='result sets of all queries is discarded (not fetched at all)', action='store_true', default=False)
    parser.add_argument('-vq', '--verbose-queries', help='print every query that is sent', action='store_true', default=False)
    parser.add_argument('-vs', '--verbose-statistics', help='print statistics about queries that have been sent', action='store_true', default=False)
    parser.add_argument('-vr', '--verbose-results', help='print result sets of every query that has been sent', action='store_true', default=False)
    parser.add_argument('-vp', '--verbose-process', help='print infos about the workflow steps', action='store_true', default=False)
    parser.add_argument('-vn', '--verbose-none', help='stay completely silent', action='store_true', default=False)
    parser.add_argument('-pn', '--num-run', help='Parameter: Number of executions per query', default=0)
    parser.add_argument('-m', '--metrics', help='collect hardware metrics per query', action='store_true', default=False)
    parser.add_argument('-mps', '--metrics-per-stream', help='collect hardware metrics per stream', action='store_true', default=False)
    parser.add_argument('-sid', '--stream-id', help='id of a stream in parallel execution of streams', default=None)
    parser.add_argument('-ssh', '--stream-shuffle', help='shuffle query execution based on id of stream', default=None)
    parser.add_argument('-wli', '--workload-intro', help='meta data: intro text for workload description', default='')
    parser.add_argument('-wln', '--workload-name', help='meta data: name of workload', default='')
    #parser.add_argument('-pt', '--timeout', help='Parameter: Timeout in seconds', default=0)
    #logger = logging.getLogger('dbmsbenchmarker')
    args = parser.parse_args()
    command_args = vars(args)
    experiments = benchmarker.run_cli(command_args)
    #if args.generate_evaluation == 'yes':
    #    benchmarker.run_evaluation(experiments)
    #print(args)
    exit()
    """
    # evaluate args
    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
        bBatch = True
    else:
        logging.basicConfig(level=logging.INFO)
        bBatch = args.batch
    # sleep before going to work
    if int(args.sleep) > 0:
        logger.debug("Sleeping {} seconds before going to work".format(int(args.sleep)))
        time.sleep(int(args.sleep))
    # make a copy of result folder
    if not args.result_folder is None and not path.isdir(args.result_folder):
        makedirs(args.result_folder)
        shutil.copyfile(args.config_folder+'/connections.config', args.result_folder+'/connections.config')#args.connection_file)
        shutil.copyfile(args.config_folder+'/queries.config', args.result_folder+'/queries.config')#args.query_file)
    subfolder = args.subfolder
    rename_connection = ''
    rename_alias = ''
    if args.copy_subfolder and len(subfolder) > 0:
        if args.stream_id is not None:
            client = int(args.stream_id)
        else:
            client = 1
        while True:
            if args.max_subfolders is not None and client > int(args.max_subfolders):
                exit()
            resultpath = args.result_folder+'/'+subfolder+'-'+str(client)
            print("Checking if {} is suitable folder for free job number".format(resultpath))
            if path.isdir(resultpath):
                client = client + 1
                waiting = random.randint(1, 10)
                print("Sleeping {} seconds before checking for next free job number".format(waiting))
                time.sleep(waiting)
            else:
                print("{} is a suitable folder for free job number".format(resultpath))
                makedirs(resultpath)
                break
        subfolder = subfolder+'-'+str(client)
        rename_connection = args.connection+'-'+str(client)
        print("Rename connection {} to {}".format(args.connection, rename_connection))
        rename_alias = args.connection_alias+'-'+str(client)
        print("Rename alias {} to {}".format(args.connection_alias, rename_alias))
    # sleep before going to work
    if args.start_time is not None:
        #logger.debug(args.start_time)
        now = datetime.utcnow()
        try:
            start = datetime.strptime(args.start_time, '%Y-%m-%d %H:%M:%S')
            if start > now:
                wait = (start-now).seconds
                now_string = now.strftime('%Y-%m-%d %H:%M:%S')
                logger.debug("Sleeping until {} before going to work ({} seconds, it is {} now)".format(args.start_time, wait, now_string))
                time.sleep(int(wait))
        except Exception as e:
            logger.debug("Invalid format: {}".format(args.start_time))
    # set verbose level
    if args.verbose_queries:
        benchmarker.BENCHMARKER_VERBOSE_QUERIES = True
    if args.verbose_statistics:
        benchmarker.BENCHMARKER_VERBOSE_STATISTICS = True
    if args.verbose_results:
        benchmarker.BENCHMARKER_VERBOSE_RESULTS = True
    if args.verbose_process:
        benchmarker.BENCHMARKER_VERBOSE_PROCESS = True
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
    # dbmsbenchmarker with reporter
    experiments = benchmarker.benchmarker(
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
        stream_id=stream_id,
        stream_shuffle=stream_shuffle,
        seed=args.seed)
    # overwrite parameters of workload header
    if len(args.workload_intro):
        experiments.workload['intro'] = args.workload_intro
    if len(args.workload_name):
        experiments.workload['name'] = args.workload_name
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
        print('Experiment {} has been finished'.format(experiments.code))
    elif args.mode == 'continue':
        if experiments.continuing:
            experiments.continueBenchmarks(overwrite = False)
        else:
            print("Continue needs result folder")
    if args.metrics:
        # collect hardware metrics
        experiments.reporter.append(benchmarker.reporter.metricer(experiments))
        experiments.generateReportsAll()
    if args.metrics_per_stream:
        # collect hardware metrics
        experiments.reporter.append(benchmarker.reporter.metricer(experiments, per_stream=True))
        experiments.generateReportsAll()
    if args.generate_evaluation == 'yes':
        # generate evaluation cube
        experiments.overwrite = True
        evaluator.evaluator(experiments, load=False, force=True)
        # show some evaluations
        evaluator.evaluator(experiments, load=False, force=True)
        result_folder = experiments.path #args.result_folder if not args.result_folder is None else "./"
        #num_processes = min(float(args.numProcesses if not args.numProcesses is None else 1), float(args.num_run) if int(args.num_run) > 0 else 1)
        evaluate = inspector.inspector(result_folder)
        evaluate.load_experiment("")#experiments.code)
        list_queries_all = evaluate.get_experiment_list_queries()
        #print(list_queries_all)
        dbms_filter = []
        if not args.connection is None:
            dbms_filter = [args.connection]
        for q in list_queries_all:
            df = evaluate.get_timer(q, "execution")
            if len(list(df.index)) > 0:
                dbms_filter = list(df.index)
                print("First successful query: {}".format(q))
                break
        #print(dbms_filter)
        #list_queries = evaluate.get_experiment_queries_successful() # evaluate.get_experiment_list_queries()
        list_queries = evaluate.get_survey_successful(timername='execution', dbms_filter=dbms_filter)
        #print(list_queries, len(list_queries))
        if 'numRun' in experiments.connectionmanagement:
            num_run = experiments.connectionmanagement['numRun']
        else:
            num_run = 1
        if 'numProcesses' in experiments.connectionmanagement:
            num_processes = experiments.connectionmanagement['numProcesses']
        else:
            num_processes = 1
        #####################
        if len(dbms_filter) > 0:
            print("Limited to:", dbms_filter)
        print("Number of runs per query:", num_run)
        print("Number of successful queries:", len(list_queries))
        print("Number of max. parallel clients:", int(num_processes))
        #####################
        print("\n### Errors (failed queries)")
        print(evaluate.get_total_errors(dbms_filter=dbms_filter).T)
        #####################
        print("\n### Warnings (result mismatch)")
        print(evaluate.get_total_warnings(dbms_filter=dbms_filter).T)
        #####################
        #df = evaluate.get_aggregated_query_statistics(type='timer', name='connection', query_aggregate='Median', dbms_filter=dbms_filter)
        df = evaluate.get_aggregated_experiment_statistics(type='timer', name='connection', query_aggregate='Median', total_aggregate='Geo', dbms_filter=dbms_filter)
        df = (df/1000.0).sort_index()
        if not df.empty:
            print("### Geometric Mean of Medians of Connection Times (only successful) [s]")
            df.columns = ['average connection time [s]']
            print(df.round(2))
            #print("### Statistics of Timer Connection (only successful) [s]")
            #df_stat = evaluator.addStatistics(df, drop_nan=False, drop_measures=True)
            #print(df_stat.round(2))
        #####################
        #df = evaluate.get_aggregated_query_statistics(type='timer', name='connection', query_aggregate='Median', dbms_filter=dbms_filter)
        df = evaluate.get_aggregated_experiment_statistics(type='timer', name='connection', query_aggregate='Max', total_aggregate='Max', dbms_filter=dbms_filter)
        df = (df/1000.0).sort_index()
        if not df.empty:
            print("### Max of Connection Times (only successful) [s]")
            df.columns = ['max connection time [s]']
            print(df.round(2))
            #print("### Statistics of Timer Connection (only successful) [s]")
            #df_stat = evaluator.addStatistics(df, drop_nan=False, drop_measures=True)
            #print(df_stat.round(2))
        #####################
        df = evaluate.get_aggregated_experiment_statistics(type='timer', name='execution', query_aggregate='Median', total_aggregate='Geo', dbms_filter=dbms_filter)
        df = (df/1000.0).sort_index()
        if not df.empty:
            print("### Geometric Mean of Medians of Execution Times (only successful) [s]")
            df.columns = ['average execution time [s]']
            print(df.round(2))
        #####################
        df = evaluate.get_aggregated_experiment_statistics(type='timer', name='execution', query_aggregate='Max', total_aggregate='Sum', dbms_filter=dbms_filter).astype('float')/1000.
        if not df.empty:
            print("### Sum of Maximum Execution Times per Query (only successful) [s]")
            df.columns = ['sum of max execution times [s]']
            print(df.round(2))
        #####################
        df = num_processes*float(len(list_queries))*3600./df
        if not df.empty:
            print("### Queries per Hour (only successful) [QpH] - {}*{}*3600/(sum of max execution times)".format(int(num_processes), int(len(list_queries))))
            df.columns = ['queries per hour [Qph]']
            print(df.round(2))
    """



