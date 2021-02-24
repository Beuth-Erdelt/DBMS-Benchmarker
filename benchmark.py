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

from dbmsbenchmarker import *


if __name__ == '__main__':
	# argparse
	parser = argparse.ArgumentParser(description='A benchmark tool for RDBMS. It connects to a given list of RDBMS via JDBC and runs a given list benchmark queries. Optionally some reports are generated.')
	parser.add_argument('mode', help='run benchmarks and save results, or just read benchmark results from folder, or continue with missing benchmarks only', choices=['run','read', 'continue'])
	parser.add_argument('-d', '--debug', help='dump debug informations', action='store_true')
	parser.add_argument('-b', '--batch', help='batch mode (more protocol-like output), automatically on for debug mode', action='store_true')
	parser.add_argument('-qf', '--query-file', help='name of query config file', default='queries.config')
	parser.add_argument('-cf', '--connection-file', help='name of connection config file', default='connections.config')
	parser.add_argument('-q', '--query', help='number of query to benchmark', default=None)
	parser.add_argument('-c', '--connection', help='name of connection to benchmark', default=None)
	parser.add_argument('-l', '--latex-template', help='name of latex template for reporting', default='pagePerQuery')
	parser.add_argument('-f', '--config-folder', help='folder containing query and connection config files. If set, the names connections.config and queries.config are assumed automatically.', default=None)
	parser.add_argument('-r', '--result-folder', help='folder for storing benchmark result files, default is given by timestamp', default=None)
	parser.add_argument('-g', '--generate-output', help='generate new report files', default='no', choices=['no','yes'])
	parser.add_argument('-e', '--generate-evaluation', help='generate new evaluation file', default='no', choices=['no','yes'])
	parser.add_argument('-w', '--working', help='working per query or connection', default='query', choices=['query','connection'])
	parser.add_argument('-a', '--anonymize', help='anonymize all dbms', action='store_true', default=False)
	parser.add_argument('-u', '--unanonymize', help='unanonymize some dbms, only sensible in combination with anonymize', nargs='*', default=[])
	parser.add_argument('-p', '--numProcesses', help='Number of parallel client processes. Global setting, can be overwritten by connection. If None given, half of all available processes is taken', default=None)
	parser.add_argument('-s', '--seed', help='random seed', default=None)
	parser.add_argument('-sl', '--sleep', help='sleep SLEEP seconds before going to work', default=0)
	parser.add_argument('-sf', '--subfolder', help='stores results in a SUBFOLDER of the result folder', default=None)
	parser.add_argument('-vq', '--verbose-queries', help='print every query that is sent', action='store_true', default=False)
	parser.add_argument('-vs', '--verbose-statistics', help='print statistics about query that have been sent', action='store_true', default=False)
	parser.add_argument('-pn', '--num-run', help='Parameter: Number of executions per query', default=0)
	parser.add_argument('-m', '--metrics', help='collect hardware metrics', action='store_true', default=False)
	#parser.add_argument('-pt', '--timeout', help='Parameter: Timeout in seconds', default=0)
	args = parser.parse_args()
	# evaluate args
	if args.debug:
		logging.basicConfig(level=logging.DEBUG)
		bBatch = True
	else:
		logging.basicConfig(level=logging.ERROR)
		bBatch = args.batch
	# sleep before going to work
	if int(args.sleep) > 0:
		print("Sleeping ", int(args.sleep), "seconds")
		time.sleep(int(args.sleep))
	# set verbose lebvel
	if args.verbose_queries:
		benchmarker.BENCHMARKER_VERBOSE_QUERIES = True
	if args.verbose_statistics:
		benchmarker.BENCHMARKER_VERBOSE_STATISTICS = True
	if int(args.num_run) > 0:
		querymanagement = {
 			'numRun': int(args.num_run),
 		}
		tools.query.template = querymanagement
	# dbmsbenchmarker with reporter
	experiments = benchmarker.benchmarker(
		result_path=args.result_folder,
		working=args.working,
		batch=bBatch,
		subfolder=args.subfolder,
		fixedQuery=args.query,
		fixedConnection=args.connection,
		anonymize=args.anonymize,
		unanonymize=args.unanonymize,
		numProcesses=args.numProcesses,
		seed=args.seed)
	experiments.getConfig(args.config_folder, args.connection_file, args.query_file)
	# switch for args.mode
	if args.mode == 'read':
		experiments.readBenchmarks()
	elif args.mode == 'run':
		if experiments.continuing:
			experiments.generateAllParameters()
			experiments.continueBenchmarks(overwrite = True)
		else:
			experiments.generateAllParameters()
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
	if args.generate_output == 'yes':
		experiments.overwrite = True
		# store measures ans statistics in separate files
		experiments.reporter.append(benchmarker.reporter.pickler(experiments))
		experiments.reporter.append(benchmarker.reporter.dataframer(experiments))
		# collect hardware metrics
		experiments.reporter.append(benchmarker.reporter.metricer(experiments))
		# generate charts
		experiments.reporter.append(benchmarker.reporter.barer(experiments))
		experiments.reporter.append(benchmarker.reporter.ploter(experiments))
		experiments.reporter.append(benchmarker.reporter.boxploter(experiments))
		experiments.reporter.append(benchmarker.reporter.tps(experiments))
		experiments.reporter.append(benchmarker.reporter.hister(experiments))
		# generate latex report
		experiments.reporter.append(benchmarker.reporter.latexer(experiments, args.latex_template))
		experiments.generateReportsAll()
	if args.generate_evaluation == 'yes':
		experiments.overwrite = True
		evaluator.evaluator(experiments, load=False, force=True)
