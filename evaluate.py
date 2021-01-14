"""
    Small demo how to inspect results of the Python Package DBMS Benchmarker.
    Mode resultsets prints the first difference in result sets for each query in the latest benchmark in the current folder.
    Copyright (C) 2021  Patrick Erdelt

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
from dbmsbenchmarker import *
import pandas as pd
from operator import itemgetter
import argparse

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='A benchmark tool for RDBMS. It connects to a given list of RDBMS via JDBC and runs a given list benchmark queries. Optionally some reports are generated.')
    parser.add_argument('-r', '--result-folder', help='folder for storing benchmark result files, default is given by timestamp', default="./")
    parser.add_argument('-c', '--code', help='code of experiment', default="")
    parser.add_argument('-q', '--query', help='number of query to inspect', default=None)
    parser.add_argument('-n', '--num-run', help='number of run to inspect', default=None)
    parser.add_argument('mode', help='run benchmarks and save results, or just read benchmark results from folder, or continue with missing benchmarks only', choices=['resultsets', 'errors', 'warnings'])
    args = parser.parse_args()
    # path of folder containing experiment results
    resultfolder = args.result_folder#"./"
    # create evaluation object for result folder
    evaluate = inspector.inspector(resultfolder)
    # list of all experiments in folder
    evaluate.list_experiments
    # dataframe of experiments
    evaluate.get_experiments_preview()
    if not len(args.code) > 0:
        # pick last experiment
        code = evaluate.list_experiments[len(evaluate.list_experiments)-1]
    else:
        code = args.code
    # load it
    evaluate.load_experiment(code)
    list_connections = evaluate.get_experiment_list_connections()
    list_queries = evaluate.get_experiment_list_queries()
    if args.mode == 'resultsets':
        for numQuery in list_queries:
            if args.query is not None and int(args.query) != numQuery:
                continue
            query = tools.query(evaluate.benchmarks.queries[numQuery-1])
            #print("PRECISION:")
            #print(query.restrict_precision)
            list_warnings = evaluate.get_warning(numQuery)
            #print("Q"+str(numQuery))
            #print(list_warnings)
            numRun=0
            df = evaluate.get_datastorage_df(numQuery, numRun)
            data = df.values
            #print(data)
            #data = [[round(float(item), int(query.restrict_precision)) if tools.convertToFloat(item) == float else item for item in sublist] for sublist in data]
            #print(data)
            if len(data) > 0 and sum([len(v) for k,v in list_warnings.items()]) > 0:
                print("\n===Q{}: {}===".format(numQuery, query.title))
                print(list_warnings)
                df = pd.DataFrame(sorted(data, key=itemgetter(*list(range(0,len(data[0]))))), columns=df.columns)
                for c in list_connections:
                    if c in list_warnings and len(list_warnings[c])>0:
                        s = evaluate.benchmarks.protocol['query'][str(numQuery)]['dataStorage']
                        r = evaluate.benchmarks.protocol['query'][str(numQuery)]['resultSets']
                        for numRun, result in enumerate(s):
                            if args.num_run is not None and int(args.num_run) != numRun:
                                continue
                            df2={}
                            data_stored = s[numRun]
                            if len(r[c]) == 0:
                                continue
                            print("numRun: "+str(numRun))
                            #print(data)
                            s2 = [[round(float(item), int(query.restrict_precision)) if tools.convertToFloat(item) == float else item for item in sublist] for sublist in data_stored]
                            #print(s2)
                            #print(r[c])
                            r2 = [[round(float(item), int(query.restrict_precision)) if tools.convertToFloat(item) == float else item for item in sublist] for sublist in r[c][numRun]]
                            #print(r2)
                            #for c, result_diff in r.items():
                            if len(r2) > 0 and r2 != s2:# r[c][numRun] != data_stored:
                                #print(c)
                                #s2 = [[round(float(item), int(query.restrict_precision)) if tools.convertToFloat(item) == float else item for item in sublist] for sublist in data_stored]
                                #print(s2)
                                #r2 = [[round(float(item), int(query.restrict_precision)) if tools.convertToFloat(item) == float else item for item in sublist] for sublist in r[c][numRun]]
                                #print(r2)
                                #exit()
                                df_tmp = evaluate.get_resultset_df(numQuery, c, numRun)
                                data = df_tmp.values
                                r2 = [[round(float(item), int(query.restrict_precision)) if tools.convertToFloat(item) == float else item for item in sublist] for sublist in data]
                                #print(r2)
                                data = r2
                                #print(data)
                                if len(data) > 0:
                                    df2[c] = pd.DataFrame(sorted(data, key=itemgetter(*list(range(0,len(data[0]))))), columns=df_tmp.columns)
                                else:
                                    df2[c] = pd.DataFrame()
                            for c,d in df2.items():
                                print("Storage has more than {}:".format(c))
                                print(inspector.getDifference12(df,d))
                                print("{} has more than storage".format(c))
                                print(inspector.getDifference12(d,df))
                            if len(df2) > 0:
                                break
    elif args.mode == 'errors':
        print(evaluate.get_total_errors())
        for numQuery in list_queries:
            if args.query is not None and int(args.query) != numQuery:
                continue
            query = tools.query(evaluate.benchmarks.queries[numQuery-1])
            print("===Q{}: {}===".format(numQuery, query.title))
            list_errors = evaluate.get_error(numQuery)
            #print(list_errors)
            list_errors = {k:v for k,v in list_errors.items() if len(v) > 0}
            df2_errors = pd.DataFrame(list_errors, index=['ERROR'])
            df2_errors = df2_errors.T.sort_index()
            pd.set_option('display.max_colwidth', None)
            print(df2_errors)
    elif args.mode == 'warnings':
        print(evaluate.get_total_errors())
        for numQuery in list_queries:
            if args.query is not None and int(args.query) != numQuery:
                continue
            query = tools.query(evaluate.benchmarks.queries[numQuery-1])
            print("===Q{}: {}===".format(numQuery, query.title))
            list_warnings = evaluate.get_warning(numQuery)
            #print(list_warnings)
            list_warnings = {k:v for k,v in list_warnings.items() if len(v) > 0}
            df2_warnings = pd.DataFrame(list_warnings, index=['WARNINGS'])
            df2_warnings = df2_warnings.T.sort_index()
            pd.set_option('display.max_colwidth', None)
            print(df2_warnings)


