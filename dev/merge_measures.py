from dbmsbenchmarker import *
import pandas as pd
from tabulate import tabulate



resultfolder = "./"
code = "1721923753"

evaluate = inspector.inspector(resultfolder)
evaluate.load_experiment(code)
evaluate = inspector.inspector(resultfolder)
evaluate.load_experiment(code)
list_connections = evaluate.get_experiment_list_connections()
list_queries = evaluate.get_experiment_list_queries()
query_properties = evaluate.get_experiment_query_properties()
def map_index_to_queryname(numQuery):
    if numQuery in query_properties and 'config' in query_properties[numQuery] and 'title' in query_properties[numQuery]['config']:
        return query_properties[numQuery]['config']['title']
    else:
        return numQuery

def get_measures_and_statistics_merged(numQuery, type='timer', name='run', dbms_filter=[], warmup=0, cooldown=0, factor_base='Mean'):
    dbms_list = {}
    for connection_nr, connection in evaluate.benchmarks.dbms.items():
        c = connection.connectiondata
        if 'orig_name' in c:
            orig_name = c['orig_name']
        else:
            orig_name = c['name']
        if not orig_name in dbms_list:
            dbms_list[orig_name] = [c['name']]
        else:
            dbms_list[orig_name].append(c['name'])
    # get measures per stream
    df1, df2 = evaluate.get_measures_and_statistics(numQuery, type=type, name=name)
    # mapping from derived name to orig_name
    reverse_mapping = {sub_db: main_db for main_db, sub_dbs in dbms_list.items() for sub_db in sub_dbs}
    df1['DBMS'] = df1.index.map(reverse_mapping)
    # concat values into single column per DBMS
    df_melted = pd.melt(df1, id_vars=['DBMS'], value_vars=df1.columns).sort_values('DBMS')
    df_melted = pd.DataFrame(df_melted[['DBMS', 'value']])
    #print(df_melted)
    #df_results = df_melted.set_index('DBMS')
    #df_results.columns = ['0']
    # compute stats per DBMS, i.e. per orig_name
    df_results = pd.DataFrame()
    df_results_stats = pd.DataFrame()
    df_groups = df_melted.groupby('DBMS')
    group_list = list(df_groups.groups.keys())
    for group in df_groups:
        df_dbms = pd.DataFrame(group[1]['value']).T
        df_dbms.index = [group[0]]
        df_dbms = df_dbms.T.reset_index(drop=True).T
        #print(df_dbms)
        df_results = pd.concat([df_results, df_dbms], ignore_index=False)
        df_dbms = evaluator.addStatistics(df_dbms, drop_measures=True)
        df_results_stats = pd.concat([df_results_stats, df_dbms], ignore_index=False)
    df_results.index.name = 'DBMS'
    if df_results_stats.empty:
        #print("No data")
        return df_results, df_results_stats
    if len(factor_base) > 0:
        df_results_stats = evaluator.addFactor(df_results_stats, factor_base)
    return df_results, df_results_stats

for numQuery in list_queries:
    print("# Query Q{}: {}".format(numQuery, map_index_to_queryname(str(numQuery))))
    #df1, df2 = evaluate.get_measures_and_statistics(numQuery)
    #print(df2.round(2))
    df1, df2 = get_measures_and_statistics_merged(numQuery)
    header = df2.columns
    print(tabulate(df2,headers=header, tablefmt="grid", floatfmt=".2f"))
    #print(df2.round(2))
    #break

