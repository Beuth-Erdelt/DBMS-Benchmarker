#!/usr/bin/env python
# coding: utf-8

# <h1>Table of Contents<span class="tocSkip"></span></h1>
# <div class="toc"><ul class="toc-item"><li><span><a href="#Load-Evaluation-of-Benchmarks" data-toc-modified-id="Load-Evaluation-of-Benchmarks-1">Load Evaluation of Benchmarks</a></span><ul class="toc-item"><li><span><a href="#Inspect-Result-Folder" data-toc-modified-id="Inspect-Result-Folder-1.1">Inspect Result Folder</a></span></li><li><span><a href="#Pick-an-Experiment-and-load-it" data-toc-modified-id="Pick-an-Experiment-and-load-it-1.2">Pick an Experiment and load it</a></span></li><li><span><a href="#Load-general-Properties-into-Variables" data-toc-modified-id="Load-general-Properties-into-Variables-1.3">Load general Properties into Variables</a></span></li></ul></li><li><span><a href="#Show-Properties-of-the-Workload" data-toc-modified-id="Show-Properties-of-the-Workload-2">Show Properties of the Workload</a></span><ul class="toc-item"><li><span><a href="#Show-Properties-of-a-DBMS" data-toc-modified-id="Show-Properties-of-a-DBMS-2.1">Show Properties of a DBMS</a></span></li><li><span><a href="#Show-Properties-of-a-Query" data-toc-modified-id="Show-Properties-of-a-Query-2.2">Show Properties of a Query</a></span><ul class="toc-item"><li><span><a href="#Show-Errors" data-toc-modified-id="Show-Errors-2.2.1">Show Errors</a></span></li><li><span><a href="#Show-Warnings" data-toc-modified-id="Show-Warnings-2.2.2">Show Warnings</a></span></li><li><span><a href="#Show-Query-Template" data-toc-modified-id="Show-Query-Template-2.2.3">Show Query Template</a></span></li><li><span><a href="#Show-Query-Parameters" data-toc-modified-id="Show-Query-Parameters-2.2.4">Show Query Parameters</a></span></li><li><span><a href="#Show-Query-as-being-Run" data-toc-modified-id="Show-Query-as-being-Run-2.2.5">Show Query as being Run</a></span></li><li><span><a href="#Show-Query-as-being-Run-by-another-DBMS" data-toc-modified-id="Show-Query-as-being-Run-by-another-DBMS-2.2.6">Show Query as being Run by another DBMS</a></span></li><li><span><a href="#Show-Result-Set" data-toc-modified-id="Show-Result-Set-2.2.7">Show Result Set</a></span></li><li><span><a href="#Show-Result-Set-from-another-DBMS" data-toc-modified-id="Show-Result-Set-from-another-DBMS-2.2.8">Show Result Set from another DBMS</a></span></li></ul></li></ul></li><li><span><a href="#Some-Measures-of-the-Workload" data-toc-modified-id="Some-Measures-of-the-Workload-3">Some Measures of the Workload</a></span><ul class="toc-item"><li><span><a href="#Hardware-Metrics" data-toc-modified-id="Hardware-Metrics-3.1">Hardware Metrics</a></span><ul class="toc-item"><li><span><a href="#List-all-available-Metrics" data-toc-modified-id="List-all-available-Metrics-3.1.1">List all available Metrics</a></span></li><li><span><a href="#Get-Hardware-Metrics-for-Loading-Test" data-toc-modified-id="Get-Hardware-Metrics-for-Loading-Test-3.1.2">Get Hardware Metrics for Loading Test</a></span></li><li><span><a href="#Get-Hardware-Metrics-per-Stream" data-toc-modified-id="Get-Hardware-Metrics-per-Stream-3.1.3">Get Hardware Metrics per Stream</a></span></li></ul></li><li><span><a href="#Timing-Measures" data-toc-modified-id="Timing-Measures-3.2">Timing Measures</a></span><ul class="toc-item"><li><span><a href="#Mean-of-Means-of-Timer-Run" data-toc-modified-id="Mean-of-Means-of-Timer-Run-3.2.1">Mean of Means of Timer Run</a></span></li><li><span><a href="#Geometric-Mean-of-Medians-of-Timer-Run" data-toc-modified-id="Geometric-Mean-of-Medians-of-Timer-Run-3.2.2">Geometric Mean of Medians of Timer Run</a></span></li></ul></li><li><span><a href="#Plots" data-toc-modified-id="Plots-3.3">Plots</a></span></li></ul></li><li><span><a href="#Some-Measures-per-Query" data-toc-modified-id="Some-Measures-per-Query-4">Some Measures per Query</a></span><ul class="toc-item"><li><span><a href="#Timing-Measures" data-toc-modified-id="Timing-Measures-4.1">Timing Measures</a></span><ul class="toc-item"><li><span><a href="#Means-of-Timer-Runs" data-toc-modified-id="Means-of-Timer-Runs-4.1.1">Means of Timer Runs</a></span></li><li><span><a href="#Maximum-of-Run-Throughput" data-toc-modified-id="Maximum-of-Run-Throughput-4.1.2">Maximum of Run Throughput</a></span></li><li><span><a href="#Latency-of-Timer-Execution" data-toc-modified-id="Latency-of-Timer-Execution-4.1.3">Latency of Timer Execution</a></span></li><li><span><a href="#Mean-of-Latency-of-Timer-Execution-per-DBMS" data-toc-modified-id="Mean-of-Latency-of-Timer-Execution-per-DBMS-4.1.4">Mean of Latency of Timer Execution per DBMS</a></span></li><li><span><a href="#Coefficient-of-Variation-of-Latency-of-Timer-Execution-per-DBMS" data-toc-modified-id="Coefficient-of-Variation-of-Latency-of-Timer-Execution-per-DBMS-4.1.5">Coefficient of Variation of Latency of Timer Execution per DBMS</a></span></li><li><span><a href="#Latency-of-Timer-Connection" data-toc-modified-id="Latency-of-Timer-Connection-4.1.6">Latency of Timer Connection</a></span></li><li><span><a href="#Latency-of-Timer-Data-Transfer" data-toc-modified-id="Latency-of-Timer-Data-Transfer-4.1.7">Latency of Timer Data Transfer</a></span></li><li><span><a href="#Latency-of-Timer-Run---normalized-to-1-per-Query" data-toc-modified-id="Latency-of-Timer-Run---normalized-to-1-per-Query-4.1.8">Latency of Timer Run - normalized to 1 per Query</a></span></li><li><span><a href="#Size-of-Result-Sets-per-Query" data-toc-modified-id="Size-of-Result-Sets-per-Query-4.1.9">Size of Result Sets per Query</a></span></li><li><span><a href="#Size-of-Result-Sets-per-Query---normalized-to-1" data-toc-modified-id="Size-of-Result-Sets-per-Query---normalized-to-1-4.1.10">Size of Result Sets per Query - normalized to 1</a></span></li><li><span><a href="#Size-of-Result-Sets-per-Query---normalized-to-1" data-toc-modified-id="Size-of-Result-Sets-per-Query---normalized-to-1-4.1.11">Size of Result Sets per Query - normalized to 1</a></span></li><li><span><a href="#Table-of-Errors" data-toc-modified-id="Table-of-Errors-4.1.12">Table of Errors</a></span></li><li><span><a href="#Table-of-Warnings" data-toc-modified-id="Table-of-Warnings-4.1.13">Table of Warnings</a></span></li><li><span><a href="#Total-Time-[s]-per-Query" data-toc-modified-id="Total-Time-[s]-per-Query-4.1.14">Total Time [s] per Query</a></span></li><li><span><a href="#Total-Time-per-Query---normalized-to-100%" data-toc-modified-id="Total-Time-per-Query---normalized-to-100%-4.1.15">Total Time per Query - normalized to 100%</a></span></li><li><span><a href="#Total-Time-per-Query---normalized-to-100%" data-toc-modified-id="Total-Time-per-Query---normalized-to-100%-4.1.16">Total Time per Query - normalized to 100%</a></span></li></ul></li><li><span><a href="#Plots" data-toc-modified-id="Plots-4.2">Plots</a></span></li></ul></li><li><span><a href="#Inspect-Single-Queries" data-toc-modified-id="Inspect-Single-Queries-5">Inspect Single Queries</a></span><ul class="toc-item"><li><span><a href="#Measures" data-toc-modified-id="Measures-5.1">Measures</a></span><ul class="toc-item"><li><span><a href="#Measures-of-Execution-Times" data-toc-modified-id="Measures-of-Execution-Times-5.1.1">Measures of Execution Times</a></span></li></ul></li><li><span><a href="#Statistics" data-toc-modified-id="Statistics-5.2">Statistics</a></span><ul class="toc-item"><li><span><a href="#Statistics-of-Execution-Times" data-toc-modified-id="Statistics-of-Execution-Times-5.2.1">Statistics of Execution Times</a></span></li></ul></li><li><span><a href="#Plots" data-toc-modified-id="Plots-5.3">Plots</a></span><ul class="toc-item"><li><span><a href="#Timer-Run---Line-Plot" data-toc-modified-id="Timer-Run---Line-Plot-5.3.1">Timer Run - Line Plot</a></span></li><li><span><a href="#Mean-of-Timer-Run---Bar-Plot" data-toc-modified-id="Mean-of-Timer-Run---Bar-Plot-5.3.2">Mean of Timer Run - Bar Plot</a></span></li><li><span><a href="#Timer-Run---Boxplot" data-toc-modified-id="Timer-Run---Boxplot-5.3.3">Timer Run - Boxplot</a></span></li><li><span><a href="#Timer-Run-Histogram" data-toc-modified-id="Timer-Run-Histogram-5.3.4">Timer Run Histogram</a></span></li></ul></li></ul></li></ul></div>

# # Load Evaluation of Benchmarks
# 
# Import some libraries

# In[1]:
import argparse
import traceback

if __name__ == '__main__':
    description = """Automatically inspect result of experiment for failures.
    """
    # argparse
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('-db', '--debug', help='dump debug informations', action='store_true')
    parser.add_argument('-cx', '--context', help='context of Kubernetes (for a multi cluster environment), default is current context', default=None)
    parser.add_argument('-e', '--experiment', help='sets experiment code for continuing started experiment', default=None)
    parser.add_argument('-r', '--result-folder',
                        help='Folder storing benchmark result files.',
                        default='./')  # set your own default path here
    # evaluate args
    args = parser.parse_args()
    try:
        code = args.experiment
        resultfolder = args.result_folder

        ok = True

        from dbmsbenchmarker import *
        import pandas as pd
        pd.set_option("display.max_rows", None)
        pd.set_option('display.max_colwidth', None)

        # Some plotly figures
        import plotly.graph_objects as go
        import plotly.figure_factory as ff

        # Some nice output
        #from IPython.display import display, Markdown

        def display(string):
            print(string)

        def Markdown(string):
            return string

        import logging
        logging.basicConfig(level=logging.INFO)


        # ## Inspect Result Folder

        # In[2]:


        # path of folder containing experiment results
        #resultfolder = "results/"

        # create evaluation object for result folder
        evaluate = inspector.inspector(resultfolder)

        # list of all experiments in folder
        # evaluate.list_experiments
        # dataframe of experiments
        print(evaluate.get_experiments_preview())


        # ## Pick an Experiment and load it

        # In[3]:


        # last Experiment
        #code = evaluate.list_experiments[len(evaluate.list_experiments)-1]

        # Specific Experiment
        #code = '1668781697'

        # load it
        evaluate.load_experiment(code)


        # ## Load general Properties into Variables

        # In[4]:


        # get experiment workflow
        df = evaluate.get_experiment_workflow()
        #print(df)

        # get workload properties
        workload_properties = evaluate.get_experiment_workload_properties()
        print("workload_properties", workload_properties)
        print(workload_properties['name'])
        print(workload_properties['info'])

        # list queries
        list_queries = evaluate.get_experiment_list_queries()

        # list connections
        list_nodes = evaluate.get_experiment_list_nodes()
        list_dbms = evaluate.get_experiment_list_dbms()
        # we need at least one dbms
        ok = ok and (len(list_dbms) > 0)
        list_connections = evaluate.get_experiment_list_connections()
        # we need at least one connection
        ok = ok and (len(list_connections) > 0)
        list_connections_node = evaluate.get_experiment_list_connections_by_node()
        list_connections_dbms = evaluate.get_experiment_list_connections_by_dbms()
        list_connections_clients = evaluate.get_experiment_list_connections_by_connectionmanagement('numProcesses')
        list_connections_gpu = evaluate.get_experiment_list_connections_by_hostsystem('GPU')
        list_connections_dockerimage = evaluate.get_experiment_list_connections_by_parameter('dockerimage')

        # colors by dbms
        list_connections_dbms = evaluate.get_experiment_list_connections_by_dbms()
        connection_colors = evaluate.get_experiment_list_connection_colors(list_connections_dbms)

        # fix some examples:
        # first connection, first query, first run
        connection = list_connections[0]
        numQuery = 1
        numRun = 0


        # # Show Properties of the Workload
        # 
        # ## Show Properties of a DBMS 

        # In[5]:


        #connection = 'MonetDB-1-1'

        display(Markdown("### Properties of {}".format(connection)))

        print(evaluator.pretty(evaluate.get_experiment_connection_properties(connection)))


        # ## Show Properties of a Query

        # In[6]:


        connection = list_connections[0]
        numQuery = 1
        numRun = 0


        # ### Show Errors

        # In[7]:


        list_errors = evaluate.get_error(numQuery)

        display(Markdown("### Errors of Query {}".format(numQuery)))
        df = pd.DataFrame.from_dict(list_errors, orient='index').sort_index()
        print(df)

        # ### Show Warnings

        # In[8]:


        list_errors = evaluate.get_warning(numQuery)

        display(Markdown("### Warnings of Query {}".format(numQuery)))
        df = pd.DataFrame.from_dict(list_errors, orient='index').sort_index()
        print(df)


        # ### Show Query Template

        # In[9]:


        query_properties = evaluate.get_experiment_query_properties()

        display(Markdown("#### Show Query Template {} - {}".format(numQuery, query_properties[str(numQuery)]['config']['title'])))
        print(query_properties[str(numQuery)]['config']['query'])


        # ### Show Query Parameters

        # In[10]:


        display(Markdown("#### Show Parameters of Query {} - {}".format(numQuery, query_properties[str(numQuery)]['config']['title'])))

        df = evaluate.get_parameter_df(numQuery)
        print(df)


        # ### Show Query as being Run

        # In[11]:


        display(Markdown("#### Show Query {} as run by {} - Run number {}".format(numQuery, connection, numRun)))

        query_string = evaluate.get_querystring(numQuery, connection, numRun)
        print(query_string)


        # ### Show Query as being Run by another DBMS

        # In[12]:


        #display(Markdown("#### Show Query {} as run by {} - Run number {}".format(numQuery, "MonetDB-1-1", numRun)))

        #query_string = evaluate.get_querystring(numQuery, connection, numRun)
        #print(query_string)


        # ### Show Result Set

        # In[13]:


        display(Markdown("#### Show Result Set of Query {} as run by {} - Run number {}".format(numQuery, connection, numRun)))

        df = evaluate.get_datastorage_df(numQuery, numRun)
        print(df)


        # ### Show Result Set from another DBMS

        # In[14]:


        #display(Markdown("#### Show Result Set of Query {} as run by {} - Run number {}".format(numQuery, "MonetDB-1-1", numRun)))

        #df = evaluate.get_resultset_df(numQuery, "MonetDB-1-1", numRun)
        #print(df)


        # # Some Measures of the Workload
        # 
        # ## Hardware Metrics

        # ### List all available Metrics

        # In[15]:


        display(Markdown("### Hardware Metrics"))

        df = pd.DataFrame(monitor.metrics.metrics).T
        print(df)


        # ### Get Hardware Metrics for Loading Test

        # In[16]:


        df = evaluate.get_loading_metrics('total_cpu_memory')
        df = df.T.max().sort_index()
        display(Markdown("### RAM of Ingestion"))
        pd.DataFrame(df)
        print(df)
        # we need at least some memory used
        if not df.empty:
            ok = ok and (df.min().min() > 0)
        else:
            ok = False

        df = evaluate.get_loading_metrics('total_cpu_util_s')
        df = df.T.max().sort_index() - df.T.min().sort_index() # compute difference of counter

        display(Markdown("### CPU of Ingestion (via counter)"))
        pd.DataFrame(df)
        print(df)
        # we need at least some CPU used
        if not df.empty:
            ok = ok and (df.min().min() > 0)
        else:
            ok = False


        # In[17]:


        df = evaluate.get_loading_metrics('total_cpu_util')
        df = df.T.sum().sort_index() # computer sum of rates

        display(Markdown("### CPU of Ingestion (via rate)"))
        pd.DataFrame(df)
        print(df)
        # we need at least some CPU used
        if not df.empty:
            ok = ok and (df.min().min() > 0)
        else:
            ok = False


        # ### Get Hardware Metrics per Stream

        # In[18]:


        df = evaluate.get_streaming_metrics('total_cpu_memory')
        df = df.T.max().sort_index()
        display(Markdown("### RAM of Stream"))
        pd.DataFrame(df)
        print(df)
        # we need at least some memory used
        if not df.empty:
            ok = ok and (df.min().min() > 0)
        else:
            ok = False

        df = evaluate.get_streaming_metrics('total_cpu_util_s')
        df = df.T.max().sort_index() - df.T.min().sort_index() # compute difference of counter

        display(Markdown("### CPU of Stream (via counter)"))
        pd.DataFrame(df)
        print(df)
        # we need at least some CPU used
        if not df.empty:
            ok = ok and (df.min().min() > 0)
        else:
            ok = False


        # In[19]:


        df = evaluate.get_streaming_metrics('total_cpu_util')
        df = df.T.sum().sort_index() # computer sum of rates

        display(Markdown("### CPU of Stream (via rate)"))
        pd.DataFrame(df)
        print(df)
        # we need at least some CPU used
        if not df.empty:
            ok = ok and (df.min().min() > 0)
        else:
            ok = False


        # ## Timing Measures
        # 
        # ### Mean of Means of Timer Run

        # In[20]:


        df = evaluate.get_aggregated_experiment_statistics(type='timer', name='run', query_aggregate='Mean', total_aggregate='Mean')
        df = (df/1000.0).sort_index()

        display(Markdown("### Mean of Means of Timer Run [s]"))
        print(df)
        # we need at least some mean of mean
        if not df.empty:
            ok = ok and (df.min().min() > 0)
        else:
            ok = False



        # ### Geometric Mean of Medians of Timer Run

        # In[21]:


        df = evaluate.get_aggregated_experiment_statistics(type='timer', name='run', query_aggregate='Median', total_aggregate='Geo')
        df = (df/1000.0).sort_index()

        display(Markdown("### Geometric Mean of Medians of Timer Run [s]"))
        print(df)
        # we need at least some geo mean of medians
        if not df.empty:
            ok = ok and (df.min().min() > 0)
        else:
            ok = False

        # ## Plots

        # In[22]:


        #get_ipython().run_line_magic('matplotlib', 'inline')


        # In[23]:


        df = evaluate.get_aggregated_experiment_statistics(type='timer', name='run', query_aggregate='Median', total_aggregate='Geo')
        df = df.sort_index()

        fig = go.Figure()
        for i in range(len(df.index)):
            t = fig.add_trace(go.Bar(x=[df.index[i]], y=df.iloc[i], name=df.index[i], marker=dict(color=connection_colors[df.index[i]])))

        fig.update_layout(title_text='Geometric Mean of Medians of Timer Run [s]')
        ##fig.show()


        # # Some Measures per Query

        # ## Timing Measures
        # 
        # ### Means of Timer Runs

        # In[24]:


        df = evaluate.get_aggregated_query_statistics(type='timer', name='run', query_aggregate='Mean').sort_index().T

        display(Markdown("### Means of Timer Runs [ms]"))
        print(df)
        # we need at least some mean values at some query
        if not df.empty:
            ok = ok and (df.min().min() > 0)
        else:
            ok = False


        # ### Maximum of Run Throughput

        # In[25]:


        df = (evaluate.get_aggregated_query_statistics(type='throughput', name='run', query_aggregate='Max')).sort_index().T

        display(Markdown("### Maximum of Run Throughput [1/s]"))
        print(df)
        # we need at least some max values at some query
        if not df.empty:
            ok = ok and (df.min().min() > 0)
        else:
            ok = False


        # ### Latency of Timer Execution

        # In[26]:


        df = evaluate.get_aggregated_query_statistics(type='latency', name='execution', query_aggregate='Mean').sort_index().T

        display(Markdown("### Latency of Timer Execution [ms]"))
        print(df)
        # we need at least some mean values at some query
        if not df.empty:
            ok = ok and (df.min().min() > 0)
        else:
            ok = False


        # ### Mean of Latency of Timer Execution per DBMS

        # In[27]:


        df = evaluate.get_aggregated_query_statistics(type='latency', name='execution', query_aggregate='Mean').sort_index()
        df = evaluate.get_aggregated_by_connection(df, list_connections_dbms, connection_aggregate='Mean').T

        display(Markdown("### Mean of Latency of Timer Execution per DBMS [ms]"))
        print(df)
        # we need at least some mean values at some query
        if not df.empty:
            ok = ok and (df.min().min() > 0)
        else:
            ok = False


        # ### Coefficient of Variation of Latency of Timer Execution per DBMS

        # In[28]:


        df = evaluate.get_aggregated_query_statistics(type='latency', name='execution', query_aggregate='Mean').sort_index()
        df = evaluate.get_aggregated_by_connection(df, list_connections_dbms, connection_aggregate='cv [%]').T

        display(Markdown("### CV of Latency of Timer Execution per DBMS [%]"))
        print(df)
        # we need at least some mean values at some query
        if not df.empty:
            ok = ok and (df.min().min() > 0)
        else:
            ok = False


        # ### Latency of Timer Connection

        # In[29]:


        df = evaluate.get_aggregated_query_statistics(type='timer', name='connection', query_aggregate='Mean').T

        display(Markdown("### Latency of Timer Connection [ms]"))
        print(df)


        # ### Latency of Timer Data Transfer

        # In[30]:


        df = evaluate.get_aggregated_query_statistics(type='timer', name='datatransfer', query_aggregate='Mean').T

        display(Markdown("### Latency of Timer Data Transfer [ms]"))
        print(df)


        # ### Latency of Timer Run - normalized to 1 per Query

        # In[31]:


        df = evaluate.get_aggregated_query_statistics(type='timer', name='run', query_aggregate='factor').sort_index().T

        display(Markdown("### Latency of Timer Run - normalized to 1 per Query"))
        print(df)
        # we need at least some mean values at some query
        if not df.empty:
            ok = ok and (df.min().min() > 0)
        else:
            ok = False


        # ### Size of Result Sets per Query

        # In[32]:


        df = evaluate.get_total_resultsize().T

        display(Markdown("### Size of Result Sets per Query"))
        print(df)


        # ### Size of Result Sets per Query - normalized to 1

        # ### Size of Result Sets per Query - normalized to 1

        # In[33]:


        df = evaluate.get_total_resultsize_normalized()

        display(Markdown("### Size of Result Sets per Query - normalized to 1"))
        print(df)


        # ### Table of Errors

        # In[34]:


        df = evaluate.get_total_errors().T

        display(Markdown("### Table of Errors"))
        print(df)


        # ### Table of Warnings

        # In[35]:


        df = evaluate.get_total_warnings().T

        display(Markdown("### Table of Warnings"))
        print(df)


        # ### Total Time [s] per Query

        # In[36]:


        df = evaluate.get_total_times().T/1000.0

        display(Markdown("### Total Time [s] per Query"))
        print(df)
        # we need at least some mean values at some query
        if not df.empty:
            ok = ok and (df.min().min() > 0)
        else:
            ok = False


        # ### Total Time per Query - normalized to 100%

        # In[37]:


        df = evaluate.get_total_times_normalized().T

        display(Markdown("### Total Time per Query - normalized to 100%"))
        print(df)


        # ### Total Time per Query - normalized to 100%

        # In[38]:


        df = evaluate.get_total_times_relative().T

        display(Markdown("### Total Time per Query - normalized to 100%"))
        print(df)


        # ## Plots

        # In[39]:


        df = (evaluate.get_aggregated_query_statistics(type='timer', name='run', query_aggregate='Mean').T/1000.0).round(2)
        #.sort_index(ascending=False)
        #df=df.T
        #df=df.round(2)

        fig1 = ff.create_annotated_heatmap(
            x=list(df.columns),
            y=list(df.index),
            z=df.values.tolist(),
            showscale=True,
            colorscale='Reds',
            xgap=1,
            ygap=1,
            )

        fig1.update_layout(title_text='Timer Run - Mean per Query [s]')
        fig1.layout.xaxis.type = 'category'
        fig1.layout.yaxis.type = 'category'

        #fig1.show()


        # In[40]:


        df = (evaluate.get_aggregated_query_statistics(type='timer', name='run', query_aggregate='Std Dev').T/1000.0).round(2)
        #.sort_index(ascending=False)
        #df=df.T
        #df=df.round(2)

        fig1 = ff.create_annotated_heatmap(
            x=list(df.columns),
            y=list(df.index),
            z=df.values.tolist(),
            showscale=True,
            colorscale='Reds',
            xgap=1,
            ygap=1,
            )

        fig1.update_layout(title_text='Timer Run - Std Dev per Query [s]')
        fig1.layout.xaxis.type = 'category'
        fig1.layout.yaxis.type = 'category'

        #fig1.show()


        # In[41]:


        df = evaluate.get_aggregated_query_statistics(type='timer', name='run', query_aggregate='factor').round(2)
        df=df.sort_index(ascending=True).T
        #df=df.T
        #df=df.round(2)

        fig1 = ff.create_annotated_heatmap(
            x=list(df.columns),
            y=list(df.index),
            z=df.values.tolist(),
            showscale=True,
            colorscale='Reds',
            xgap=1,
            ygap=1,
            )

        fig1.update_layout(title_text='Timer Run - Factor per Query [s]')
        fig1.layout.xaxis.type = 'category'
        fig1.layout.yaxis.type = 'category'

        #fig1.show()


        # # Inspect Single Queries
        # 
        # ## Measures

        # In[42]:


        numQuery = 1

        # ### Measures of Execution Times

        # In[43]:


        df1, df2 = evaluate.get_measures_and_statistics(numQuery, type='timer', name='execution')

        display(Markdown("### Measures of Execution Times - {} Runs of Query {}".format(len(df1.columns), numQuery)))
        print(df1.sort_index())
        # we need at least some values at some query
        if not df1.empty:
            ok = ok and (df1.min().min() > 0)
        else:
            ok = False


        # ## Statistics

        # ### Statistics of Execution Times

        # In[44]:


        display(Markdown("### Statistics of Execution Times - {} Runs of Query {}".format(len(df1.columns), numQuery)))
        print(df2.sort_index())


        # ## Plots

        # ### Timer Run - Line Plot

        # In[45]:


        df1,df2=evaluate.get_measures_and_statistics(numQuery, type='timer', name='run', warmup=0)
        df1 = df1.sort_index()

        # Plots
        fig = go.Figure()
        for i in range(len(df1.index)):
            t = fig.add_trace(go.Scatter(x=df1.T.index, y=df1.iloc[i], name=df1.index[i], line=dict(color=connection_colors[df1.index[i]], width=1)))

        fig.update_layout(title_text='Timer Run [ms] - Query {} ({} Measures)'.format(numQuery, len(df1.columns)))
        #fig.show()


        # ### Mean of Timer Run - Bar Plot

        # In[46]:


        # Bar
        df1, df2 = evaluate.get_measures_and_statistics(numQuery, type='timer', name='run')
        df = tools.dataframehelper.collect(df2, 'Mean', 'timer_run_mean').sort_index()

        fig = go.Figure()
        for i in range(len(df.index)):
            t = fig.add_trace(go.Bar(x=[df.index[i]], y=df.iloc[i], name=df.index[i], marker=dict(color=connection_colors[df.index[i]])))

        fig.update_layout(title_text='Mean of Timer Run [s] - Query {}'.format(numQuery))
        #fig.show()


        # ### Timer Run - Boxplot

        # In[47]:


        # Boxplots
        df1, df2 = evaluate.get_measures_and_statistics(numQuery, type='timer', name='run')
        df1 = df1.sort_index()

        fig = go.Figure()
        for i in range(len(df1.index)):
            t = fig.add_trace(go.Box(y=df1.iloc[i], name=df1.index[i], line=dict(color=connection_colors[df1.index[i]], width=1), boxmean='sd'))

        fig.update_layout(title_text='Timer Run [ms] - Query {}'.format(numQuery))
        #fig.show()


        # ### Timer Run Histogram

        # In[48]:


        # Histogram
        numQuery = 1

        df1, df2 = evaluate.get_measures_and_statistics(numQuery, type='timer', name='run')
        df1=df1.sort_index()

        fig = go.Figure(layout = go.Layout(barmode='overlay'))
        for i in range(len(df1.index)):
            t = fig.add_trace(go.Histogram(x=df1.iloc[i], name=df1.index[i], opacity=0.75, marker=dict(color=connection_colors[df1.index[i]])))

        fig.update_layout(title_text='Timer Run Histogram - Query {}'.format(numQuery))
        #fig.show()


        # In[ ]:

        print("ok is", ok)

        print("EVERYTHING WENT WELL")
        exit(0)

    except Exception as e:
        print("SOMETHING WENT WRONG")
        print(traceback.format_exc())
        exit(1)
    finally:
        pass



