"""
    Classes for collecting monitoring data for the Python Package DBMS Benchmarker
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
import requests
import pandas as pd
from datetime import datetime
import matplotlib
import matplotlib.pyplot as plt
import csv
import os.path
import logging
from dbmsbenchmarker import benchmarker, tools
from numpy import nan
# https://www.robustperception.io/productive-prometheus-python-parsing

class metrics():
    metrics = {
        'total_cpu_memory': {
            'query': '(node_memory_MemTotal_bytes-node_memory_MemFree_bytes-node_memory_Buffers_bytes-node_memory_Cached_bytes)/1024/1024',
            'title': 'CPU Memory [MiB]'
        },
        'total_cpu_memory_cached': {
            'query': '(node_memory_Cached_bytes)/1024/1024',
            'title': 'CPU Memory Cached [MiB]'
        },
        'total_cpu_util': {
            'query': '100 - (avg by (instance) (irate(node_cpu_seconds_total{mode="idle"}[5m])) * 100)',
            'title': 'CPU Util [%]'
        },
        'total_gpu_util': {
            'query': 'sum(dcgm_gpu_utilization)',
            'title': 'GPU Util [%]'
        },
        'total_gpu_power': {
            'query': 'sum(dcgm_power_usage)',
            'title': 'GPU Power Usage [W]'
        },
        'total_gpu_memory': {
            'query': 'sum(dcgm_fb_used)',
            'title': 'GPU Memory [MiB]'
        },
        'total_cpu_throttled': {
            'query': 'sum(rate(container_cpu_cfs_throttled_seconds_total{{job="monitor-node", container_label_io_kubernetes_container_name="dbms"}}[1m]))',
            'title': 'CPU Throttle [%]'
        },
        'total_cpu_util_others': {
            'query': 'sum(irate(container_cpu_usage_seconds_total{{job="monitor-node", container_label_io_kubernetes_container_name!="dbms",id!="/"}}[1m]))',
            'title': 'CPU Others [%]'
        },
        'total_cpu_util_s': {
            'query': 'sum(container_cpu_usage_seconds_total{{job="monitor-node", container_label_io_kubernetes_container_name="dbms"}})',
            'title': 'CPU Util [s]'
        },
        'total_cpu_util_user_s': {
            'query': 'sum(container_cpu_user_seconds_total{{job="monitor-node", container_label_io_kubernetes_container_name="dbms"}})',
            'title': 'CPU Util User [s]'
        },
        'total_cpu_util_sys_s': {
            'query': 'sum(container_cpu_system_seconds_total{{job="monitor-node", container_label_io_kubernetes_container_name="dbms"}})',
            'title': 'CPU Util Sys [s]'
        },
        'total_cpu_throttled_s': {
            'query': 'sum(container_cpu_cfs_throttled_seconds_total{{job="monitor-node", container_label_io_kubernetes_container_name="dbms"}})',
            'title': 'CPU Throttle [s]'
        },
        'total_cpu_util_others_s': {
            'query': 'sum(container_cpu_usage_seconds_total{{job="monitor-node", container_label_io_kubernetes_container_name!="dbms",id!="/"}})',
            'title': 'CPU Util Others [s]'
        },
        'total_network_rx': {
            'query': 'sum(container_network_receive_bytes_total{{container_label_app="dbmsbenchmarker", job="monitor-node"}})',
            'title': 'Net Rx [b]'
        },
        'total_network_tx': {
            'query': 'sum(container_network_transmit_bytes_total{{container_label_app="dbmsbenchmarker", job="monitor-node"}})',
            'title': 'Net Tx [b]'
        },
        'total_fs_read': {
            'query': 'sum(container_fs_reads_bytes_total{{job="monitor-node", container_label_io_kubernetes_container_name="dbms"}})',
            'title': 'FS Read [b]'
        },
        'total_fs_write': {
            'query': 'sum(container_fs_writes_bytes_total{{job="monitor-node", container_label_io_kubernetes_container_name="dbms"}})',
            'title': 'FS Write [b]'
        },
    }
    m_avg = None
    def __init__(self, benchmarks):
        self.step = 1
        self.benchmarker = benchmarks
    @staticmethod
    def getMetrics(url, metric, time_start, time_end, step=1):
        logging.debug("getMetrics from "+url)
        def fetch_interval(time_start, time_end, step):
            query = 'query_range'#?query='+metric['query']+'&start='+str(time_start)+'&end='+str(time_end)+'&step='+str(self.step)
            params = {
                'query': metric['query'],
                'start': str(time_start),
                'end': str(time_end),
                'step': str(step),
            }
            logging.debug("Querying metrics: "+url+query, params)
            logging.debug(params)
            #headers = {'Authorization': self.token}
            l = [(t,0) for t in range(time_start, time_end+1)]#[(time_start,0)]
            #return l
            #print(self.url+query)
            try:
                #r = requests.post(self.url+query)#, headers=headers)
                r = requests.get(url+query, params=params)#, headers=headers)
                #print(r.json())
                if isinstance(r.json(), dict) and 'data' in r.json() and 'result' in r.json()['data'] and len(r.json()['data']['result']) > 0:
                    l = r.json()['data']['result'][0]['values']
                    # missing values due to end of monitoring?
                    n = time_end-time_start-len(l)+1
                    l2 = [(t+len(l)+time_start, 0) for t in range(n)]
                    l = l + l2
                else:
                    #print(metric, url+query, r.json())
                    l = [(t,0) for t in range(time_start, time_end+1)]#[(time_start,0)]
                    logging.error('Metrics missing: '+url+query, params)
                    logging.error(r.text)
            except Exception as e:
                logging.exception('Caught an error: %s' % str(e))
            return l
        # split the time span into max 9000s intervals
        list_values = []
        max_span_len = 9000
        span = time_end - time_start
        intervals = span//max_span_len+1
        for i in range(0, intervals):
            time_interval_start = i*9000 + time_start
            time_interval_end = min((i+1)*9000-1, span) + time_start
            print("Fetch metric interval {} to {} = {} s span".format(time_interval_start, time_interval_end, time_interval_end - time_interval_start))
            list_values_interval = fetch_interval(time_interval_start, time_interval_end, step)
            list_values = list_values + list_values_interval
        return list_values
    @staticmethod
    def metricsToDataframe(metric, values):
        df = pd.DataFrame.from_records(values)
        df.columns = ['time [s]', metric['title']]
        df.iloc[0:,0] = df.iloc[0:,0].map(int)
        minimum = df.iloc[0:,0].min()
        df.iloc[0:,0] = df.iloc[0:,0].map(lambda x: x-minimum)
        df = df.set_index(df.columns[0])
        df.iloc[0:,0] = df.iloc[0:,0].map(float)
        return df
    @staticmethod
    def saveMetricsDataframe(filename, df):
        if df is not None:
            logging.debug("saveMetricsDataframe "+filename)
            csv = df.to_csv(index_label=False,index=False)
            # save
            csv_file = open(filename, "w")
            csv_file.write(csv)
            csv_file.close()
    @staticmethod
    def loadMetricsDataframe(filename):
        if os.path.isfile(filename):
            logging.debug("loadMetricsDataframe "+filename)
            df = pd.read_csv(filename)
            return df
        else:
            return None
    def generatePlots(self):
        for q,d in self.benchmarker.protocol['query'].items():
            logging.debug("Hardware metrics for Q"+str(q))
            self.generatePlotForQuery(q)
    def generateLatexForQuery(self, parameter):
        latex = "\\subsubsection{{Hardware Metrics}}\n\\begin{{figure}}[ht]\n\\centering\n"
        query = parameter['queryNumber']
        numPlots = 0
        for m, metric in metrics.metrics.items():
            plotfile = self.benchmarker.path+'/query_'+str(query)+'_metric_'+str(m)+'.png'
            if os.path.isfile(plotfile):
                latex += "    \\hfill\\subfloat["+tools.tex_escape(metric['title']).replace('[','{{[}}').replace(']','{{]}}')+"]{{\\includegraphics[width=0.45\\textwidth,height=0.2\\textheight]{{query_{queryNumber}_metric_"+m+".png}}}}"
                if numPlots % 2:
                    latex += "\\\\\n"
                numPlots = numPlots + 1
        if numPlots > 0:
            latex += "\\caption{{Query {queryNumber}: Server Hardware Metrics}}\\end{{figure}}"
            return latex.format(**parameter)
        else:
            return ""
    @staticmethod
    def fetchMetric(query, metric_code, connection, connectiondata, time_start, time_end, path, container=None, metrics_query_path='metrics'):
        # metrics_query_path = where to find the promql paths in connectiondata (metrics for components managed by bexhoma, metrics_special otherwise)
        #for m, metric in metrics.metrics.items():
        logging.debug("Metric "+metric_code)
        df_all = None
        #for c,t in times["starts"].items():
        if 'monitoring' in connectiondata:
            #self.token = self.benchmarker.dbms[c].connectiondata['monitoring']['grafanatoken']
            #self.url = self.benchmarker.dbms[c].connectiondata['monitoring']['grafanaurl']
            #self.url = connectiondata['monitoring']['prometheus_url']
            url = connectiondata['monitoring']['prometheus_url']
            if connectiondata['active'] and url: #
                logging.debug("Connection "+connection)
                # is there a custom query for this metric and dbms?
                if metrics_query_path in connectiondata['monitoring'] and metric_code in connectiondata['monitoring'][metrics_query_path]:
                    metric = connectiondata['monitoring'][metrics_query_path][metric_code].copy()
                else:
                    metric = metrics.metrics[metric_code].copy()
                #print(metric)
                if container is not None:
                    metric['query'] = metric['query'].replace('container_label_io_kubernetes_container_name="dbms"', 'container_label_io_kubernetes_container_name="{}"'.format(container))
                    metric['query'] = metric['query'].replace('container_label_io_kubernetes_container_name!="dbms"', 'container_label_io_kubernetes_container_name!="{}"'.format(container))
                    metric['query'] = metric['query'].replace('container="dbms"', 'container="{}"'.format(container))
                    metric['query'] = metric['query'].replace('container!="dbms"', 'container!="{}"'.format(container))
                    # open TODO:
                    # container_label_component="worker"
                    # container_label_component="sut"
                # this yields seconds
                # is there a global timeshift
                if 'grafanashift' in connectiondata['monitoring']:
                    time_shift = connectiondata['monitoring']['grafanashift']
                elif 'shift' in connectiondata['monitoring']:
                    time_shift = connectiondata['monitoring']['shift']
                else:
                    time_shift = 0
                time_start = time_start + time_shift
                time_end = time_end + time_shift
                if 'extend' in connectiondata['monitoring']:
                    add_interval = int(connectiondata['monitoring']['extend'])
                else:
                    add_interval = 0
                #add_interval = int(connectiondata['monitoring']['extend'])
                #add_interval = int(connectiondata['monitoring']['grafanaextend'])
                time_start = time_start - add_interval
                time_end = time_end + add_interval
                #print(time_end-time_start)
                csvfile = path+'/query_'+str(query)+'_metric_'+str(metric_code)+'_'+connection+'.csv'
                #print(csvfile)
                if os.path.isfile(csvfile):# and not self.benchmarker.overwrite:
                    logging.debug("Data exists")
                    df = metrics.loadMetricsDataframe(csvfile)
                    df.columns=[connection]
                else:
                    values = metrics.getMetrics(url, metric, time_start, time_end)
                    df = metrics.metricsToDataframe(metric, values)
                    df.columns=[connection]
                    metrics.saveMetricsDataframe(csvfile, df)
                #print(df)
                return df
        df = pd.DataFrame()
        return df
    def fetchMetricPerQuery(self, query):
        times = self.benchmarker.protocol['query'][str(query)]
        for m, metric in metrics.metrics.items():
            intervals = {}
            logging.debug("Metric "+m)
            for c,t in times["starts"].items():
                time_start = int(datetime.timestamp(datetime.strptime(times["starts"][c],'%Y-%m-%d %H:%M:%S.%f')))
                time_end = int(datetime.timestamp(datetime.strptime(times["ends"][c],'%Y-%m-%d %H:%M:%S.%f')))
                #if 'extend' in self.benchmarker.dbms[c].connectiondata['monitoring']:
                #    add_interval = int(self.benchmarker.dbms[c].connectiondata['monitoring']['extend'])
                #else:
                #    add_interval = 0
                #intervals[c] = time_end-time_start #+1# because of ceil()
                logging.debug("Start={} End={}".format(time_start, time_end))
                df = self.fetchMetric(query, m, c, self.benchmarker.dbms[c].connectiondata, time_start, time_end, self.benchmarker.path)
                print(df)
    def DEPRECATED_generatePlotForQuery(self, query):
        times = self.benchmarker.protocol['query'][str(query)]
        for m, metric in metrics.metrics.items():
            intervals = {}
            df_all = None
            logging.debug("Metric "+m)
            for c,t in times["starts"].items():
                time_start = int(datetime.timestamp(datetime.strptime(times["starts"][c],'%Y-%m-%d %H:%M:%S.%f')))
                time_end = int(datetime.timestamp(datetime.strptime(times["ends"][c],'%Y-%m-%d %H:%M:%S.%f')))
                if 'extend' in self.benchmarker.dbms[c].connectiondata['monitoring']:
                    add_interval = int(self.benchmarker.dbms[c].connectiondata['monitoring']['extend'])
                else:
                    add_interval = 0
                #add_interval = int(self.benchmarker.dbms[c].connectiondata['monitoring']['extend'])
                intervals[c] = time_end-time_start #+1# because of ceil()
                df = metrics.fetchMetric(query, m, c, self.benchmarker.dbms[c].connectiondata, time_start, time_end, self.benchmarker.path)
                if df.empty or len(df.index)==1:
                    continue
                if df_all is None:
                    df_all = df
                else:
                    df_all = df_all.merge(df,how='outer', left_index=True,right_index=True)
            if df_all is None:
                continue
            # options
            remove_delay = True
            show_shift_line = False
            show_end_line = True
            show_first_connection_line = False
            # remove connection delay (metrics are collected, but nothing happens here)
            queryObject = tools.query(self.benchmarker.queries[int(query)-1])
            if remove_delay:
                df_all = df_all.iloc[int(queryObject.delay_connect):]
            # anonymize dbms
            df_all.columns = df_all.columns.map(tools.dbms.anonymizer)
            df_all.index.name = "Seconds"
            if add_interval > 0:
                df_all.index = df_all.index.map(mapper=(lambda i: i-add_interval))
                #print(df_all.index)
            #print(df_all)
            title=metric['title']
            # remove zeros to compensate monitoring shifts
            #df_all = clean_dataframe(df_all.T).T
            # plot lines
            ax = df_all.plot(title=title, color=[tools.dbms.dbmscolors.get(x, '#333333') for x in df_all.columns], legend=False)
            ax.set_ylim(bottom=0, top=df_all.max().max()*1.10+0.00001)
            #plt.legend(title="Metric")
            # show start line
            plt.axvline(x=int(queryObject.delay_connect), linestyle="--", color="black")
            # show shift line
            if show_shift_line and time_shift > 0:
                plt.axvline(x=time_shift, linestyle=":", color="black")
            # show first connection
            if show_first_connection_line and queryObject.delay_connect > 0:
                plt.axvline(x=queryObject.delay_connect, linestyle=":", color="black")
            #if add_interval > 0:
            # show end line
            if show_end_line:
                list_of_colors = plt.rcParams['axes.prop_cycle'].by_key()['color']
                i = 0
                for c, end in intervals.items():
                    if self.benchmarker.dbms[c].getName() in df_all.columns:
                        plt.axvline(x=end, linestyle="--", color=tools.dbms.dbmscolors[self.benchmarker.dbms[c].getName()])#list_of_colors[i])
                        i = i + 1
                #plt.axvline(x=len(df_all.index)-2*add_interval-1, linestyle="--", color="black")
            plt.ticklabel_format(useOffset=False)
            plt.savefig(self.benchmarker.path+'/query_'+str(query)+'_metric_'+str(m)+'.png', bbox_inches='tight')
            plt.close()
    def computeAverages(self):
        if metrics.m_avg is not None:
            return metrics.m_avg
        m_n = {}
        m_sum = {}
        # find position of execution timer
        e = [i for i,t in enumerate(self.benchmarker.timers) if t.name=="execution"]
        # list of active queries for timer e[0] = execution
        qs = tools.findSuccessfulQueriesAllDBMS(self.benchmarker, None, self.benchmarker.timers)[e[0]]
        numContribute = 0
        #print(qs)
        for query, protocol in self.benchmarker.protocol['query'].items():
            if int(query)-1 in qs:
                for c,t in protocol["starts"].items():
                    if 'monitoring' in self.benchmarker.dbms[c].connectiondata:
                        #self.token = self.benchmarker.dbms[c].connectiondata['monitoring']['grafanatoken']
                        #self.url = self.benchmarker.dbms[c].connectiondata['monitoring']['grafanaurl']
                        self.url = self.benchmarker.dbms[c].connectiondata['monitoring']['prometheus_url']
                        if self.benchmarker.dbms[c].connectiondata['active'] and self.url: # and self.token
                            numContribute = numContribute + 1
                            if not c in m_n:
                                m_n[c] = {}
                                m_sum[c] = {}
                            for m, metric in metrics.metrics.items():
                                if not m in m_n[c]:
                                    m_n[c][m] = 0
                                    m_sum[c][m] = 0
                                #logging.debug("Connection "+c)
                                if 'extend' in self.benchmarker.dbms[c].connectiondata['monitoring']:
                                    add_interval = int(self.benchmarker.dbms[c].connectiondata['monitoring']['extend'])
                                else:
                                    add_interval = 0
                                #add_interval = int(self.benchmarker.dbms[c].connectiondata['monitoring']['extend'])
                                csvfile = self.benchmarker.path+'/query_'+str(query)+'_metric_'+str(m)+'_'+c+'.csv'
                                if os.path.isfile(csvfile):
                                    #print(csvfile)
                                    logging.debug("Data exists")
                                    df = self.loadMetricsDataframe(csvfile)
                                    #df = pd.read_csv(csvfile)
                                    #print(df)
                                    m_n[c][m] += len(df.index)-add_interval
                                    m_sum[c][m] += float(df.iloc[add_interval:m_n[c][m]+add_interval].sum())
                                    #print(m_n)
        metrics.m_avg = {c:{m:float(m_sum[c][m]/v) if v > 0 else 0 for m,v in a.items()} for c,a in m_n.items()}
        #print(m_n)
        #print(m_sum)
        #print(metrics.m_avg)
        #print(numContribute)
        return metrics.m_avg
    def dfHardwareMetrics(self, numQuery, metric):
        filename = self.benchmarker.path+'/query_'+str(numQuery)+'_metric_'+str(metric)+'.csv'
        #print(filename)
        if os.path.isfile(filename) and not self.benchmarker.overwrite:
            df_all = metrics.loadMetricsDataframe(filename)
        else:
            df_all = None
        if df_all is None:
            dbms_filter = self.benchmarker.protocol['query'][str(numQuery)]["starts"].keys()
            for c in dbms_filter:
                filename = self.benchmarker.path+'/query_'+str(numQuery)+'_metric_'+str(metric)+'_'+c+'.csv'
                df = metrics.loadMetricsDataframe(filename)
                if df is None:
                    continue
                df.columns=[c]
                if df_all is None:
                    df_all = df
                else:
                    df_all = df_all.merge(df, how='outer', left_index=True,right_index=True)
            filename = self.benchmarker.path+'/query_'+str(numQuery)+'_metric_'+str(metric)+'.csv'
            metrics.saveMetricsDataframe(filename, df_all)
        if df_all is None:
            return pd.DataFrame()
        # remove connection delay (metrics are collected, but nothing happens here)
        query = tools.query(self.benchmarker.queries[numQuery-1])
        df_all = df_all.iloc[int(query.delay_connect):]
        #print(df_all)
        # remove extend
        #for c, connection in self.benchmarker.dbms.items():
        #    add_interval = int(connection.connectiondata['monitoring']['grafanaextend'])
        #    #print(add_interval)
        #    #print(c)
        #    #print(df_all[c])
        #    #df_all[c] = list(df_all[c])[add_interval:-add_interval].extend([0]*(2*add_interval))
        # take last extend value
        #df_all = df_all.iloc[add_interval:-add_interval]
        #print(df_all)
        return df_all.T
    def dfHardwareMetricsLoader(self, metric):
        return self.dfHardwareMetricsComponentOriginal(metric, component="loader")
    def dfHardwareMetricsBenchmarker(self, metric):
        return self.dfHardwareMetricsComponentOriginal(metric, component="benchmarker")
    def dfHardwareMetricsDatagenerator(self, metric):
        return self.dfHardwareMetricsComponentOriginal(metric, component="datagenerator")
    def dfHardwareMetricsLoading(self, metric):
        return self.dfHardwareMetricsComponentOriginal(metric, component="loading")
    def dfHardwareMetricsComponentOriginal(self, metric, component="loading"):
        #filename = self.benchmarker.path+'/query_loading_metric_'+str(metric)+'.csv'
        filename = "{path}/query_{component}_metric_{metric}.csv".format(path=self.benchmarker.path, component=component, metric=metric)
        #print(filename)
        if os.path.isfile(filename) and not self.benchmarker.overwrite:
            df_all = metrics.loadMetricsDataframe(filename)
        else:
            df_all = None
        if df_all is None:
            dbms_filter = self.benchmarker.dbms.keys()#self.benchmarker.protocol['query'][str(numQuery)]["starts"].keys()
            for c in dbms_filter:
                # load metrics are fetched before possibly renaming connection
                if 'orig_name' in self.benchmarker.dbms[c].connectiondata:
                    connectionname = self.benchmarker.dbms[c].connectiondata['orig_name']
                else:
                    connectionname = c
                #print(connectionname, df_all)
                if df_all is not None and connectionname in df_all.columns:
                    print("We already have the metrics of this instance {c} as {connectionname}".format(c=c, connectionname=connectionname))
                    continue
                filename_component = "{path}/query_{component}_metric_{metric}_{connectionname}.csv".format(path=self.benchmarker.path, component=component, metric=metric, connectionname=connectionname)
                #filename = self.benchmarker.path+'/query_loading_metric_'+str(metric)+'_'+connectionname+'.csv'
                df = metrics.loadMetricsDataframe(filename_component)
                if df is None:
                    continue
                df.columns=[connectionname]
                if df_all is None:
                    df_all = df
                else:
                    # produces suffixes because of duplicate columns 
                    df_all = df_all.merge(df, how='outer', left_index=True,right_index=True)
            #filename = self.benchmarker.path+'/query_loading_metric_'+str(metric)+'.csv'
            metrics.saveMetricsDataframe(filename, df_all)
        if df_all is None:
            return pd.DataFrame()
        # remove connection delay (metrics are collected, but nothing happens here)
        #query = tools.query(self.benchmarker.queries[numQuery-1])
        #df_all = df_all.iloc[int(query.delay_connect):]
        #print(df_all)
        # remove extend
        #for c, connection in self.benchmarker.dbms.items():
        #    add_interval = int(connection.connectiondata['monitoring']['grafanaextend'])
        #    #print(add_interval)
        #    #print(c)
        #    #print(df_all[c])
        #    #df_all[c] = list(df_all[c])[add_interval:-add_interval].extend([0]*(2*add_interval))
        # take last extend value
        #df_all = df_all.iloc[add_interval:-add_interval]
        #print(df_all)
        #print(df_all)
        return df_all.T
    def dfHardwareMetricsStreaming(self, metric):
        return self.dfHardwareMetricsComponentOriginal(metric, component="stream")
    def dfHardwareMetricsComponent(self, metric, component="stream"):
        filename = "{path}/query_{component}_metric_{metric}.csv".format(path=self.benchmarker.path, component=component, metric=metric)
        #filename = self.benchmarker.path+'/query_stream_metric_'+str(metric)+'.csv'
        #print(filename)
        if os.path.isfile(filename) and not self.benchmarker.overwrite:
            df_all = metrics.loadMetricsDataframe(filename)
        else:
            df_all = None
        if df_all is None:
            dbms_filter = self.benchmarker.dbms.keys()#self.benchmarker.protocol['query'][str(numQuery)]["starts"].keys()
            for c in dbms_filter:
                connectionname = c
                #print(connectionname, df_all)
                filename_component = "{path}/query_{component}_metric_{metric}_{connectionname}.csv".format(path=self.benchmarker.path, component=component, metric=metric, connectionname=connectionname)
                #filename = self.benchmarker.path+'/query_stream_metric_'+str(metric)+'_'+connectionname+'.csv'
                df = metrics.loadMetricsDataframe(filename_component)
                if df is None:
                    continue
                df.columns=[connectionname]
                if df_all is None:
                    df_all = df
                else:
                    df_all = df_all.merge(df, how='outer', left_index=True,right_index=True)
            #filename = self.benchmarker.path+'/query_stream_metric_'+str(metric)+'.csv'
            metrics.saveMetricsDataframe(filename, df_all)
        if df_all is None:
            return pd.DataFrame()
        return df_all.T

def clean_dataframe(dataframe):
    # helps evaluating GPU util
    # removes leading zeros in each row
    # shifts values > 0 to the beginning
    # appends nan to keep numbers of columns the same
    # replaces all 0 by nan
    #print(dataframe)
    for rows in range(len(dataframe.index)):
        s = dataframe.iloc[rows]
        for i,j in s.items():
            if j > 0:
                break
        #print("{}: remove {} leading zero values".format(dataframe.index[rows], i))
        t = s[i:]
        s = t.append(pd.Series([nan for x in range(i)]))
        s = s.reset_index(drop=True)
        dataframe.iloc[rows] = s
    return dataframe.replace(0, nan)
