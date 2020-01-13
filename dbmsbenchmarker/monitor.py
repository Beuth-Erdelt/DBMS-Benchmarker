import requests
import pandas as pd
from datetime import datetime
import matplotlib
import matplotlib.pyplot as plt
import csv
import os.path
import logging
from dbmsbenchmarker import benchmarker, tools
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
    }
    m_avg = None
    def __init__(self, benchmarks):
        self.step = 1
        self.benchmarker = benchmarks
    def getMetrics(self, metric,time_start, time_end, step=1):
        query = 'query_range?query='+metric['query']+'&start='+str(time_start)+'&end='+str(time_end)+'&step='+str(self.step)
        headers = {'Authorization': self.token}
        l = [(time_start,0)]
        #return l
        #print(self.url+query)
        try:
            r = requests.post(self.url+query, headers=headers)
            #print(r.json)
            if len(r.json()['data']['result']) > 0:
                l = r.json()['data']['result'][0]['values']
            else:
                l = [(time_start,0)]
        except Exception as e:
            logging.exception('Caught an error: %s' % str(e))
        return l
    def metricsToDataframe(self, metric, values):
        df = pd.DataFrame.from_records(values)
        df.columns = ['time [s]', metric['title']]
        df.iloc[0:,0] = df.iloc[0:,0].map(int)
        minimum = df.iloc[0:,0].min()
        df.iloc[0:,0] = df.iloc[0:,0].map(lambda x: x-minimum)
        df = df.set_index(df.columns[0])
        df.iloc[0:,0] = df.iloc[0:,0].map(float)
        return df
    def saveMetricsDataframe(self, filename, df):
        csv = df.to_csv(index_label=False,index=False)
        # save
        csv_file = open(filename, "w")
        csv_file.write(csv)
        csv_file.close()
    def loadMetricsDataframe(self, filename):
        df = pd.read_csv(filename)
        return df
    def generatePlots(self):
        for q,d in self.benchmarker.protocol['query'].items():
            print("Hardware metrics for Q"+str(q))
            self.generatePlotForQuery(q)
    def generateLatexForQuery(self, parameter):
        latex = "\\newpage\\subsubsection{{Hardware Metrics}}\n\\begin{{figure}}[ht]\n\\centering\n"
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
    def generatePlotForQuery(self, query):
        intervals = {}
        times = self.benchmarker.protocol['query'][str(query)]
        for m, metric in metrics.metrics.items():
            logging.debug("Metric "+m)
            df_all = None
            for c,t in times["starts"].items():
                self.token = self.benchmarker.dbms[c].connectiondata['monitoring']['grafanatoken']
                self.url = self.benchmarker.dbms[c].connectiondata['monitoring']['grafanaurl']
                if self.benchmarker.dbms[c].connectiondata['active'] and self.token and self.url:
                    logging.debug("Connection "+c)
                    # is there a custom query for this metric and dbms?
                    if 'metrics' in self.benchmarker.dbms[c].connectiondata['monitoring'] and m in self.benchmarker.dbms[c].connectiondata['monitoring']['metrics']:
                        metric = self.benchmarker.dbms[c].connectiondata['monitoring']['metrics'][m].copy()
                    #print(metric)
                    # this yields seconds
                    time_start = int(datetime.timestamp(datetime.strptime(times["starts"][c],'%Y-%m-%d %H:%M:%S.%f')))
                    time_end = int(datetime.timestamp(datetime.strptime(times["ends"][c],'%Y-%m-%d %H:%M:%S.%f')))
                    intervals[c] = time_end-time_start #+1# because of ceil()
                    add_interval = int(self.benchmarker.dbms[c].connectiondata['monitoring']['grafanaextend'])
                    time_start = time_start - add_interval
                    time_end = time_end + add_interval
                    #print(time_end-time_start)
                    csvfile = self.benchmarker.path+'/query_'+str(query)+'_metric_'+str(m)+'_'+c+'.csv'
                    #print(csvfile)
                    if os.path.isfile(csvfile) and not self.benchmarker.overwrite:
                        logging.debug("Data exists")
                        df = self.loadMetricsDataframe(csvfile)
                        df.columns=[c]
                    else:
                        values = self.getMetrics(metric,time_start, time_end)
                        df = self.metricsToDataframe(metric, values)
                        df.columns=[c]
                        self.saveMetricsDataframe(csvfile, df)
                    #print(df)
                    if df.empty or len(df.index)==1:
                        continue
                    if df_all is None:
                        df_all = df
                    else:
                        df_all = df_all.merge(df,how='outer', left_index=True,right_index=True)
                    #print(df_all)
            if df_all is None:
                continue
            df_all.columns = df_all.columns.map(tools.dbms.anonymizer)
            df_all.index.name = "Seconds"
            if add_interval > 0:
                df_all.index = df_all.index.map(mapper=(lambda i: i-add_interval))
                #print(df_all.index)
            #print(df_all)
            title=metric['title']
            ax = df_all.plot(title=title)
            ax.set_ylim(bottom=0, top=df_all.max().max()*1.10)
            #plt.legend(title="Metric")
            if add_interval > 0:
                plt.axvline(x=0, linestyle="--", color="black")
                list_of_colors = plt.rcParams['axes.prop_cycle'].by_key()['color']
                i = 0
                for c,end in intervals.items():
                    if self.benchmarker.dbms[c].getName() in df_all.columns:
                        plt.axvline(x=end, linestyle="--", color=list_of_colors[i])
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
                    self.token = self.benchmarker.dbms[c].connectiondata['monitoring']['grafanatoken']
                    self.url = self.benchmarker.dbms[c].connectiondata['monitoring']['grafanaurl']
                    if self.benchmarker.dbms[c].connectiondata['active'] and self.token and self.url:
                        numContribute = numContribute + 1
                        if not c in m_n:
                            m_n[c] = {}
                            m_sum[c] = {}
                        for m, metric in metrics.metrics.items():
                            if not m in m_n[c]:
                                m_n[c][m] = 0
                                m_sum[c][m] = 0
                            #logging.debug("Connection "+c)
                            add_interval = int(self.benchmarker.dbms[c].connectiondata['monitoring']['grafanaextend'])
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
