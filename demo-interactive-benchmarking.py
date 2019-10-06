"""
:Date: 2018-02-03
:Version: 0.1
:Authors: Patrick Erdelt

Small demo for manual configuration and usage of DBMS Benchmarker in an interactive python shell.

"""
from dbmsbenchmarker import *
import logging

resultfolder = "tmp/results"
configfolder = "tmp"
logging.basicConfig(level=logging.DEBUG)

dbms = benchmarker.benchmarker(result_path=resultfolder, numProcesses=8)
dbms.getConfig(configfolder)
dbms.reporter.append(benchmarker.reporter.pickler(dbms))
dbms.reporter.append(benchmarker.reporter.dataframer(dbms))
dbms.reporter.append(benchmarker.reporter.barer(dbms))
dbms.reporter.append(benchmarker.reporter.ploter(dbms))
dbms.reporter.append(benchmarker.reporter.boxploter(dbms))
dbms.reporter.append(benchmarker.reporter.latexer(dbms, "pagePerQuery"))
dbms.runBenchmarks()
#dbms.continueBenchmarks(overwrite = False)
#dbms.readBenchmarks()

