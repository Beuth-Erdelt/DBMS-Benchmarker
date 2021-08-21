
## Overview

This documentation contains
* an example of how to perform a [TPC-H-like Benchmark](docs/Example-TPC-H.md) from a command line
* a list of the [key features](#key-features)
* an example of the [basic usage](#basic-usage) in Python
* an illustration of the [concepts](docs/Concept.md)
* an illustration of the [evaluations](docs/Evaluations.md)
* a description of the [options and configurations](docs/Options.md)
* more extensive [examples](docs/Usage.md) of using the [cli tool](docs/Options.md#command-line-options-and-configuration)
* some [use-cases](docs/Usecases.md#use-cases) and [test scenarios](docs/Usecases.md#scenarios)
* examples of how to use the interactive [inspector](docs/Inspection.md)
* examples of how to use the interactive [dashboard](docs/Dashboard.md)


In Python we basically use the benchmarker as follows:
```
from dbmsbenchmarker import *

# tell the benchmarker where to find the config files
configfolder = "./config"
# tell the benchmarker where to put results
resultfolder = "/results"

# get a benchmarker object
dbms = benchmarker.benchmarker(result_path=resultfolder)
dbms.getConfig(configfolder)

# tell the benchmarker which fixed evaluations we want to have (line plot and box plot per query)
dbms.reporter.append(benchmarker.reporter.ploter(dbms))
dbms.reporter.append(benchmarker.reporter.boxploter(dbms))

# start benchmarking
dbms.runBenchmarks()

# print collected errors
dbms.printErrors()

# get unique code of this experiment
code = dbms.code

# generate inspection object
evaluate = inspector.inspector(resultfolder)

# load this experiment into inspector
evaluate.load_experiment(code)

# get latency of run (measures and statistics) of first query
df_measure, df_statistics = evaluate.get_measures_and_statistics(1, type='latency', name='run')
```
There also is a [command line interface](docs/Options.md#command-line-options-and-configuration) for running benchmarks and generation of reports.
