# Evaluation

After an experiment has finished, the results can be evaluated
* with an interactive [dashboard](Dashboard.html)
* in a Latex report containing most of the results
* with an interactive [inspection module](Inspection.html)

There is an *evaluator class*, which collects most of the (numerical) evaluations and provides them as an **evaluation dict**.

* `numWarmup`: Number of runs of this query for warmup (first `n` queries not counting into statistics), between 0 and `numRun`. This makes sure data is hot and caching is in effect.
* `numCooldown`: Number of runs of this query for cooldown (last `n` queries not counting into statistics), between 0 and `numRun`. This helps sorting out faster executions when the number of parallel clients decreases near the end of a batch.

## Featured Evaluations

Predefined evaluations are
* [Global Metrics](#global-metrics)
  * [average position](#average-ranking)
  * [latency and throughput](#latency-and-throughput)
  * [ingestion](#time-of-ingest-per-dbms)
  * [hardware metrics](#hardware-metrics)
  * [host metrics](#host-metrics)
* [Drill-Down Timers](#drill-down-timers)
  * [relative position](#relative-ranking-based-on-times)
  * [average times](#average-times)
* [Slices of Timers](#slice-timers)
  * [heatmap of factors](#heatmap-of-factors)
* [Drill-Down Queries](#drill-down-queries)
  * [total times](#total-times)
  * [normalized total times](#normalized-total-times)
  * [latencies](#latencies)
  * [throughputs](#throughputs)
  * [sizes of result sets](#sizes-of-result-sets)
  * [errors](#errors)
  * [warnings](#warnings)
* [Slices of Queries](#slice-queries)
  * [latency and throughput](#latency-and-throughput-per-query)
  * [hardware metrics](#hardware-metrics-per-query)
  * [timers](#timers-per-query)
* [Slices of Queries and Timers](#slice-queries-and-timers)
  * [statistics](#statistics-table) - measures of tendency and dispersion, sensitive and insensitive to outliers
  * [plots](#plot-of-values) of times
  * [box plots](#boxplot-of-values) of times
* summarizing and exhaustive latex reports containing [further data](#further-data) like
  * precision and identity checks of [result sets](#comparison)
  * [error messages](#all-errors)
  * [warnings](#all-warnings)
  * [benchmark times](#all-benchmark-times)
  * [experiment workflow](#bexhoma-workflow)
  * [initialization scripts](#initialization-scripts)
* an interactive [inspection tool](#inspector)
* a Latex report containing most of these

### Informations about DBMS
This evaluation is available in the evaluation dict and in the latex reports.

<p align="center">
<img src="https://raw.githubusercontent.com/Beuth-Erdelt/DBMS-Benchmarker/master/docs/dbms.png" width="480">
</p>

The user has to provide in a [config file](Options.html#connection-file)
* a unique name (**connectionname**)
* JDBC connection information

If a monitoring interface is provided, [hardware metrics](Concept.html#monitoring-hardware-metrics) are collected and aggregated.
We may further provide describing information for reporting.

#### Throughput and Latency

The abbreviations mean
```
lat_r = Latency of runs (mean time) [ms]
lat_s = Latency of session (mean time) [ms]
tps_r1 = Throughput of runs (number of runs / total time) [Hz]
tps_r2 = Throughput of runs (number of parallel clients / cleaned mean time) [Hz]
tps_s1 = Throughput of sessions (number of runs / length of sessions / total time) [Hz]
tps_s2 = Throughput of sessions (number of parallel clients / cleaned mean time) [Hz]
tph_r2 = Throughput of runs (tps_r2 * 3600) [pH]
```

The metrics of index 2 are based on the assumption that the number of clients equals the size of the queues. To check this, there are another metrics:
```
qs_r = Queue size of runs (tps_r1 * lat_r * 1000)
qs_s = Queue size of sessions (tps_s1 * lat_s * 1000)
```

**Note** that the total times include some overhead like spawning a pool of subprocesses, so these metrics are also a measurement of overhead.



### Global Metrics

#### Latency and Throughput
This evaluation is available as dataframes, in the evaluation dict and as png files.

<p align="center">
<img src="https://raw.githubusercontent.com/Beuth-Erdelt/DBMS-Benchmarker/master/docs/relative-tps-lat.png" width="640">
</p>

For each query, latency and throughput is computed per DBMS.
This chart shows the geometric mean over all queries and per DBMS.
Only successful queries and DBMS not producing any error are considered there.

#### Average Ranking
This evaluation is available as dataframes, in the evaluation dict and as png files.

<p align="center">
<img src="https://raw.githubusercontent.com/Beuth-Erdelt/DBMS-Benchmarker/master/docs/ranking.png" width="480">
</p>

We compute a ranking of DBMS for each query based on the sum of times, from fastest to slowest.
Unsuccessful DBMS are considered last place.
The chart shows the average ranking per DBMS.

#### Time of Ingest per DBMS
This evaluation is available as dataframes, in the evaluation dict and as png files.

<p align="center">
<img src="https://raw.githubusercontent.com/Beuth-Erdelt/DBMS-Benchmarker/master/docs/total_barh_ingest.png" width="480">
</p>

This is part of the informations provided by the user.
The tool does not measure time of ingest explicitly.

#### Hardware Metrics

The chart shows the metrics obtained from monitoring.
Values are computed as arithmetic mean across benchmarking time.
Only successful queries and DBMS not producing any error are considered.

<p align="center">
<img src="https://raw.githubusercontent.com/Beuth-Erdelt/DBMS-Benchmarker/master/docs/monitoring-metrics.png" width="640">
</p>

#### Host Metrics

The chart shows the metrics obtained from inside docker containers.
The host information is provided in the [config file](#connection-file).
Here, cost is based on the total time.

<p align="center">
<img src="https://raw.githubusercontent.com/Beuth-Erdelt/DBMS-Benchmarker/master/docs/host-metrics.png" width="640">
</p>

### Drill-Down Timers

#### Relative Ranking based on Times
This evaluation is available as dataframes, in the evaluation dict and as png files.

<p align="center">
<img src="https://raw.githubusercontent.com/Beuth-Erdelt/DBMS-Benchmarker/master/docs/relative.png" width="480">
</p>

For each query and timer, the best DBMS is considered as gold standard = 100%. Based on their times, the other DBMS obtain a relative ranking factor.
Only successful queries and DBMS not producing any error are considered.
The chart shows the geometric mean of factors per DBMS.

#### Average Times
This evaluation is available as dataframes, in the evaluation dict and as png files.

<p align="center">
<img src="https://raw.githubusercontent.com/Beuth-Erdelt/DBMS-Benchmarker/master/docs/sum_of_times.png" width="480">
</p>

This is based on the mean times of all benchmark test runs.
Measurements start before each benchmark run and stop after the same benchmark run has been finished. The average value is computed per query.
Parallel benchmark runs should not slow down in an ideal situation.
Only successful queries and DBMS not producing any error are considered.
The chart shows the average of query times based on mean values per DBMS and per timer.

**Note** that the mean of mean values (here) is in general not the same as mean of all runs (different queries may have different number of runs).

### Slice Timers

#### Heatmap of Factors
This evaluation is available as dataframes, in the evaluation dict and as png files.

<p align="center">
<img src="https://raw.githubusercontent.com/Beuth-Erdelt/DBMS-Benchmarker/master/docs/heatmap-timer.png" width="480">
</p>

The relative ranking can be refined to see the contribution of each query.
The chart shows the factor of the corresponding timer per query and DBMS.
All active queries and DBMS are considered.


### Drill-Down Queries

#### Total Times
This evaluation is available as dataframes, in the evaluation dict and as png files.

<p align="center">
<img src="https://raw.githubusercontent.com/Beuth-Erdelt/DBMS-Benchmarker/master/docs/total_times.png" width="480">
</p>

This is based on the times each DBMS is queried in total. Measurement starts before first benchmark run and stops after the last benchmark run has been finished. Parallel benchmarks should speed up the total time in an ideal situation.
Only successful queries and DBMS not producing any error are considered.
Note this also includes the time needed for sorting and storing result sets etc.
The chart shows the total query time per DBMS and query.

#### Normalized Total Times
This evaluation is available in the evaluation dict and as png files.

<p align="center">
<img src="https://raw.githubusercontent.com/Beuth-Erdelt/DBMS-Benchmarker/master/docs/total_times-norm.png" width="480">
</p>

The chart shows total times per query, normalized to the average total time of that query.
Only successful queries and DBMS not producing any error are considered.
This is also available as a heatmap.

<p align="center">
<img src="https://raw.githubusercontent.com/Beuth-Erdelt/DBMS-Benchmarker/master/docs/total_times-heatmap.png" width="480">
</p>

#### Throughputs

This evaluation is available in the evaluation dict and as png files.

<p align="center">
<img src="https://raw.githubusercontent.com/Beuth-Erdelt/DBMS-Benchmarker/master/docs/tps-heatmap.png" width="480">
</p>

For each query, latency and throughput is computed per DBMS.
The chart shows tps_r2.
Only successful queries and DBMS not producing any error are considered there.

#### Latencies

This evaluation is available in the evaluation dict and as png files.

<p align="center">
<img src="https://raw.githubusercontent.com/Beuth-Erdelt/DBMS-Benchmarker/master/docs/lat-heatmap.png" width="480">
</p>

For each query, latency and throughput is computed per DBMS.
The chart shows lat_r.
Only successful queries and DBMS not producing any error are considered there.

#### Sizes of Result Sets

This evaluation is available in the evaluation dict and as png files.

<p align="center">
<img src="https://raw.githubusercontent.com/Beuth-Erdelt/DBMS-Benchmarker/master/docs/resultsets-heatmap.png" width="480">
</p>

For each query, the size of received data per DBMS is stored.
The chart shows the size of result sets per DBMS and per timer.
Sizes are normalized to minimum per query.
All active queries and DBMS are considered.

#### Errors

This evaluation is available in the evaluation dict and as png files.

<p align="center">
<img src="https://raw.githubusercontent.com/Beuth-Erdelt/DBMS-Benchmarker/master/docs/errors-heatmap.png" width="480">
</p>

The chart shows per DBMS and per timer, if an error has occured.
All active queries and DBMS are considered.

#### Warnings

This evaluation is available in the evaluation dict and as png files.

<p align="center">
<img src="https://raw.githubusercontent.com/Beuth-Erdelt/DBMS-Benchmarker/master/docs/warnings-heatmap.png" width="480">
</p>

The chart shows per DBMS and per timer, if a warning has occured.
All active queries and DBMS are considered.

### Slice Queries

#### Latency and Throughput per Query
This evaluation is available as dataframes, in the evaluation dict and as png files.

<p align="center">
<img src="https://raw.githubusercontent.com/Beuth-Erdelt/DBMS-Benchmarker/master/docs/tps-lat.png" width="640">
</p>

For each query, latency and throughput is computed per DBMS.
This is available as dataframes, in the evaluation dict and as png files per query.
Only successful queries and DBMS not producing any error are considered there.

#### Hardware Metrics per Query
These metrics are available as png files and csv files.

<p align="center">
<img src="https://raw.githubusercontent.com/Beuth-Erdelt/DBMS-Benchmarker/master/docs/hardware-metrics.png" width="640">
</p>

These metrics are collected from a Prometheus / Grafana stack.
This expects time-synchronized servers.

#### Timers Per Query
These plots are available as png files.

<p align="center">
<img src="https://raw.githubusercontent.com/Beuth-Erdelt/DBMS-Benchmarker/master/docs/bar.png" width="480">
</p>

This is based on the sum of times of all single benchmark test runs.
These charts show the average of times per DBMS based on mean value.
Warmup and cooldown are not included.
If data transfer or connection time is also benchmarked, the chart is stacked.
The bars are ordered ascending.

### Slice Queries and Timers

#### Statistics Table
These tables are available as dataframes and in the evaluation dict.

<p align="center">
<img src="https://raw.githubusercontent.com/Beuth-Erdelt/DBMS-Benchmarker/master/docs/table.png" width="640">
</p>

These tables show [statistics](Concept.html#aggregation-functions) about benchmarking time during the various runs per DBMS as a table.
Warmup and cooldown are not included.
This is for inspection of stability.
A factor column is included.
This is computed as the multiple of the minimum of the mean of benchmark times per DBMS.
The DBMS are ordered ascending by factor.

#### Plot of Values
These plots are available as png files.

<p align="center">
<img src="https://raw.githubusercontent.com/Beuth-Erdelt/DBMS-Benchmarker/master/docs/plot.png" width="640">
</p>

These plots show the variation of benchmarking time during the various runs per DBMS as a plot.
Warmup and cooldown are included and marked as such.
This is for inspection of time dependence.

**Note** this is only reliable for non-parallel runs.

#### Boxplot of Values
These plots are available as png files.

<p align="center">
<img src="https://raw.githubusercontent.com/Beuth-Erdelt/DBMS-Benchmarker/master/docs/boxplot.png" width="640">
</p>

These plots show the variation of benchmarking time during the various runs per DBMS as a boxplot.
Warmup, cooldown and zero (missing) values are not included.
This is for inspection of variation and outliers.

#### Histogram of Values
These plots are available as png files.

<p align="center">
<img src="https://raw.githubusercontent.com/Beuth-Erdelt/DBMS-Benchmarker/master/docs/histogram.png" width="640">
</p>

These plots show the variation of benchmarking time during the various runs per DBMS as a histogram.
The number of bins equals the minimum number of result times.
Warmup, cooldown and zero (missing) values are not included.
This is for inspection of the distribution of times.

### Further Data

#### Result Sets per Query
This evaluation is available as dataframes and csv files.

The result set (sorted values, hashed or pure size) of the first run of each DBMS can be saved per query.
This is for comparison and inspection. 

#### All Benchmark Times
This evaluation is available as dataframes, in the evaluation dict and as csv files.

The benchmark times of all runs of each DBMS can be saved per query.
This is for comparison and inspection. 

#### All Errors
This evaluation is available as dicts.

The errors that may have occured are saved for each DBMS and per query.
The error messages are fetched from Python exceptions thrown during a benchmark run.
This is for inspection of problems.

#### All Warnings
This evaluation is available as dicts.

The warnings that may have occured are saved for each DBMS and per query.
The warning messages are generated if comparison of result sets detects any difference.
This is for inspection of problems.

#### Initialization Scripts

If the result folder contains init scripts, they will be included in the latex report.

#### Bexhoma Workflow

If the result folder contains the configuration of a [bexhoma](https://github.com/Beuth-Erdelt/Benchmark-Experiment-Host-Manager) workflow, it will be included in the latex report.

