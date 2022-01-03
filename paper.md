---
title: 'DBMS-Benchmarker is a Python-based application-level blackbox benchmark tool for Database Management Systems (DBMS)'
tags:
  - Python
  - DBMS
authors:
  - name: Patrick K. Erdelt
    orcid: 0000-0002-3359-2386
    affiliation: 1
affiliations:
 - name: Berliner Hochschule für Technik (BHT)
   index: 1
date: 03 January 2022
bibliography: paper.bib

---

# Summary

DBMS-Benchmarker is a Python-based application-level blackbox benchmark tool for Database Management Systems (DBMS). It aims at reproducible measuring and easy evaluation of the performance the user receives even in complex benchmark situations. It connects to a given list of DBMS (via JDBC) and runs a given list of (SQL) benchmark queries. Queries can be parametrized and randomized. Results and evaluations are available via a Python interface. Optionally some reports are generated. An interactive dashboard assists in multi-dimensional analysis of the results.


## Key Features

DBMS-Benchmarker
* is Python3-based
* connects to all [DBMS](Options.html#connection-file) having a JDBC interface - including GPU-enhanced DBMS
* requires *only* JDBC - no vendor specific supplements are used
* benchmarks arbitrary SQL queries - in all dialects
* allows [planning](Options.html#query-file) of complex test scenarios - to simulate realistic or revealing use cases
* allows easy repetition of benchmarks in varying settings - different hardware, DBMS, DBMS configurations, DB settings etc
* investigates a number of timing aspects - connection, execution, data transfer, in total, per session etc
* investigates a number of other aspects - received result sets, precision, number of clients
* collects hardware metrics from a Grafana server - hardware utilization, energy consumption etc
* helps to [evaluate](Evaluations.html) results - by providing  
  * standard Python data structures
  * predefined evaluations like statistics, plots, Latex reporting
  * an [inspection tool](Inspection.html)
  * an [interactive dashboard](Dashboard.html)

In the end this tool provides metrics that can be analyzed by [aggregation](Concept.html#aggregation-functions) in [multi-dimensions](Concept.html#evaluation), like maximum throughput per DBMS, average CPU utilization per query or geometric mean of run latency per workload.

For more informations, see a [basic example](#basic-usage), take a look at help for a full list of [options](Options.html#command-line-options-and-configuration) or take a look at a [demo report](Report-example-tpch.pdf).

The code uses several Python modules, in particular <a href="https://github.com/baztian/jaydebeapi" target="_blank">jaydebeapi</a> for handling DBMS.
This module has been tested with Brytlyt, Citus, Clickhouse, DB2, Exasol, Kinetica, MariaDB, MariaDB Columnstore, MemSQL, Mariadb, MonetDB, MySQL, OmniSci, Oracle DB, PostgreSQL, SingleStore, SQL Server and SAP HANA.

# Acknowledgements


# References