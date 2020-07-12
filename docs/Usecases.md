# Use Cases

[Use Cases](#use-cases) may be
* [Benchmark 1 Query in 1 DBMS](#benchmark-1-query-in-1-dbms)
* [Compare 2 Queries in 1 DBMS](#compare-2-queries-in-1-dbms)
* [Compare 2 Databases in 1 DBMS](#compare-2-databases-in-1-dbms)
* [Compare 1 Query in 2 DBMS](#compare-1-query-in-2-dbms)  
and combinations like compare n queries in m DBMS.
* [Benchmarking DBMS Configurations](#benchmarking-dbms-configurations)

[Scenarios](#scenarios) may be
* [Many Users / Few, Complex Queries](#many-users--few-complex-queries)
* [Few Users / Several simple Queries](#few-users--several-simple-queries)
* [Updated Database](#updated-database)

## Benchmark 1 Query in 1 DBMS

We want to measure how long it takes to run one query in a DBMS.

The following performs a counting query 10 times against one DBMS.
A report in latex is generated containing a plot of the 10 runs.
It also contains statistics, which ignore the first 5 runs as warmups.
The statistics are shown in a table, as boxplots, plots and bar charts.
The script benchmarks the execution of the query and also the transfer of data.
The result (the number of rows in table test) is stored and should be the same for each run.

`connections.config`:
```
[
  {
    'name': "MySQL",
    'version': "CE 8.0.13",
    'info': "This uses engine innodb",
    'active': True,
    'JDBC': {
      'driver': "com.mysql.cj.jdbc.Driver",
      'url': "jdbc:mysql://localhost:3306/database",
      'auth': ["username", "password"],
      'jar': "mysql-connector-java-8.0.13.jar"
    },
  },
]
```

`queries.config`:
```
{
  'name': 'A counting query',
  'queries':
  [
    {
      'title': "Count all rows in test",
      'query': "SELECT COUNT(*) FROM test",
      `numWarmup`: 5,
      'numRun': 10,
      'timer':
      {
        'datatransfer':
        {
          'active': True,
          'compare': 'result'
        },
      }
    },
  ]
}
```

## Compare 2 Queries in 1 DBMS

We want to compare run times of two queries in a DBMS.

The following performs a query 10 times against two DBMS each.
This helps comparing the relevance of position of ordering in the execution plan in this case.  
A report in latex is generated containing a plot of the 10 runs.
It also contains statistics, which ignore the first 5 runs as warmups.
The statistics are shown in a table, as boxplots, plots and bar charts.
The script benchmarks the execution of the query and also the transfer of data.
The result (some rows of table test in a certain order) is stored and should be the same for each run.
**Beware** that storing result may take a lot of RAM and disk space!

`connections.config`:
```
[
  {
    'name': "MySQL-1",
    'version': "CE 8.0.13",
    'info': "This uses engine innodb",
    'active': True,
    'JDBC': {
      'driver': "com.mysql.cj.jdbc.Driver",
      'url': "jdbc:mysql://localhost:3306/database",
      'auth': ["username", "password"],
      'jar': "mysql-connector-java-8.0.13.jar"
    },
  },
  {
    'name': "MySQL-2",
    'version': "CE 8.0.13",
    'info': "This uses engine innodb",
    'active': True,
    'JDBC': {
      'driver': "com.mysql.cj.jdbc.Driver",
      'url': "jdbc:mysql://localhost:3306/database",
      'auth': ["username", "password"],
      'jar': "mysql-connector-java-8.0.13.jar"
    },
  },
]
```

`queries.config`:
```
{
  'name': 'An ordering query',
  'queries':
  [
    {
      'title': "Retrieve rows in test in a certain order",
      'DBMS': {
        'MySQL-1': "SELECT * FROM (SELECT * FROM test WHERE a IS TRUE) tmp ORDER BY b",
        'MySQL-2': "SELECT * FROM (SELECT * FROM test ORDER BY b) tmp WHERE a IS TRUE",
      },
      `numWarmup`: 5,
      'numRun': 10,
      'timer':
      {
        'datatransfer':
        {
          'active': True,
          'compare': 'result'
        },
      }
    },
  ]
}
```

## Compare 2 Databases in 1 DBMS

We want to compare run times of two databases in a DBMS.
An application may be having the same tables with different indices and data types to measure influence.

The following performs a query 10 times against two databases in a DBMS each.
This helps comparing the relevance of table structure in this case.
Suppose we have a table test in database database and in database2 resp.  
A report in latex is generated containing a plot of the 10 runs.
It also contains statistics, which ignore the first 5 runs as warmups.
The statistics are shown in a table, as boxplots, plots and bar charts.
The script benchmarks the execution of the query and also the transfer of data.
The result (the number of rows in table test) is stored and should be the same for each run.

`connections.config`:
```
[
  {
    'name': "MySQL",
    'version': "CE 8.0.13",
    'info': "This uses engine innodb",
    'active': True,
    'JDBC': {
      'driver': "com.mysql.cj.jdbc.Driver",
      'url': "jdbc:mysql://localhost:3306/database",
      'auth': ["username", "password"],
      'jar': "mysql-connector-java-8.0.13.jar"
    },
  },
  {
    'name': "MySQL-2",
    'version': "CE 8.0.13",
    'info': "This uses engine myisam",
    'active': True,
    'JDBC': {
      'driver': "com.mysql.cj.jdbc.Driver",
      'url': "jdbc:mysql://localhost:3306/database2",
      'auth': ["username", "password"],
      'jar': "mysql-connector-java-8.0.13.jar"
    },
  },
]
```

`queries.config`:
```
{
  'name': 'A counting query',
  'queries':
  [
    {
      'title': "Count all rows in test",
      'query': "SELECT COUNT(*) FROM test",
      `numWarmup`: 5,
      'numRun': 10,
      'timer':
      {
        'datatransfer':
        {
          'active': True,
          'compare': 'result'
        },
      }
    },
  ]
}
```

## Compare 1 Query in 2 DBMS

We want to compare run times of two DBMS.
An application may be having the same tables in different DBMS and want to find out which one is faster.

The following performs a query 10 times against two DBMS each.
This helps comparing the power of the two DBMS, MySQL and PostgreSQL in this case.
Suppose we have a table test in both DBMS.  
A report in latex is generated containing a plot of the 10 runs.
It also contains statistics, which ignore the first 5 runs as warmups.
The statistics are shown in a table, as boxplots, plots and bar charts.
The script benchmarks the execution of the query and also the transfer of data.
The result (the number of rows in table test) is stored and should be the same for each run.

`connections.config`:
```
[
  {
    'name': "MySQL",
    'version': "CE 8.0.13",
    'info': "This uses engine innodb",
    'active': True,
    'JDBC': {
      'driver': "com.mysql.cj.jdbc.Driver",
      'url': "jdbc:mysql://localhost:3306/database",
      'auth': ["username", "password"],
      'jar': "mysql-connector-java-8.0.13.jar"
    },
  },
  {
    'name': "PostgreSQL",
    'version': "v11",
    'info': "This uses standard config"
    'active': True,
    'JDBC': {
      'driver': "org.postgresql.Driver",
      'url': "jdbc:postgresql://localhost:5432/database",
      'auth': ["username", "password"],
      'jar': "postgresql-42.2.5.jar"
    },
  },
]
```

`queries.config`:
```
{
  'name': 'A counting query',
  'queries':
  [
    {
      'title': "Count all rows in test",
      'query': "SELECT COUNT(*) FROM test",
      `numWarmup`: 5,
      'numRun': 10,
      'timer':
      {
        'datatransfer':
        {
          'active': True,
          'compare': 'result'
        },
      }
    },
  ]
}
```

## Benchmarking DBMS Configurations

Setup
* a query config file in `hpo/queries.config`
* a connection config file `hpo/connections.config`, which contains just an empty list `[]`
* a local PostgreSQL instance with open JDBC at `localhost:5432`

```
from dbmsbenchmarker import *

resultfolder = "tmp/results"
configfolder = "hpo"
code = None

# template for connection
template = {
  'active': True,
  'version': 'v11.4',
  'alias': '',
  'JDBC': {
    'driver': "org.postgresql.Driver",
    'auth': ["postgres", ""],
    'url': 'jdbc:postgresql://localhost:5432/postgres',
    'jar': '~/JDBC/postgresql-42.2.5.jar'
  }
}

# setup PostgreSQL instance with specific config
...

# give the config a unique name
connection = "setup A"

# generate new connection config
c = template.copy()
c['name'] = connection

# if this is not the first benchmark:
if code is not None:
  resultfolder += '/'+str(code)

# generate benchmarker object for this specific config
benchmarks = benchmarker.benchmarker(
    fixedConnection=connection,
    result_path=resultfolder,
    batch=True,
    working='connection'
)
benchmarks.getConfig(configfolder)

# append new connection
benchmarks.connections.append(c)
benchmarks.dbms[c['name']] = tools.dbms(c, False)
filename = benchmarks.path+'/connections.config'
with open(filename, 'w') as f:
  f.write(str(benchmarks.connections))

# run or continue benchmarks
if code is not None:
  benchmarks.continueBenchmarks(overwrite = True)
else:
  benchmarks.runBenchmarks()

# store code (unique id) of benchmark for further usage
code = benchmarks.code

# now go back to: setup PostgreSQL instance with specific config

```

# Scenarios

## Many Users / Few, Complex Queries

Excerpt from `connections.config`:
```
'connectionmanagement': {
  'timeout': 600,
  'numProcesses': 20,
  'runsPerConnection': 1
},
```
That is we allow 20 parallel clients, which connect to the DBMS host to run 1 single query each.  
Note the host of the benchmarking tool must be capable of 20 parallel processes.

Excerpt from `queries.config`:
```
{
  'title': "Pricing Summary Report (TPC-H Q1)",
  'query': """select
    l_returnflag,
    l_linestatus,
    cast(sum(l_quantity) as int) as sum_qty,
    sum(l_extendedprice) as sum_base_price,
    sum(l_extendedprice*(1-l_discount)) as sum_disc_price,
    sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge,
    avg(l_quantity) as avg_qty,
    avg(l_extendedprice) as avg_price,
    avg(l_discount) as avg_disc,
    count(*) as count_order
    from
    lineitem
    where
    l_shipdate <= date '1998-12-01' - interval '{DELTA}'  day
    group by
    l_returnflag,
    l_linestatus
    order by
    l_returnflag,
    l_linestatus
    limit 10000000""",
  'parameter': {
    'DELTA': {
      'type': "integer",
      'range': [60,99]
    },
  },
  'active': True,
  'numWarmup': 0,
  'numCooldown': 0,
  'numRun': 20,
  'timer':
  {
    'datatransfer':
    {
      'active': True,
      'sorted': True,
      'compare': 'hash',
      'store': 'dataframe',
      'precision': 0,
    },
    'connection':
    {
      'active': True,
    }
  }
},
```
That is each simulated user runs the (randomized) TPC-H query number 1.
The result sets will be truncated to no decimals, sorted and compared by their hash values.
The result set of the first run will be stored to disk as a pickled pandas dataframe.
The time for connection, execution and data transfer will be measured.

## Few Users / Several simple Queries

Excerpt from `connections.config`:
```
'connectionmanagement': {
  'timeout': 600,
  'numProcesses': 1,
  'runsPerConnection': 5
},
```
That is we allow only one client at a time, which connects to the DBMS host to run 5 single queries.  

Excerpt from `queries.config`:
```
{
  'title': "Count rows in nation",
  'query': "SELECT COUNT(*) c FROM nation",
  'active': True,
  'numWarmup': 5,
  'numCooldown': 5,
  'numRun': 20,
  'timer':
  {
    'datatransfer':
    {
      'active': True,
      'sorted': True,
      'compare': 'result',
      'store': 'dataframe',
      'precision': 4,
    },
    'connection':
    {
      'active': True,
    }
  }
},
```
That is each simulated user counts the number of rows in table nations (five times per connection). We want to have 20 counts in total, so the simulated user (re)connects four times one after the other.
The first 5 and the last 5 of the 20 runs are not taken into account for evaluation.
The result sets will be truncated to 4 decimals, sorted and compared.
The result set of the first run will be stored to disk as a pickled pandas dataframe.
The time for connection, execution and data transfer will be measured.

## Updated Database

We want to compute a sum, update some value and compute the sum again.
This will take place 10 times as a sequence one after the other.

Excerpt from `queries.config`:
```
{
  'title': "Sum of facts",
  'query': "SELECT SUM(fact) s FROM facts",
  'active': False,
  'numRun': 1,
},
{
  'title': "Update Facts",
  'query': "UPDATE facts SET fact={FACT} WHERE id={ID}",
  'active': False,
  'numRun': 10,
  'parameter': {
    'FACT': {
      'type': "float",
      'range': [0.05, 20.00]
    },
    'ID': {
      'type': "integer",
      'range': [1,1000]
    },
  },
{
  'title': "Sequence of compute/update/compute",
  'queryList': [1,2,1]
  'active': True,
  'numRun': 30,
  'connectionmanagement': {
    'timeout': 600,
    'numProcesses': 1,
    'runsPerConnection': 3
  }
}
```


