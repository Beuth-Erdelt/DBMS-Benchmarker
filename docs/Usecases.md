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

# Example Runs

## Run benchmarks

`python3 benchmark.py run -f test` generates a folder containing result files: csv of benchmarks per query.
The example uses `test/connections.config` and `test/queries.config` as config files.

Example: This produces a folder containing
```
connections.config
queries.config
protocol.json
query_1_connection.csv
query_1_execution.csv
query_1_transfer.csv
query_2_connection.csv
query_2_execution.csv
query_2_transfer.csv
query_3_connection.csv
query_3_execution.csv
query_3_transfer.csv
```
where
- `connections.config` is a copy of the input file
- `queries.config` is a copy of the input file
- `protocol.json`: JSON file containing error messages (up to one per query and connection), durations (per query) and retried data (per query)
- `query_n_connection.csv`: CSV containing times (columns) for each dbms (rows) for query n - duration of establishing JDBC connection
- `query_n_execution.csv`: CSV containing times (columns) for each dbms (rows) for query n - duration of execution
- `query_n_transfer.csv`: CSV containing times (columns) for each dbms (rows) for query n - duration of data transfer

## Run benchmarks and generate evaluations

`python3 benchmark.py run -e yes -f test` is the same as above, and additionally generates evaluation cube files.

```
evaluation.dict
evaluation.json
```
These can be inspected comfortably using the dashboard or the Python API.

## Read stored benchmarks

`python3 benchmark.py read  -r 12345` reads files from folder `12345`containing result files and shows summaries of the results.       

## Generate evaluation of stored benchmarks

`python3 benchmark.py read -r 12345 -e yes` reads files from folder `12345`  containing result files, and generates evaluation cubes.
The example uses `12345/connections.config` and `12345/queries.config` as config files.

## Continue benchmarks

`python3 benchmark.py continue -r 12345 -e yes` reads files from folder `12345` containing result files, continues to perform possibly missing benchmarks and generates evaluation cubes.
This is useful if a run had to be stopped. It continues automatically at the first missing query.
It can be restricted to specific queries or connections using `-q` and `c` resp.
The example uses `12345/connections.config` and `12345/queries.config` as config files.

### Continue benchmarks for more queries
You would go to a result folder, say `12345`, and add queries to the query file.
`python3 benchmark.py continue -r 12345 -g yes` then reads files from folder `12345` and continue benchmarking the new (missing) queries.

**Do not remove existing queries, since results are mapped to queries via their number (position). Use `active` instead.**

### Continue benchmarks for more connections
You would go to a result folder, say `12345`, and add connections to the connection file.
`python3 benchmark.py continue -r 12345 -g yes` then reads files from folder `12345` and continue benchmarking the new (missing) connections.

**Do not remove existing connections, since their results would not make any sense anymore. Use `active` instead.**

## Rerun benchmarks

`python3 benchmark.py run -r 12345 -e yes` reads files from folder `12345` containing result files, performs benchmarks again and generates evaluation cubes.
It also performs benchmarks of missing queries.
It can be restricted to specific queries or connections using `-q` and `c` resp.
The example uses `12345/connections.config` and `12345/queries.config` as config files.

### Rerun benchmarks for one query

`python3 benchmark.py run -r 12345 -e yes -q 5` reads files from folder `12345`containing result files, performs benchmarks again and generates evaluation cubes.
The example uses `12345/connections.config` and `12345/queries.config` as config files.
In this example, query number 5 is benchmarked (again) in any case.

### Rerun benchmarks for one connection

`python3 benchmark.py run -r 12345 -g yes -c MySQL` reads files from folder `12345`containing result files, performs benchmarks again and generates evaluation cubes.
The example uses `12345/connections.config` and `12345/queries.config` as config files.
In this example, the connection named MySQL is benchmarked (again) in any case.

