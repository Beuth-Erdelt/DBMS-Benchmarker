# DBMS

In the following we list some minimal connection information that have been used in the past to connect to DBMS successfully.

Make sure to adjust the

* URL of the server `localhost`
* port of the server, when necessary
* name of the database `database` (and / or `schema`)
* username and passwort `user`/`password`
* path of the (locally existing) JDBC jar file - you will have to download the file from the website of the vendor



## DB2

```
[
    {
        'name': 'DB2',
        'info': 'This is a demo of DB2',
        'active': True,
        'JDBC': {
            'driver': 'com.ibm.db2.jcc.DB2Driver',
            'url': 'jdbc:db2://localhost:50000/database:currentSchema=schema',
            'auth': ['user', 'password'],
            'jar': 'jars/db2jcc4.jar'
        }
    },
]
```

## Exasol

```
[
    {
        'name': 'Exasol',
        'info': 'This is a demo of Exasol',
        'active': True,
        'JDBC': {
            'driver': 'com.exasol.jdbc.EXADriver',
            'url': 'jdbc:exa:localhost:8888;schema=schema',
            'auth': ['user', 'password'],
            'jar': 'jars/db2jcc4.jar'
        }
    },
]
```

## Hyperscale (Citus)

```
[
    {
        'name': 'Citus',
        'info': 'This is a demo of Citus',
        'active': True,
        'JDBC': {
            'driver': 'org.postgresql.Driver',
            'url': 'jdbc:postgresql://localhost:5432/database',
            'auth': ['user', 'password'],
            'jar': 'jars/postgresql-42.2.5.jar'
        }
    },
]
```

## MariaDB

```
[
    {
        'name': 'MariaDB',
        'info': 'This is a demo of MariaDB',
        'active': True,
        'JDBC': {
            'driver': 'org.mariadb.jdbc.Driver',
            'url': 'jdbc:mariadb://localhost:3306/database',
            'auth': ['user', 'password'],
            'jar': 'jars/mariadb-java-client-2.4.0.jar'
        }
    },
]
```

## MonetDB

```
[
    {
        'name': 'MonetDB',
        'info': 'This is a demo of MonetDB',
        'active': True,
        'JDBC': {
            'driver': 'nl.cwi.monetdb.jdbc.MonetDriver',
            'url': 'jdbc:monetdb://localhost:50000/database?so_timeout=10000',
            'auth': ['user', 'password'],
            'jar': 'jars/monetdb-jdbc-2.29.jar',
        }
    },
]
```

## MySQL

```
[
    {
        'name': 'MySQL',
        'info': 'This is a demo of MySQL',
        'active': True,
        'JDBC': {
            'driver': 'com.mysql.cj.jdbc.Driver',
            'url': 'jdbc:mysql://localhost:3306/database?serverTimezone=Europe/Berlin',
            'auth': ['user', 'password'],
            'jar': 'jars/mysql-connector-java-8.0.13.jar'
        },
    },
]
```

## PostgreSQL

```
[
    {
        'name': 'PostgreSQL',
        'info': 'This is a demo of PostgreSQL',
        'active': True,
        'JDBC': {
            'driver': 'org.postgresql.Driver',
            'url': 'jdbc:postgresql://localhost:5432/database',
            'auth': ['user', 'password'],
            'jar': 'jars/postgresql-42.2.5.jar'
        }
    },
]
```

