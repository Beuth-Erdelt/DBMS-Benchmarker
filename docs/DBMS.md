# DBMS

In the following we list some minimal connection information that have been used in the past to connect to DBMS successfully.

Make sure to adjust the

* URL of the server `localhost`
* port of the server, when necessary
* name of the database `database` (and / or `schema`)
* username and passwort `user`/`password`
* path of the (locally existing) JDBC jar file - **you will have to download the jar file from the website of the vendor**



    {
        'name': "Vertica",
        'info': "This is Vertica on dsm server SF=1",
        'active': True,
        'JDBC': {
            'driver': "com.mysql.cj.jdbc.Driver",
            'url': 'jdbc:vertica://dsm.beuth-hochschule.de:5433/',
            'auth': ["dbadmin", ""],
            'jar': "jars/vertica-jdbc-11.1.0-0.jar"
        },
    },




## Citus Data

https://www.citusdata.com/

JDBC driver: https://jdbc.postgresql.org/

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

## Clickhouse

https://clickhouse.com/

JDBC driver: https://github.com/ClickHouse/clickhouse-jdbc

```
[
    {
        'name': 'Clickhouse',
        'info': 'This is a demo of Clickhouse',
        'active': True,
        'JDBC': {
            'driver': 'ru.yandex.clickhouse.ClickHouseDriver',
            'url': 'jdbc:clickhouse://localhost:8123/database',
            'auth': ['user', 'password'],
            'jar': ['clickhouse-jdbc-0.2.4.jar', 'commons-codec-1.9.jar', 'commons-logging-1.2.jar', 'guava-19.0.jar', 'httpclient-4.5.2.jar', 'httpcore-4.4.4.jar', 'httpmime-4.5.2.jar', 'jackson-annotations-2.7.0.jar', 'jackson-core-2.7.3.jar', 'jackson-databind-2.7.3.jar', 'jaxb-api-2.3.0.jar', 'lz4-1.3.0.jar', 'slf4j-api-1.7.21.jar'],
        }
    },
]
```

## Exasol

https://www.exasol.com/de/

JDBC driver: https://docs.exasol.com/db/latest/connect_exasol/drivers/jdbc.htm

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
            'jar': 'jars/exajdbc.jar'
        }
    },
]
```

## HEAVY.AI

### OmniSci

https://www.heavy.ai/

JDBC driver: https://search.maven.org/artifact/com.omnisci/omnisci-jdbc/5.10.0/jar

```
[
    {
        'name': 'OmniSci',
        'info': 'This is a demo of OmniSci',
        'active': True,
        'JDBC': {
            'driver': 'com.omnisci.jdbc.OmniSciDriver',
            'url': 'jdbc:omnisci:localhost:6274:omnisci',
            'auth': ['user', 'password'],
            'jar': ['omnisci-jdbc-5.5.0.jar', 'libthrift-0.13.0.jar', 'commons-codec-1.9.jar', 'commons-logging-1.2.jar', 'guava-19.0.jar', 'httpclient-4.5.2.jar', 'httpcore-4.4.4.jar', 'httpmime-4.5.2.jar', 'jackson-annotations-2.7.0.jar', 'jackson-core-2.7.3.jar', 'jackson-databind-2.7.3.jar', 'jaxb-api-2.3.0.jar', 'lz4-1.3.0.jar', 'slf4j-api-1.7.21.jar']
        }
    },
]
```

## IBM DB2

https://www.ibm.com/products/db2-database

JDBC driver: https://www.ibm.com/support/pages/db2-jdbc-driver-versions-and-downloads

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

## MariaDB

https://mariadb.com/

JDBC driver: https://mariadb.com/kb/en/about-mariadb-connector-j/

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

## MariaDB Columnstore

https://mariadb.com/kb/en/mariadb-columnstore/

JDBC driver: https://mariadb.com/kb/en/about-mariadb-connector-j/

```
[
    {
        'name': 'MariaDB Columnstore',
        'info': 'This is a demo of MariaDB Columnstore',
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

https://www.monetdb.org/

JDBC driver: https://www.monetdb.org/downloads/Java/

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

## MS SQL Server

https://www.microsoft.com/en-us/sql-server

JDBC driver: https://github.com/microsoft/mssql-jdbc

```
[
    {
        'name': 'MS SQL Server',
        'info': 'This is a demo of MS SQL Server',
        'active': True,
        'JDBC': {
            'driver': 'com.microsoft.sqlserver.jdbc.SQLServerDriver',
            'url': 'jdbc:sqlserver://localhost:1433;databaseName=database',
            'auth': ['user', 'password'],
            'jar': 'mssql-jdbc-8.2.2.jre8.jar'
        }
    },
]
```

## MySQL

https://www.mysql.com/

JDBC driver: https://dev.mysql.com/downloads/connector/j/

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

## Oracle DB

https://www.oracle.com/database/technologies/

JDBC driver: https://www.oracle.com/database/technologies/appdev/jdbc.html

```
[
    {
        'name': 'Oracle DB',
        'info': 'This is a demo of Oracle DB',
        'active': True,
        'JDBC': {
            'driver': 'oracle.jdbc.driver.OracleDriver',
            'url': 'jdbc:oracle:thin:@{localhost:1521',
            'auth': ['user', 'password'],
            'jar': 'ojdbc8.jar'
        }
    },
]
```

## PostgreSQL

https://www.postgresql.org/

JDBC driver: https://jdbc.postgresql.org/

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

## SAP HANA

https://www.sap.com/products/hana.html

JDBC driver: https://mvnrepository.com/artifact/com.sap.cloud.db.jdbc/ngdbc

```
[
    {
        'name': 'SAP HANA',
        'info': 'This is a demo of SAP HANA',
        'active': True,
        'JDBC': {
            'driver': 'com.sap.db.jdbc.Driver',
            'url': 'jdbc:sap://localhost:39041/HXE?currentSchema=schema',
            'auth': ['user', 'password'],
            'jar': 'jars/ngdbc-2.7.7.jar'
        }
    },
]
```

## SingleStore

https://www.singlestore.com/

JDBC driver: https://mariadb.com/kb/en/about-mariadb-connector-j/

```
[
    {
        'name': 'SingleStore',
        'info': 'This is a demo of SingleStore',
        'active': True,
        'JDBC': {
            'driver': 'org.mariadb.jdbc.Driver',
            'url': 'jdbc:mariadb://localhost:3306/database',
            'auth': ['user', 'password'],
            'jar': 'mariadb-java-client-2.4.0.jar'
        }
    },
]
```

### MemSQL

https://www.singlestore.com/

JDBC driver: https://mariadb.com/kb/en/about-mariadb-connector-j/

```
[
    {
        'name': 'MemSQL',
        'info': 'This is a demo of MemSQL',
        'active': True,
        'JDBC': {
            'driver': 'org.mariadb.jdbc.Driver',
            'url': 'jdbc:mariadb://localhost:3306/database',
            'auth': ['user', 'password'],
            'jar': 'mariadb-java-client-2.4.0.jar'
        }
    },
]
```

## Vertica

https://www.vertica.com/

JDBC driver: https://www.vertica.com/download/vertica/client-drivers/

```
[
    {
        'name': 'Vertica',
        'info': 'This is a demo of Vertica',
        'active': True,
        'JDBC': {
            'driver': "com.vertica.jdbc.Driver",
            'url': 'jdbc:vertica://localhost:5433/database',
            'auth': ["user", "password"],
            'jar': "jars/vertica-jdbc-11.1.0-0.jar"
        },
    },
]
```

