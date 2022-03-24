# DBMS

In the following we list some minimal connection information that have been used in the past to connect to DBMS successfully.

Make sure to adjust the

* URL of the server `localhost`
* port of the server, when necessary
* name of the database `database`
* username and passwort `user`/`password`
* path of the (locally existing) JDBC jar file

## MonetDB

```
[
    {
        'name': "MonetDB",
        'info': "This is a demo of MonetDB",
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
        'name': "MySQL",
        'info': "This is a demo of MySQL",
        'active': True,
        'JDBC': {
            'driver': "com.mysql.cj.jdbc.Driver",
            'url': "jdbc:mysql://localhost:3306/database?serverTimezone=Europe/Berlin",
            'auth': ["user", "password"],
            'jar': "jars/mysql-connector-java-8.0.13.jar"
        },
    },
]
```
