"""
End-to-end test: run a benchmark against SQLite and evaluate it with the inspector.

The test builds a small SQLite database with known contents, runs ``benchmark.py``
on it through the SQLite JDBC driver (as Bexhoma does in its benchmarker pods), and
then checks with ``inspector.inspector`` that

* every query ran on both connections without errors, and the result sets of the
  two connections agree,
* the timers hold one positive measurement per run,
* the stored result sets match what SQLite itself computes, so the database was
  really read (and not empty),
* randomized parameters were applied per run.

Needs a Java runtime (``JAVA_HOME`` or ``java`` on the path) and the SQLite JDBC
driver: ``tests/jars/sqlite-jdbc-*.jar`` or the path in ``DBMSBENCHMARKER_SQLITE_JAR``.
Without them the test is skipped locally and fails in CI (``CI`` is set).

Run from the repository root: python -m unittest tests.test_benchmark_sqlite -v
"""
import glob
import math
import os
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pprint import pformat

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

SEGMENTS = {
    # segment: customer ids; uneven on purpose, so every segment has its own order count
    'AUTOMOBILE': range(1, 6),
    'BUILDING': range(6, 16),
    'FURNITURE': range(16, 31),
}
NUM_CUSTOMERS = 30
NUM_ORDERS = 300
CONNECTIONS = ['SQLite-A', 'SQLite-B']
NUM_RUN = {1: 3, 2: 3, 3: 6}

DATATRANSFER = {'active': True, 'sorted': True, 'compare': 'result', 'store': 'dataframe', 'precision': 0}
QUERIES = {
    'name': 'SQLite test workload',
    'connectionmanagement': {'timeout': 60, 'numProcesses': 1, 'runsPerConnection': 0, 'singleConnection': False},
    'queries': [
        {
            'title': 'Count orders',
            'query': 'SELECT COUNT(*) AS c FROM orders',
            'numRun': NUM_RUN[1],
            'timer': {'connection': {'active': True}, 'datatransfer': DATATRANSFER},
        },
        {
            'title': 'Revenue per segment',
            'query': 'SELECT c.segment, SUM(o.amount) AS revenue FROM orders o '
                     'JOIN customer c ON c.id = o.customer_id GROUP BY c.segment',
            'numRun': NUM_RUN[2],
            'timer': {'connection': {'active': True}, 'datatransfer': DATATRANSFER},
        },
        {
            'title': 'Orders of a random segment',
            'query': "SELECT COUNT(*) AS c FROM orders o JOIN customer c ON c.id = o.customer_id "
                     "WHERE c.segment = '{SEGMENT}'",
            'numRun': NUM_RUN[3],
            'parameter': {'SEGMENT': {'type': 'list', 'size': 1, 'range': list(SEGMENTS)}},
            'timer': {'connection': {'active': True}, 'datatransfer': DATATRANSFER},
        },
    ],
}


def find_jar():
    """The SQLite JDBC driver, or None."""
    jar = os.environ.get('DBMSBENCHMARKER_SQLITE_JAR')
    if jar:
        return jar if os.path.isfile(jar) else None
    jars = sorted(glob.glob(os.path.join(REPO, 'tests', 'jars', 'sqlite-jdbc-*.jar')))
    return jars[-1] if jars else None


def find_java():
    """True if JPype can find a Java runtime."""
    java_home = os.environ.get('JAVA_HOME')
    if java_home and os.path.isdir(java_home):
        return True
    return shutil.which('java') is not None


def create_database(path):
    """A small shop database with fixed contents."""
    db = sqlite3.connect(path)
    db.execute('CREATE TABLE customer (id INTEGER PRIMARY KEY, segment TEXT NOT NULL)')
    db.execute('CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER NOT NULL, amount INTEGER NOT NULL)')
    db.executemany('INSERT INTO customer VALUES (?, ?)',
                   [(i, segment) for segment, ids in SEGMENTS.items() for i in ids])
    db.executemany('INSERT INTO orders VALUES (?, ?, ?)',
                   [(i, 1 + i % NUM_CUSTOMERS, i % 97) for i in range(1, NUM_ORDERS + 1)])
    db.commit()
    db.close()


def query_database(path, sql):
    db = sqlite3.connect(path)
    try:
        return db.execute(sql).fetchall()
    finally:
        db.close()


class BenchmarkSQLiteTest(unittest.TestCase):
    """Runs the benchmark once, then checks the result folder with the inspector."""

    @classmethod
    def setUpClass(cls):
        jar = find_jar()
        if jar is None or not find_java():
            message = ('needs a Java runtime and the SQLite JDBC driver in tests/jars/ '
                       'or DBMSBENCHMARKER_SQLITE_JAR')
            if os.environ.get('CI'):
                raise RuntimeError(message)
            raise unittest.SkipTest(message)
        cls.folder = tempfile.mkdtemp(prefix='dbmsbenchmarker-test-')
        cls.database = os.path.join(cls.folder, 'shop.db').replace('\\', '/')
        config_folder = os.path.join(cls.folder, 'config')
        cls.result_folder = os.path.join(cls.folder, 'results')
        os.makedirs(config_folder)
        os.makedirs(cls.result_folder)
        create_database(cls.database)
        connections = [{
            'name': name,
            'active': True,
            'JDBC': {
                'driver': 'org.sqlite.JDBC',
                'url': 'jdbc:sqlite:' + cls.database,
                'auth': ['', ''],
                'jar': [jar.replace('\\', '/')],
            },
        } for name in CONNECTIONS]
        with open(os.path.join(config_folder, 'connections.config'), 'w') as file:
            file.write(pformat(connections))
        with open(os.path.join(config_folder, 'queries.config'), 'w') as file:
            file.write(pformat(QUERIES))
        cls.process = subprocess.run(
            [sys.executable, 'benchmark.py', 'run', '-f', config_folder, '-r', cls.result_folder, '-e', 'yes', '-b'],
            cwd=REPO, capture_output=True, text=True, timeout=600)
        codes = os.listdir(cls.result_folder)
        cls.code = codes[0] if len(codes) == 1 else None
        cls.evaluate = None
        if cls.process.returncode == 0 and cls.code is not None:
            from dbmsbenchmarker import inspector
            cls.evaluate = inspector.inspector(cls.result_folder)
            cls.evaluate.load_experiment(cls.code)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.folder, ignore_errors=True)

    def setUp(self):
        if self.evaluate is None:
            self.fail('benchmark run failed (exit code {}), result folders {}:\n{}\n{}'.format(
                self.process.returncode, os.listdir(self.result_folder), self.process.stdout[-3000:], self.process.stderr[-3000:]))

    def test_database_is_not_empty(self):
        self.assertEqual(query_database(self.database, 'SELECT COUNT(*) FROM customer'), [(NUM_CUSTOMERS,)])
        self.assertEqual(query_database(self.database, 'SELECT COUNT(*) FROM orders'), [(NUM_ORDERS,)])

    def test_run_finished_and_wrote_results(self):
        self.assertIn('Experiment {} has been finished'.format(self.code), self.process.stdout)
        files = os.listdir(os.path.join(self.result_folder, self.code))
        for name in ['protocol.json', 'evaluation.json', 'connections.config', 'queries.config']:
            self.assertIn(name, files)
        for query in NUM_RUN:
            self.assertIn('query_{}_execution.csv'.format(query), files)

    def test_queries_and_connections(self):
        self.assertEqual(self.evaluate.get_experiment_list_queries(), list(NUM_RUN))
        self.assertEqual(sorted(self.evaluate.get_experiment_list_connections()), CONNECTIONS)
        self.assertEqual(self.evaluate.get_experiment_workload_properties()['name'], QUERIES['name'])

    def test_no_errors_and_result_sets_agree(self):
        errors = self.evaluate.get_total_errors()
        self.assertFalse(errors.values.any(), msg='\n{}'.format(errors))
        # a warning means the result sets of the two connections differ
        warnings = self.evaluate.get_total_warnings()
        self.assertFalse(warnings.values.any(), msg='\n{}'.format(warnings))

    def test_one_measurement_per_run(self):
        for query, num_run in NUM_RUN.items():
            for timer in ['connection', 'execution', 'datatransfer']:
                with self.subTest(query=query, timer=timer):
                    df = self.evaluate.get_timer(query, timer)
                    self.assertEqual(sorted(df.index), CONNECTIONS)
                    self.assertEqual(df.shape[1], num_run)
                    for value in df.values.flatten():
                        self.assertTrue(math.isfinite(value) and value >= 0, msg='\n{}'.format(df))
                    if timer == 'execution':
                        self.assertTrue((df.values > 0).all(), msg='\n{}'.format(df))
                    if timer == 'connection':
                        # each query opens one connection per DBMS, timed in its first run
                        self.assertTrue((df[0] > 0).all(), msg='\n{}'.format(df))

    def test_statistics(self):
        statistics = self.evaluate.get_aggregated_query_statistics(type='timer', name='execution')
        self.assertEqual(sorted(statistics.index), CONNECTIONS)
        self.assertEqual(statistics.shape[1], len(NUM_RUN))
        self.assertTrue((statistics.values > 0).all(), msg='\n{}'.format(statistics))
        totals = self.evaluate.get_total_times()
        self.assertTrue((totals.values > 0).all(), msg='\n{}'.format(totals))

    def test_count_matches_database(self):
        result = self.evaluate.get_datastorage_df(1)
        self.assertEqual(result.values.tolist(), [[NUM_ORDERS]])

    def test_aggregation_matches_database(self):
        expected = query_database(
            self.database,
            'SELECT c.segment, SUM(o.amount) FROM orders o JOIN customer c ON c.id = o.customer_id '
            'GROUP BY c.segment ORDER BY c.segment')
        self.assertEqual(len(expected), len(SEGMENTS))
        result = self.evaluate.get_datastorage_df(2)
        self.assertEqual(list(result.columns), ['SEGMENT', 'REVENUE'])
        self.assertEqual(sorted((segment, float(revenue)) for segment, revenue in result.values.tolist()),
                         [(segment, float(revenue)) for segment, revenue in expected])

    def test_parameters_are_applied_per_run(self):
        expected = dict(query_database(
            self.database,
            'SELECT c.segment, COUNT(*) FROM orders o JOIN customer c ON c.id = o.customer_id GROUP BY c.segment'))
        self.assertEqual(len(set(expected.values())), len(SEGMENTS), msg='order counts must differ per segment')
        parameters = self.evaluate.get_parameter_df(3)
        self.assertEqual(len(parameters), NUM_RUN[3])
        for run, segment in zip(parameters['numRun'], parameters['SEGMENT']):
            with self.subTest(run=run, segment=segment):
                self.assertIn(segment, SEGMENTS)
                for connection in CONNECTIONS:
                    self.assertIn("'{}'".format(segment), self.evaluate.get_querystring(3, connection, run))
                result = self.evaluate.get_datastorage_df(3, run)
                self.assertEqual(result.values.tolist(), [[expected[segment]]])


if __name__ == '__main__':
    unittest.main()
