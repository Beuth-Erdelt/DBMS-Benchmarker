"""
End-to-end test of the scripts Bexhoma runs on a result folder, against SQLite.

Bexhoma prepares one result folder per experiment, runs one benchmarker pod per
connection, each writing into its own subfolder, and then evaluates the folder in
the dashboard pod. This test runs the same chain locally:

1. the result folder gets ``connections.config``, ``queries.config``, one
   ``<connection>.config`` per connection and a ``protocol.json`` with the random
   query parameters, drawn once for all pods (as ``BenchmarkRunner.run_pod()``),
2. ``benchmark.py run -cs -sf <connection> -c <connection> -sid 1`` per connection
   (the flags of Bexhoma's ``benchmarker.sh``),
3. ``metrics.evaluation.py`` combines per-connection monitoring files,
4. ``merge.py`` joins the subfolders, ``benchmark.py read -e yes -scm`` builds
   the evaluation cube,
5. ``evaluate.py`` and the dashboard read the result.

The checks are about the outcome, not about timings: no errors, result sets of
the two pods agree and match SQLite, all pods used the same parameters.

Needs a Java runtime and the SQLite JDBC driver, see test_benchmark_sqlite.py.

Run from the repository root: python -m unittest tests.test_pipeline_sqlite -v
"""
import ast
import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import unittest
import urllib.request
from pprint import pformat

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from test_benchmark_sqlite import (CONNECTIONS, NUM_RUN, NUM_ORDERS, QUERIES, SEGMENTS, REPO,  # noqa: E402
                                   create_database, find_jar, find_java, query_database)

CODE = '1700000000'
# names of the merged connections: one pod per connection, renamed <connection>-1
PODS = [name + '-1' for name in CONNECTIONS]
METRIC = 'total_cpu_util'
METRIC_VALUES = {name: [10 * (i + 1) + k for k in range(5)] for i, name in enumerate(CONNECTIONS)}

PREPARE_PARAMETERS = """
import sys
from dbmsbenchmarker import benchmarker
result_path, code, folder = sys.argv[1:4]
b = benchmarker.benchmarker(result_path=result_path, code=code, batch=True, working='connection')
b.getConfig(connectionfile=folder + '/connections.config', queryfile=folder + '/queries.config')
b.reporterStore.readProtocol()
b.generateAllParameters()
b.reporterStore.writeProtocol()
"""


def run(*args):
    return subprocess.run([sys.executable, *args], cwd=REPO, capture_output=True, text=True, timeout=600)


def free_port():
    with socket.socket() as s:
        s.bind(('127.0.0.1', 0))
        return s.getsockname()[1]


class PipelineSQLiteTest(unittest.TestCase):
    """Runs the chain once, then checks each step's outcome."""

    @classmethod
    def setUpClass(cls):
        jar = find_jar()
        if jar is None or not find_java():
            message = ('needs a Java runtime and the SQLite JDBC driver in tests/jars/ '
                       'or DBMSBENCHMARKER_SQLITE_JAR')
            if os.environ.get('CI'):
                raise RuntimeError(message)
            raise unittest.SkipTest(message)
        cls.base = tempfile.mkdtemp(prefix='dbmsbenchmarker-pipeline-').replace('\\', '/')
        cls.database = cls.base + '/shop.db'
        cls.results = cls.base + '/results'
        cls.folder = cls.results + '/' + CODE
        os.makedirs(cls.folder)
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
            'monitoring': {'metrics': {METRIC: {'query': 'not fetched in this test', 'title': 'CPU'}}},
        } for name in CONNECTIONS]
        cls.write(cls.folder + '/connections.config', pformat(connections))
        cls.write(cls.folder + '/queries.config', pformat(QUERIES))
        # the per-connection files as JSON: the pods rename their connection in the copy
        for connection in connections:
            cls.write(cls.folder + '/' + connection['name'] + '.config', json.dumps([connection], indent=2))
        cls.steps = {}
        cls.steps['parameters'] = run('-c', PREPARE_PARAMETERS, cls.results, CODE, cls.folder)
        for name in CONNECTIONS:
            cls.steps['benchmark ' + name] = run(
                'benchmark.py', 'run', '-b', '-w', 'connection', '-f', cls.folder, '-r', cls.folder,
                '-cs', '-sf', name, '-ms', '1', '-c', name, '-ca', name, '-cf', name + '.config',
                '-rcp', '0', '-sid', '1')
        # monitoring files as metrics.py writes them: one column per connection
        for name, values in METRIC_VALUES.items():
            cls.write('{}/query_stream_metric_{}_{}.csv'.format(cls.folder, METRIC, name),
                      name + '\n' + ''.join('{}\n'.format(v) for v in values))
        cls.steps['metrics.evaluation'] = run('metrics.evaluation.py', '-r', cls.results, '-ct', 'stream', '-e', CODE)
        cls.steps['merge'] = run('merge.py', '-r', cls.results + '/', '-c', CODE)
        cls.steps['read'] = run('benchmark.py', 'read', '-e', 'yes', '-scm', '-r', cls.folder)
        cls.evaluate = None
        if all(p.returncode == 0 for p in cls.steps.values()):
            from dbmsbenchmarker import inspector
            cls.evaluate = inspector.inspector(cls.results)
            cls.evaluate.load_experiment(CODE)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.base, ignore_errors=True)

    @staticmethod
    def write(filename, text):
        with open(filename, 'w') as file:
            file.write(text)

    def setUp(self):
        for step, process in self.steps.items():
            if process.returncode != 0:
                self.fail('step "{}" failed (exit code {}):\n{}\n{}'.format(
                    step, process.returncode, process.stdout[-3000:], process.stderr[-3000:]))

    def protocol(self, subfolder=''):
        with open(os.path.join(self.folder, subfolder, 'protocol.json')) as file:
            return json.load(file)

    def test_each_pod_wrote_its_subfolder(self):
        for name, pod in zip(CONNECTIONS, PODS):
            with self.subTest(pod=pod):
                self.assertIn('Experiment {} stream 1 for {} has been finished'.format(CODE, name),
                              self.steps['benchmark ' + name].stdout)
                files = os.listdir(os.path.join(self.folder, pod))
                for query in NUM_RUN:
                    self.assertIn('query_{}_resultset_complete_{}.pickle'.format(query, pod), files)

    def test_pods_used_the_same_parameters(self):
        expected = self.protocol()['query']['3']['parameter']
        self.assertEqual(len(expected), NUM_RUN[3])
        for pod in PODS:
            with self.subTest(pod=pod):
                self.assertEqual(self.protocol(pod)['query']['3']['parameter'], expected)

    def test_merged_connections(self):
        self.assertEqual(sorted(self.evaluate.get_experiment_list_connections()), PODS)
        with open(self.folder + '/connections.config') as file:
            merged = ast.literal_eval(file.read())
        self.assertEqual(sorted((c['name'], c['orig_name']) for c in merged), list(zip(PODS, CONNECTIONS)))

    def test_version_recorded_per_pod(self):
        import dbmsbenchmarker
        total = self.protocol()['total']
        self.assertEqual({pod: total[pod]['dbmsbenchmarker_version'] for pod in PODS},
                         {pod: dbmsbenchmarker.__version__ for pod in PODS})

    def test_no_errors_and_result_sets_agree(self):
        errors = self.evaluate.get_total_errors()
        self.assertFalse(errors.values.any(), msg='\n{}'.format(errors))
        # merge.py compares the result sets of the pods
        warnings = self.evaluate.get_total_warnings()
        self.assertFalse(warnings.values.any(), msg='\n{}'.format(warnings))

    def test_one_measurement_per_run(self):
        for query, num_run in NUM_RUN.items():
            with self.subTest(query=query):
                df = self.evaluate.get_timer(query, 'execution')
                self.assertEqual(sorted(df.index), PODS)
                self.assertEqual(df.shape[1], num_run)
                self.assertTrue((df.values > 0).all(), msg='\n{}'.format(df))

    def test_results_match_database(self):
        self.assertEqual(self.evaluate.get_datastorage_df(1).values.tolist(), [[NUM_ORDERS]])
        expected = dict(query_database(
            self.database,
            'SELECT c.segment, COUNT(*) FROM orders o JOIN customer c ON c.id = o.customer_id GROUP BY c.segment'))
        for run_number, segment in enumerate(p['SEGMENT'] for p in self.protocol()['query']['3']['parameter']):
            with self.subTest(run=run_number, segment=segment):
                self.assertIn(segment, SEGMENTS)
                self.assertEqual(self.evaluate.get_datastorage_df(3, run_number).values.tolist(), [[expected[segment]]])

    def test_monitoring_metrics_combined(self):
        with open('{}/query_stream_metric_{}.csv'.format(self.folder, METRIC)) as file:
            lines = file.read().split()
        self.assertEqual(lines[0], ','.join(CONNECTIONS))
        self.assertEqual([[int(v) for v in line.split(',')] for line in lines[1:]],
                         [list(row) for row in zip(*METRIC_VALUES.values())])

    def test_evaluate_script(self):
        for mode in ['errors', 'warnings', 'resultsets']:
            with self.subTest(mode=mode):
                process = run('evaluate.py', '-r', self.results, '-e', CODE, mode)
                self.assertEqual(process.returncode, 0, msg=process.stderr[-3000:])
                self.assertNotIn('Traceback', process.stderr)
        process = run('evaluate.py', '-r', self.results, '-e', CODE, 'resultsets')
        self.assertIn('===Q2: Revenue per segment===', process.stdout)

    def test_dashboard_serves_layout(self):
        port = free_port()
        dashboard = subprocess.Popen([sys.executable, 'dashboard.py', '-r', self.results], cwd=REPO,
                                     env=dict(os.environ, PORT=str(port)),
                                     stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        try:
            layout = None
            deadline = time.time() + 120
            while layout is None and time.time() < deadline and dashboard.poll() is None:
                try:
                    with urllib.request.urlopen('http://127.0.0.1:{}/_dash-layout'.format(port), timeout=5) as response:
                        layout = response.read().decode()
                except OSError:
                    time.sleep(1)
        finally:
            dashboard.terminate()
            output = dashboard.communicate(timeout=30)[0]
        self.assertIsNotNone(layout, msg=output[-3000:])
        # the experiment is offered for selection
        self.assertIn(CODE, layout)

    def test_merge_without_stream_id(self):
        # a run without -sid stores its query ordering as a list instead of a dict
        folder = self.base + '/no-sid/' + CODE
        shutil.copytree(self.folder, folder)
        for pod in PODS:
            filename = os.path.join(folder, pod, 'protocol.json')
            with open(filename) as file:
                protocol = json.load(file)
            protocol['ordering'] = protocol['ordering']['1']
            with open(filename, 'w') as file:
                json.dump(protocol, file)
        process = run('merge.py', '-r', self.base + '/no-sid/', '-c', CODE)
        self.assertEqual(process.returncode, 0, msg=process.stderr[-3000:])
        with open(folder + '/protocol.json') as file:
            ordering = json.load(file)['ordering']
        self.assertEqual(sorted(ordering), PODS)
        self.assertEqual(ordering[PODS[0]], list(NUM_RUN))


if __name__ == '__main__':
    unittest.main()
