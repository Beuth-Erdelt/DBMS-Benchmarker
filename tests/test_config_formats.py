"""
Config files (queries.config, connections.config) may be JSON or a Python literal.

Run from the repository root: python -m unittest tests.test_config_formats -v
"""
import ast
import json
import os
import shutil
import tempfile
import unittest

from dbmsbenchmarker import benchmarker, tools

CONNECTIONS = [{
    'name': 'PostgreSQL',
    'active': True,
    'JDBC': {'driver': 'org.postgresql.Driver', 'url': 'jdbc:postgresql://localhost:5432/DBMSBENCHMARKER_DATABASE',
             'auth': ['user', ''], 'jar': ['jars/postgresql.jar']},
    'monitoring': None,
}]


class ConfigFormatsTest(unittest.TestCase):

    def setUp(self):
        self.folder = tempfile.mkdtemp(prefix='dbmsbenchmarker-config-')

    def tearDown(self):
        shutil.rmtree(self.folder, ignore_errors=True)

    def write(self, name, text):
        filename = os.path.join(self.folder, name)
        with open(filename, 'w') as file:
            file.write(text)
        return filename

    def test_parse_json(self):
        self.assertEqual(tools.parse_config(json.dumps(CONNECTIONS)), CONNECTIONS)

    def test_parse_python_literal(self):
        self.assertEqual(tools.parse_config(repr(CONNECTIONS)), CONNECTIONS)

    def test_parse_python_literal_with_comment(self):
        self.assertEqual(tools.parse_config("{'name': 'TPC-H'} # workload\n"), {'name': 'TPC-H'})

    def test_copy_json_stores_python_literal(self):
        source = self.write('connections.json', json.dumps(CONNECTIONS, indent=2))
        target = os.path.join(self.folder, 'connections.config')
        benchmarker.copy_config_atomic(source, target)
        with open(target) as file:
            text = file.read()
        self.assertEqual(ast.literal_eval(text), CONNECTIONS)
        # the pods rename their connection by replacing this text
        self.assertIn("'name': 'PostgreSQL'", text)

    def test_copy_python_literal_unchanged(self):
        text = "# connections\n" + repr(CONNECTIONS)
        source = self.write('connections.py.config', text)
        target = os.path.join(self.folder, 'connections.config')
        benchmarker.copy_config_atomic(source, target)
        with open(target) as file:
            self.assertEqual(file.read(), text)


if __name__ == '__main__':
    unittest.main()
