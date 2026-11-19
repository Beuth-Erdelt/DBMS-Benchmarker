"""
The version is written in three places; Zenodo and citations read the
latter two. They must agree.

Run from the repository root: python -m unittest discover tests
"""
import json
import os
import re
import unittest

from dbmsbenchmarker.__version__ import __version__

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class VersionTest(unittest.TestCase):

    def test_citation_cff(self):
        with open(os.path.join(REPO, 'CITATION.cff'), encoding='utf-8') as f:
            match = re.search(r'^version:\s*"?([^"\s]+)"?\s*$', f.read(), re.MULTILINE)
        self.assertIsNotNone(match, 'CITATION.cff has no version')
        self.assertEqual(match.group(1), __version__)

    def test_codemeta(self):
        with open(os.path.join(REPO, 'codemeta.json'), encoding='utf-8') as f:
            self.assertEqual(json.load(f)['version'], __version__)


if __name__ == '__main__':
    unittest.main()
