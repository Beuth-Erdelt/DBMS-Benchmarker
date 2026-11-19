"""
Sets the version in dbmsbenchmarker/__version__.py, CITATION.cff and codemeta.json.

    python bump_version.py 0.15.11

tests/test_metadata.py checks that the three agree.
"""
import os
import re
import sys

REPO = os.path.dirname(os.path.abspath(__file__))

FILES = {
    'dbmsbenchmarker/__version__.py': (r'^__version__ = "[^"]*"$', '__version__ = "{}"'),
    'CITATION.cff': (r'^version: "[^"]*"$', 'version: "{}"'),
    'codemeta.json': (r'^  "version": "[^"]*",$', '  "version": "{}",'),
}


def main():
    if len(sys.argv) != 2 or not re.fullmatch(r'\d+\.\d+\.\d+', sys.argv[1]):
        sys.exit('usage: python bump_version.py MAJOR.MINOR.PATCH')
    version = sys.argv[1]
    for path, (pattern, line) in FILES.items():
        full = os.path.join(REPO, path)
        with open(full, encoding='utf-8', newline='') as f:
            text = f.read()
        text, n = re.subn(pattern, line.format(version), text, count=1, flags=re.MULTILINE)
        if n != 1:
            sys.exit(f'{path}: version line not found')
        with open(full, 'w', encoding='utf-8', newline='') as f:
            f.write(text)
        print(f'{path}: {version}')


if __name__ == '__main__':
    main()
