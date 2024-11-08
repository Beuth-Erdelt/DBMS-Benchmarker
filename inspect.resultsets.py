"""
    Small demo how to inspect result sets of the Python Package DBMS Benchmarker.
    This prints the first difference in result sets for each query in the latest benchmark in the current folder.
    TODO: Improve usability and flexibility
    Copyright (C) 2020  Patrick Erdelt

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <https://www.gnu.org/licenses/>.
"""
from dbmsbenchmarker import *
from dbmsbenchmarker.scripts import inspect
import pandas as pd
import argparse

#import logging
#logging.basicConfig(level=logging.DEBUG)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='A benchmark tool for RDBMS. It connects to a given list of RDBMS via JDBC and runs a given list benchmark queries. Optionally some reports are generated.')
    parser.add_argument('-r', '--result-folder', help='folder for storing benchmark result files, default is given by timestamp', default="./")
    parser.add_argument('-c', '--code', help='code of experiment', default="")
    parser.add_argument('-q', '--query', help='number of query to inspect', default=None)
    parser.add_argument('-n', '--num-run', help='number of run to inspect', default=None)

    args = parser.parse_args()

    inspect.result()
