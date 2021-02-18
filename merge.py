"""
    Command line interface for merging partial results of the Python Package DBMS Benchmarker
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
import pandas as pd
from os.path import isdir, isfile, join
from os import listdir, stat
from dbmsbenchmarker import *
import json
import logging
import argparse
import time

logging.basicConfig(level=logging.ERROR)

if __name__ == '__main__':
	description = """Merge partial subfolder of a result folder
	"""
	# argparse
	parser = argparse.ArgumentParser(description=description)
	parser.add_argument('-r', '--result-folder', help='folder for storing benchmark result files, default is given by timestamp', default=None)
	parser.add_argument('-c', '--code', help='folder for storing benchmark result files, default is given by timestamp', default=None)
	args = parser.parse_args()
	result_path = args.result_folder
	code = str(args.code)
	tools.merge_partial_results(result_path, code)




exit()

