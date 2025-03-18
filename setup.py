import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

with open('requirements.txt') as f:
    requirements = f.read().splitlines()

setuptools.setup(
    name="dbmsbenchmarker",
    version="0.14.7",
    author="Patrick Erdelt",
    author_email="perdelt@beuth-hochschule.de",
    description="DBMS-Benchmarker is a Python-based application-level blackbox benchmark tool for Database Management Systems (DBMS). It connects to a given list of DBMS (via JDBC) and runs a given list of parametrized and randomized (SQL) benchmark queries. Evaluations are available via Python interface, in reports and at an interactive multi-dimensional dashboard.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/Beuth-Erdelt/DBMS-Benchmarker",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: GNU Affero General Public License v3",
        "Operating System :: OS Independent",
    ],
    license="GNU Affero General Public License v3",
    python_requires='>=3.6',
    include_package_data=True,
    install_requires=requirements,
    package_dir={'dbmsbenchmarker': 'dbmsbenchmarker'},
    package_data={
        'dbmsbenchmarker': ['dbmsbenchmarker/scripts/assets/*']
    },
    py_modules=['cli'],
    entry_points='''
        [console_scripts]
        dbmsbenchmarker=dbmsbenchmarker.scripts.cli:run_benchmarker
        dbmsdashboard=dbmsbenchmarker.scripts.dashboardcli:startup
        dbmsinspect=dbmsbenchmarker.scripts.inspect:result
    ''',
)
