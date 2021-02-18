import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

with open('requirements.txt') as f:
    requirements = f.read().splitlines()

setuptools.setup(
    name="dbmsbenchmarker",
    version="0.10.6",
    author="Patrick Erdelt",
    author_email="perdelt@beuth-hochschule.de",
    description="DBMS-Benchmarker is an application-level blackbox benchmark tool for Database Management Systems (DBMS). It aims at easily measuring and evaluation of the performance the user receives even in complex benchmark situations.",
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
    package_data={'dbmsbenchmarker': ['dbmsbenchmarker/latex/*']},
)