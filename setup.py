from setuptools import setup, find_packages

def readme():
    with open("README.md", 'r') as f:
        return f.read()

setup(
    name = "materialsuite_endpoint",
    description = "A REST API for ingesting and distributing material suites",
    long_description = readme(),
    packages = find_packages(
        exclude = [
        ]
    ),
    dependency_links = [
        'https://github.com/uchicago-library/uchicagoldr-premiswork' +
        '/tarball/master#egg=pypremis',
        'https://github.com/bnbalsamo/pypairtree' +
        '/tarball/master#egg=pypairtree'
    ],
    install_requires = [
        'flask>0',
        'flask_restful',
        'pypremis',
        'pypairtree'
    ],
)
