from setuptools import setup, find_packages

setup(
    name='geesedb',
    version='0.0.1',
    description='Graph Engine for Exploration and Search over Evolving DataBases',
    author='Chris Kamphuis',
    author_email='chris@cs.ru.nl',
    url='https://github.com/informagi/GeeseDB',
    install_requires=['duckdb', 'numpy', 'pandas'],
    packages=find_packages(),
    include_package_data=True,
    package_data={'': ['qrels.*', 'topics.*']}
)
