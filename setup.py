from setuptools import setup

setup(
   name='geesedb',
   version='0.0.1',
   description='Graph Engine for Exploration and Search over Evolving DataBases',
   author='Chris Kamphuis',
   author_email='chris@cs.ru.nl',
   packages=['geesedb'],
   install_requires=['duckdb'],
)
