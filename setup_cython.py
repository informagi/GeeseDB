from setuptools import setup, Extension
from Cython.Build import cythonize


setup(
    name='Test app',
    ext_modules=cythonize("geesedb/index/cython/test.pyx", libraries=["cymem", "spacy"]),
    zip_safe=False,
)
