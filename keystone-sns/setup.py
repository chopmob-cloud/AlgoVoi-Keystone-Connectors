from setuptools import setup
from Cython.Build import cythonize
setup(ext_modules=cythonize("algovoi_keystone_sns/adapter.pyx", build_dir="build", compiler_directives={"language_level": "3", "annotation_typing": False}))
