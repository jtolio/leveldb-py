from distutils.core import setup
from distutils.extension import Extension
from Cython.Build import cythonize


setup(
    name='leveldbpy',
    version='0.1',
    description='Python bindings for LevelDB',
    author='JT Olds',
    author_email='jt@spacemonkey.com',
    url="http://leveldb-py.googlecode.com/",
    packages = ["leveldb"],
    ext_modules = cythonize([Extension(
            "leveldb.cython_impl", ["leveldb/cython_impl.pyx"],
            libraries=["leveldb"])]))
