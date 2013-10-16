from __future__ import absolute_import

from .ctypes_impl import CtypesDB
from .memory_impl import MemoryDB
from .cython_impl import CythonDB
from .convenience import Error, WriteBatch

DB = CythonDB
