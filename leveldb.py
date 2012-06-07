#!/usr/bin/env python
#
# Copyright (C) 2012 Space Monkey, Inc.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#

"""
    LevelDB Python interface via C-Types.
    http://code.google.com/p/leveldb-py/

    Missing still (but in progress):
      * iterators
      * snapshots
      * batches
      * custom comparators, filter policies, caches

    This isn't exactly the most performant interface to LevelDB, but this
    interface requires nothing more than the leveldb shared object with the C
    api being installed.

    There's a bug with LevelDB 1.5's build script that may make getting this
    to work challenging for you. See:
    http://code.google.com/p/leveldb/issues/detail?id=94
"""

__author__ = "JT Olds"
__email__ = "jt@spacemonkey.com"

import ctypes
import ctypes.util

_ldb = ctypes.CDLL(ctypes.util.find_library('leveldb'))
_libc = ctypes.CDLL(ctypes.util.find_library('c'))

_ldb.leveldb_filterpolicy_create_bloom.argtypes = [ctypes.c_int]
_ldb.leveldb_filterpolicy_create_bloom.restype = ctypes.c_void_p
_ldb.leveldb_filterpolicy_destroy.argtypes = [ctypes.c_void_p]
_ldb.leveldb_cache_create_lru.argtypes = [ctypes.c_size_t]
_ldb.leveldb_cache_create_lru.restype = ctypes.c_void_p
_ldb.leveldb_cache_destroy.argtypes = [ctypes.c_void_p]

_ldb.leveldb_options_create.argtypes = []
_ldb.leveldb_options_create.restype = ctypes.c_void_p
_ldb.leveldb_options_set_filter_policy.argtypes = [ctypes.c_void_p,
        ctypes.c_void_p]
_ldb.leveldb_options_set_create_if_missing.argtypes = [ctypes.c_void_p,
        ctypes.c_ubyte]
_ldb.leveldb_options_set_error_if_exists.argtypes = [ctypes.c_void_p,
        ctypes.c_ubyte]
_ldb.leveldb_options_set_paranoid_checks.argtypes = [ctypes.c_void_p,
        ctypes.c_ubyte]
_ldb.leveldb_options_set_write_buffer_size.argtypes = [ctypes.c_void_p,
        ctypes.c_size_t]
_ldb.leveldb_options_set_max_open_files.argtypes = [ctypes.c_void_p,
        ctypes.c_int]
_ldb.leveldb_options_set_cache.argtypes = [ctypes.c_void_p, ctypes.c_void_p]
_ldb.leveldb_options_set_block_size.argtypes = [ctypes.c_void_p,
        ctypes.c_size_t]
_ldb.leveldb_options_destroy.argtypes = [ctypes.c_void_p]

_ldb.leveldb_open.argtypes = [ctypes.c_void_p, ctypes.c_char_p,
        ctypes.c_void_p]
_ldb.leveldb_close.argtypes = [ctypes.c_void_p]
_ldb.leveldb_put.argtypes = [ctypes.c_void_p, ctypes.c_void_p,
        ctypes.c_char_p, ctypes.c_size_t, ctypes.c_char_p, ctypes.c_size_t,
        ctypes.c_void_p]
_ldb.leveldb_delete.argtypes = [ctypes.c_void_p, ctypes.c_void_p,
        ctypes.c_char_p, ctypes.c_size_t, ctypes.c_void_p]
_ldb.leveldb_get.argtypes = [ctypes.c_void_p, ctypes.c_void_p,
        ctypes.c_char_p, ctypes.c_size_t, ctypes.c_void_p, ctypes.c_void_p]
_ldb.leveldb_get.restype = ctypes.POINTER(ctypes.c_char)

_ldb.leveldb_writeoptions_create.argtypes = []
_ldb.leveldb_writeoptions_create.restype = ctypes.c_void_p
_ldb.leveldb_writeoptions_destroy.argtypes = [ctypes.c_void_p]
_ldb.leveldb_writeoptions_set_sync.argtypes = [ctypes.c_void_p,
        ctypes.c_ubyte]

_ldb.leveldb_readoptions_create.argtypes = []
_ldb.leveldb_readoptions_create.restype = ctypes.c_void_p
_ldb.leveldb_readoptions_destroy.argtypes = [ctypes.c_void_p]
_ldb.leveldb_readoptions_set_verify_checksums.argtypes = [ctypes.c_void_p,
        ctypes.c_ubyte]
_ldb.leveldb_readoptions_set_fill_cache.argtypes = [ctypes.c_void_p,
        ctypes.c_ubyte]

_libc.free.argtypes = [ctypes.c_void_p]


class Error(Exception):
    pass


class DB(object):

    def __init__(self, path, bloom_filter_size=10, create_if_missing=False,
            error_if_exists=False, paranoid_checks=False,
            write_buffer_size=(4*1024*1024), max_open_files=1000,
            block_cache_size=(8*1024*1024), block_size=(4*1024)):

        self._filter_policy = _ldb.leveldb_filterpolicy_create_bloom(
                bloom_filter_size)
        self._cache = _ldb.leveldb_cache_create_lru(block_cache_size)
        self._db = None
        self._closed = False
        options = _ldb.leveldb_options_create()
        _ldb.leveldb_options_set_filter_policy(options, self._filter_policy)
        _ldb.leveldb_options_set_create_if_missing(options, create_if_missing)
        _ldb.leveldb_options_set_error_if_exists(options, error_if_exists)
        _ldb.leveldb_options_set_paranoid_checks(options, paranoid_checks)
        _ldb.leveldb_options_set_write_buffer_size(options, write_buffer_size)
        _ldb.leveldb_options_set_max_open_files(options, max_open_files)
        _ldb.leveldb_options_set_cache(options, self._cache)
        _ldb.leveldb_options_set_block_size(options, block_size)
        error = ctypes.POINTER(ctypes.c_char)()
        db = _ldb.leveldb_open(options, path, ctypes.byref(error))
        _ldb.leveldb_options_destroy(options)
        self._checkError(error)
        self._db = db

    def _checkError(self, error):
        if bool(error):
            message = ctypes.string_at(error)
            _libc.free(ctypes.cast(error, ctypes.c_void_p))
            raise Error(message)

    def close(self):
        closed, self._closed = self._closed, True
        if not closed:
            if self._db:
                _ldb.leveldb_close(self._db)
            _ldb.leveldb_cache_destroy(self._cache)
            _ldb.leveldb_filterpolicy_destroy(self._filter_policy)

    def __del__(self):
        self.close()

    def put(self, key, val, sync=False):
        error = ctypes.POINTER(ctypes.c_char)()
        options = _ldb.leveldb_writeoptions_create()
        _ldb.leveldb_writeoptions_set_sync(options, sync)
        _ldb.leveldb_put(self._db, options, key, len(key), val, len(val),
                ctypes.byref(error))
        _ldb.leveldb_writeoptions_destroy(options)
        self._checkError(error)

    def delete(self, key, sync=False):
        error = ctypes.POINTER(ctypes.c_char)()
        options = _ldb.leveldb_writeoptions_create()
        _ldb.leveldb_writeoptions_set_sync(options, sync)
        _ldb.leveldb_delete(self._db, options, key, len(key),
                ctypes.byref(error))
        _ldb.leveldb_writeoptions_destroy(options)
        self._checkError(error)

    def get(self, key, verify_checksums=False, fill_cache=True):
        error = ctypes.POINTER(ctypes.c_char)()
        options = _ldb.leveldb_readoptions_create()
        _ldb.leveldb_readoptions_set_verify_checksums(options,
                verify_checksums)
        _ldb.leveldb_readoptions_set_fill_cache(options, fill_cache)
        size = ctypes.c_size_t(0)
        val_p = _ldb.leveldb_get(self._db, options, key, len(key),
                ctypes.byref(size), ctypes.byref(error))
        if bool(val_p):
            val = ctypes.string_at(val_p, size.value)
            _libc.free(ctypes.cast(val_p, ctypes.c_void_p))
        else:
            val = None
        _ldb.leveldb_readoptions_destroy(options)
        self._checkError(error)
        return val
