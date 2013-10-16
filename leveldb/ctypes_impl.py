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

from __future__ import absolute_import

"""
    LevelDB Python interface via C-Types.
    http://code.google.com/p/leveldb-py/

    Missing still (but in progress):
      * custom comparators, filter policies, caches

    This interface requires nothing more than the leveldb shared object with
    the C api being installed.

    Now requires LevelDB 1.6 or newer.

    For most usages, you are likely to only be interested in the "DB" and maybe
    the "WriteBatch" classes for construction. The other classes are helper
    classes that you may end up using as part of those two root classes.

     * DBInterface - This class wraps a LevelDB. Created by either the DB or
            MemoryDB constructors
     * Iterator - this class is created by calls to DBInterface::iterator.
            Supports range requests, seeking, prefix searching, etc
     * WriteBatch - this class is a standalone object. You can perform writes
            and deletes on it, but nothing happens to your database until you
            write the writebatch to the database with DB::write
"""

__author__ = "JT Olds"
__email__ = "jt@spacemonkey.com"

import ctypes
import ctypes.util
import weakref

from .convenience import DBInterface, Error

_ldb = ctypes.CDLL(ctypes.util.find_library('leveldb'))

_ldb.leveldb_filterpolicy_create_bloom.argtypes = [ctypes.c_int]
_ldb.leveldb_filterpolicy_create_bloom.restype = ctypes.c_void_p
_ldb.leveldb_filterpolicy_destroy.argtypes = [ctypes.c_void_p]
_ldb.leveldb_filterpolicy_destroy.restype = None
_ldb.leveldb_cache_create_lru.argtypes = [ctypes.c_size_t]
_ldb.leveldb_cache_create_lru.restype = ctypes.c_void_p
_ldb.leveldb_cache_destroy.argtypes = [ctypes.c_void_p]
_ldb.leveldb_cache_destroy.restype = None

_ldb.leveldb_options_create.argtypes = []
_ldb.leveldb_options_create.restype = ctypes.c_void_p
_ldb.leveldb_options_set_filter_policy.argtypes = [ctypes.c_void_p,
        ctypes.c_void_p]
_ldb.leveldb_options_set_filter_policy.restype = None
_ldb.leveldb_options_set_create_if_missing.argtypes = [ctypes.c_void_p,
        ctypes.c_ubyte]
_ldb.leveldb_options_set_create_if_missing.restype = None
_ldb.leveldb_options_set_error_if_exists.argtypes = [ctypes.c_void_p,
        ctypes.c_ubyte]
_ldb.leveldb_options_set_error_if_exists.restype = None
_ldb.leveldb_options_set_paranoid_checks.argtypes = [ctypes.c_void_p,
        ctypes.c_ubyte]
_ldb.leveldb_options_set_paranoid_checks.restype = None
_ldb.leveldb_options_set_write_buffer_size.argtypes = [ctypes.c_void_p,
        ctypes.c_size_t]
_ldb.leveldb_options_set_write_buffer_size.restype = None
_ldb.leveldb_options_set_max_open_files.argtypes = [ctypes.c_void_p,
        ctypes.c_int]
_ldb.leveldb_options_set_max_open_files.restype = None
_ldb.leveldb_options_set_cache.argtypes = [ctypes.c_void_p, ctypes.c_void_p]
_ldb.leveldb_options_set_cache.restype = None
_ldb.leveldb_options_set_block_size.argtypes = [ctypes.c_void_p,
        ctypes.c_size_t]
_ldb.leveldb_options_set_block_size.restype = None
_ldb.leveldb_options_destroy.argtypes = [ctypes.c_void_p]
_ldb.leveldb_options_destroy.restype = None

_ldb.leveldb_open.argtypes = [ctypes.c_void_p, ctypes.c_char_p,
        ctypes.c_void_p]
_ldb.leveldb_open.restype = ctypes.c_void_p
_ldb.leveldb_close.argtypes = [ctypes.c_void_p]
_ldb.leveldb_close.restype = None
_ldb.leveldb_put.argtypes = [ctypes.c_void_p, ctypes.c_void_p,
        ctypes.c_void_p, ctypes.c_size_t, ctypes.c_void_p, ctypes.c_size_t,
        ctypes.c_void_p]
_ldb.leveldb_put.restype = None
_ldb.leveldb_delete.argtypes = [ctypes.c_void_p, ctypes.c_void_p,
        ctypes.c_void_p, ctypes.c_size_t, ctypes.c_void_p]
_ldb.leveldb_delete.restype = None
_ldb.leveldb_write.argtypes = [ctypes.c_void_p, ctypes.c_void_p,
        ctypes.c_void_p, ctypes.c_void_p]
_ldb.leveldb_write.restype = None
_ldb.leveldb_get.argtypes = [ctypes.c_void_p, ctypes.c_void_p,
        ctypes.c_void_p, ctypes.c_size_t, ctypes.c_void_p, ctypes.c_void_p]
_ldb.leveldb_get.restype = ctypes.POINTER(ctypes.c_char)

_ldb.leveldb_writeoptions_create.argtypes = []
_ldb.leveldb_writeoptions_create.restype = ctypes.c_void_p
_ldb.leveldb_writeoptions_destroy.argtypes = [ctypes.c_void_p]
_ldb.leveldb_writeoptions_destroy.restype = None
_ldb.leveldb_writeoptions_set_sync.argtypes = [ctypes.c_void_p,
        ctypes.c_ubyte]
_ldb.leveldb_writeoptions_set_sync.restype = None

_ldb.leveldb_readoptions_create.argtypes = []
_ldb.leveldb_readoptions_create.restype = ctypes.c_void_p
_ldb.leveldb_readoptions_destroy.argtypes = [ctypes.c_void_p]
_ldb.leveldb_readoptions_destroy.restype = None
_ldb.leveldb_readoptions_set_verify_checksums.argtypes = [ctypes.c_void_p,
        ctypes.c_ubyte]
_ldb.leveldb_readoptions_set_verify_checksums.restype = None
_ldb.leveldb_readoptions_set_fill_cache.argtypes = [ctypes.c_void_p,
        ctypes.c_ubyte]
_ldb.leveldb_readoptions_set_fill_cache.restype = None
_ldb.leveldb_readoptions_set_snapshot.argtypes = [ctypes.c_void_p,
        ctypes.c_void_p]
_ldb.leveldb_readoptions_set_snapshot.restype = None

_ldb.leveldb_create_iterator.argtypes = [ctypes.c_void_p, ctypes.c_void_p]
_ldb.leveldb_create_iterator.restype = ctypes.c_void_p
_ldb.leveldb_iter_destroy.argtypes = [ctypes.c_void_p]
_ldb.leveldb_iter_destroy.restype = None
_ldb.leveldb_iter_valid.argtypes = [ctypes.c_void_p]
_ldb.leveldb_iter_valid.restype = ctypes.c_bool
_ldb.leveldb_iter_key.argtypes = [ctypes.c_void_p,
        ctypes.POINTER(ctypes.c_size_t)]
_ldb.leveldb_iter_key.restype = ctypes.c_void_p
_ldb.leveldb_iter_value.argtypes = [ctypes.c_void_p,
        ctypes.POINTER(ctypes.c_size_t)]
_ldb.leveldb_iter_value.restype = ctypes.c_void_p
_ldb.leveldb_iter_next.argtypes = [ctypes.c_void_p]
_ldb.leveldb_iter_next.restype = None
_ldb.leveldb_iter_prev.argtypes = [ctypes.c_void_p]
_ldb.leveldb_iter_prev.restype = None
_ldb.leveldb_iter_seek_to_first.argtypes = [ctypes.c_void_p]
_ldb.leveldb_iter_seek_to_first.restype = None
_ldb.leveldb_iter_seek_to_last.argtypes = [ctypes.c_void_p]
_ldb.leveldb_iter_seek_to_last.restype = None
_ldb.leveldb_iter_seek.argtypes = [ctypes.c_void_p, ctypes.c_void_p,
        ctypes.c_size_t]
_ldb.leveldb_iter_seek.restype = None
_ldb.leveldb_iter_get_error.argtypes = [ctypes.c_void_p, ctypes.c_void_p]
_ldb.leveldb_iter_get_error.restype = None

_ldb.leveldb_writebatch_create.argtypes = []
_ldb.leveldb_writebatch_create.restype = ctypes.c_void_p
_ldb.leveldb_writebatch_destroy.argtypes = [ctypes.c_void_p]
_ldb.leveldb_writebatch_destroy.restype = None
_ldb.leveldb_writebatch_clear.argtypes = [ctypes.c_void_p]
_ldb.leveldb_writebatch_clear.restype = None

_ldb.leveldb_writebatch_put.argtypes = [ctypes.c_void_p, ctypes.c_void_p,
        ctypes.c_size_t, ctypes.c_void_p, ctypes.c_size_t]
_ldb.leveldb_writebatch_put.restype = None
_ldb.leveldb_writebatch_delete.argtypes = [ctypes.c_void_p, ctypes.c_void_p,
        ctypes.c_size_t]
_ldb.leveldb_writebatch_delete.restype = None

_ldb.leveldb_approximate_sizes.argtypes = [ctypes.c_void_p, ctypes.c_int,
        ctypes.c_void_p, ctypes.c_void_p, ctypes.c_void_p, ctypes.c_void_p,
        ctypes.c_void_p]
_ldb.leveldb_approximate_sizes.restype = None

_ldb.leveldb_compact_range.argtypes = [ctypes.c_void_p, ctypes.c_void_p,
        ctypes.c_size_t, ctypes.c_void_p, ctypes.c_size_t]
_ldb.leveldb_compact_range.restype = None

_ldb.leveldb_create_snapshot.argtypes = [ctypes.c_void_p]
_ldb.leveldb_create_snapshot.restype = ctypes.c_void_p
_ldb.leveldb_release_snapshot.argtypes = [ctypes.c_void_p, ctypes.c_void_p]
_ldb.leveldb_release_snapshot.restype = None

_ldb.leveldb_free.argtypes = [ctypes.c_void_p]
_ldb.leveldb_free.restype = None


class _PointerRef(object):

    __slots__ = ["ref", "_close", "_referrers", "__weakref__"]

    def __init__(self, ref, close_cb):
        self.ref = ref
        self._close = close_cb
        self._referrers = weakref.WeakValueDictionary()

    def addReferrer(self, referrer):
        self._referrers[id(referrer)] = referrer

    def close(self):
        ref, self.ref = self.ref, None
        close, self._close = self._close, None
        referrers = self._referrers
        self._referrers = weakref.WeakValueDictionary()
        for referrer in referrers.valuerefs():
            referrer = referrer()
            if referrer is not None:
                referrer.close()
        if ref is not None and close is not None:
            close(ref)

    __del__ = close


def _checkError(error):
    if bool(error):
        message = ctypes.string_at(error)
        _ldb.leveldb_free(ctypes.cast(error, ctypes.c_void_p))
        raise Error(message)


class _IteratorDbImpl(object):

    __slots__ = ["_ref"]

    def __init__(self, iterator_ref):
        self._ref = iterator_ref

    def valid(self):
        return _ldb.leveldb_iter_valid(self._ref.ref)

    def key(self):
        length = ctypes.c_size_t(0)
        val_p = _ldb.leveldb_iter_key(self._ref.ref, ctypes.byref(length))
        assert bool(val_p)
        return ctypes.string_at(val_p, length.value)

    def val(self):
        length = ctypes.c_size_t(0)
        val_p = _ldb.leveldb_iter_value(self._ref.ref, ctypes.byref(length))
        assert bool(val_p)
        return ctypes.string_at(val_p, length.value)

    def seek(self, key):
        _ldb.leveldb_iter_seek(self._ref.ref, key, len(key))
        self._checkError()

    def seekFirst(self):
        _ldb.leveldb_iter_seek_to_first(self._ref.ref)
        self._checkError()

    def seekLast(self):
        _ldb.leveldb_iter_seek_to_last(self._ref.ref)
        self._checkError()

    def prev(self):
        _ldb.leveldb_iter_prev(self._ref.ref)
        self._checkError()

    def next(self):
        _ldb.leveldb_iter_next(self._ref.ref)
        self._checkError()

    def _checkError(self):
        error = ctypes.POINTER(ctypes.c_char)()
        _ldb.leveldb_iter_get_error(self._ref.ref, ctypes.byref(error))
        _checkError(error)

    def close(self):
      self._ref.close()


def CtypesDB(path, bloom_filter_size=10, create_if_missing=False,
       error_if_exists=False, paranoid_checks=False,
       write_buffer_size=(4 * 1024 * 1024), max_open_files=1000,
       block_cache_size=(8 * 1024 * 1024), block_size=(4 * 1024),
       default_sync=False, default_verify_checksums=False,
       default_fill_cache=True):
    """This is the expected way to open a database. Returns a DBInterface.
    """

    filter_policy = _PointerRef(
            _ldb.leveldb_filterpolicy_create_bloom(bloom_filter_size),
            _ldb.leveldb_filterpolicy_destroy)
    cache = _PointerRef(
            _ldb.leveldb_cache_create_lru(block_cache_size),
            _ldb.leveldb_cache_destroy)

    options = _ldb.leveldb_options_create()
    _ldb.leveldb_options_set_filter_policy(
            options, filter_policy.ref)
    _ldb.leveldb_options_set_create_if_missing(options, create_if_missing)
    _ldb.leveldb_options_set_error_if_exists(options, error_if_exists)
    _ldb.leveldb_options_set_paranoid_checks(options, paranoid_checks)
    _ldb.leveldb_options_set_write_buffer_size(options, write_buffer_size)
    _ldb.leveldb_options_set_max_open_files(options, max_open_files)
    _ldb.leveldb_options_set_cache(options, cache.ref)
    _ldb.leveldb_options_set_block_size(options, block_size)

    error = ctypes.POINTER(ctypes.c_char)()
    db = _ldb.leveldb_open(options, path, ctypes.byref(error))
    _ldb.leveldb_options_destroy(options)
    _checkError(error)

    db = _PointerRef(db, _ldb.leveldb_close)
    filter_policy.addReferrer(db)
    cache.addReferrer(db)

    return DBInterface(_LevelDBImpl(db, other_objects=(filter_policy, cache)),
                       allow_close=True, default_sync=default_sync,
                       default_verify_checksums=default_verify_checksums,
                       default_fill_cache=default_fill_cache)


class _LevelDBImpl(object):

    __slots__ = ["_objs", "_db", "_snapshot"]

    def __init__(self, db_ref, snapshot_ref=None, other_objects=()):
        self._objs = other_objects
        self._db = db_ref
        self._snapshot = snapshot_ref

    def close(self):
        db, self._db = self._db, None
        objs, self._objs = self._objs, ()
        if db is not None:
            db.close()
        for obj in objs:
            obj.close()

    def put(self, key, val, sync=False):
        if self._snapshot is not None:
            raise TypeError("cannot put on leveldb snapshot")
        error = ctypes.POINTER(ctypes.c_char)()
        options = _ldb.leveldb_writeoptions_create()
        _ldb.leveldb_writeoptions_set_sync(options, sync)
        _ldb.leveldb_put(self._db.ref, options, key, len(key), val, len(val),
                ctypes.byref(error))
        _ldb.leveldb_writeoptions_destroy(options)
        _checkError(error)

    def delete(self, key, sync=False):
        if self._snapshot is not None:
            raise TypeError("cannot delete on leveldb snapshot")
        error = ctypes.POINTER(ctypes.c_char)()
        options = _ldb.leveldb_writeoptions_create()
        _ldb.leveldb_writeoptions_set_sync(options, sync)
        _ldb.leveldb_delete(self._db.ref, options, key, len(key),
                ctypes.byref(error))
        _ldb.leveldb_writeoptions_destroy(options)
        _checkError(error)

    def get(self, key, verify_checksums=False, fill_cache=True):
        error = ctypes.POINTER(ctypes.c_char)()
        options = _ldb.leveldb_readoptions_create()
        _ldb.leveldb_readoptions_set_verify_checksums(options,
                verify_checksums)
        _ldb.leveldb_readoptions_set_fill_cache(options, fill_cache)
        if self._snapshot is not None:
            _ldb.leveldb_readoptions_set_snapshot(options, self._snapshot.ref)
        size = ctypes.c_size_t(0)
        val_p = _ldb.leveldb_get(self._db.ref, options, key, len(key),
                ctypes.byref(size), ctypes.byref(error))
        if bool(val_p):
            val = ctypes.string_at(val_p, size.value)
            _ldb.leveldb_free(ctypes.cast(val_p, ctypes.c_void_p))
        else:
            val = None
        _ldb.leveldb_readoptions_destroy(options)
        _checkError(error)
        return val

    # pylint: disable=W0212
    def write(self, batch, sync=False):
        if self._snapshot is not None:
            raise TypeError("cannot delete on leveldb snapshot")
        real_batch = _ldb.leveldb_writebatch_create()
        for key, val in batch._puts.iteritems():
            _ldb.leveldb_writebatch_put(real_batch, key, len(key), val,
                    len(val))
        for key in batch._deletes:
            _ldb.leveldb_writebatch_delete(real_batch, key, len(key))
        error = ctypes.POINTER(ctypes.c_char)()
        options = _ldb.leveldb_writeoptions_create()
        _ldb.leveldb_writeoptions_set_sync(options, sync)
        _ldb.leveldb_write(self._db.ref, options, real_batch,
                ctypes.byref(error))
        _ldb.leveldb_writeoptions_destroy(options)
        _ldb.leveldb_writebatch_destroy(real_batch)
        _checkError(error)

    def iterator(self, verify_checksums=False, fill_cache=True):
        options = _ldb.leveldb_readoptions_create()
        if self._snapshot is not None:
            _ldb.leveldb_readoptions_set_snapshot(options, self._snapshot.ref)
        _ldb.leveldb_readoptions_set_verify_checksums(
                options, verify_checksums)
        _ldb.leveldb_readoptions_set_fill_cache(options, fill_cache)
        it_ref = _PointerRef(
                _ldb.leveldb_create_iterator(self._db.ref, options),
                _ldb.leveldb_iter_destroy)
        _ldb.leveldb_readoptions_destroy(options)
        self._db.addReferrer(it_ref)
        return _IteratorDbImpl(it_ref)

    def approximateDiskSizes(self, *ranges):
        if self._snapshot is not None:
            raise TypeError("cannot calculate disk sizes on leveldb snapshot")
        assert len(ranges) > 0
        key_type = ctypes.c_void_p * len(ranges)
        len_type = ctypes.c_size_t * len(ranges)
        start_keys, start_lens = key_type(), len_type()
        end_keys, end_lens = key_type(), len_type()
        sizes = (ctypes.c_uint64 * len(ranges))()
        for i, range_ in enumerate(ranges):
            assert isinstance(range_, tuple) and len(range_) == 2
            assert isinstance(range_[0], str) and isinstance(range_[1], str)
            start_keys[i] = ctypes.cast(range_[0], ctypes.c_void_p)
            end_keys[i] = ctypes.cast(range_[1], ctypes.c_void_p)
            start_lens[i], end_lens[i] = len(range_[0]), len(range_[1])
        _ldb.leveldb_approximate_sizes(self._db.ref, len(ranges), start_keys,
                start_lens, end_keys, end_lens, sizes)
        return list(sizes)

    def compactRange(self, start_key, end_key):
        assert isinstance(start_key, str) and isinstance(end_key, str)
        _ldb.leveldb_compact_range(self._db.ref, start_key, len(start_key),
                end_key, len(end_key))

    def snapshot(self):
        snapshot_ref = _PointerRef(
                _ldb.leveldb_create_snapshot(self._db.ref),
                lambda ref: _ldb.leveldb_release_snapshot(self._db.ref, ref))
        self._db.addReferrer(snapshot_ref)
        return _LevelDBImpl(self._db, snapshot_ref=snapshot_ref,
                            other_objects=self._objs)
