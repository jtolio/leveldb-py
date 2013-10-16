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

import weakref

from .convenience import DBInterface, Error

cdef extern from "stdint.h":
    ctypedef unsigned long long uint64_t

cdef extern from "Python.h":
    void* PyMem_Malloc(size_t n)
    void PyMem_Free(void *p)

cdef extern from "leveldb/c.h":
    ctypedef struct leveldb_t:
        pass
    ctypedef struct leveldb_cache_t:
        pass
    ctypedef struct leveldb_comparator_t:
        pass
    ctypedef struct leveldb_env_t:
        pass
    ctypedef struct leveldb_filelock_t:
        pass
    ctypedef struct leveldb_filterpolicy_t:
        pass
    ctypedef struct leveldb_iterator_t:
        pass
    ctypedef struct leveldb_logger_t:
        pass
    ctypedef struct leveldb_options_t:
        pass
    ctypedef struct leveldb_randomfile_t:
        pass
    ctypedef struct leveldb_readoptions_t:
        pass
    ctypedef struct leveldb_seqfile_t:
        pass
    ctypedef struct leveldb_snapshot_t:
        pass
    ctypedef struct leveldb_writeablefile_t:
        pass
    ctypedef struct leveldb_writebatch_t:
        pass
    ctypedef struct leveldb_writeoptions_t:
        pass

    leveldb_t* leveldb_open(
        leveldb_options_t* options,
        char* name,
        char** errptr) nogil

    void leveldb_close(leveldb_t* db) nogil

    void leveldb_put(
        leveldb_t* db,
        leveldb_writeoptions_t* options,
        char* key, size_t keylen,
        char* val, size_t vallen,
        char** errptr) nogil

    void leveldb_delete(
        leveldb_t* db,
        leveldb_writeoptions_t* options,
        char* key, size_t keylen,
        char** errptr) nogil

    void leveldb_write(
        leveldb_t* db,
        leveldb_writeoptions_t* options,
        leveldb_writebatch_t* batch,
        char** errptr) nogil

    char* leveldb_get(
        leveldb_t* db,
        leveldb_readoptions_t* options,
        char* key, size_t keylen,
        size_t* vallen,
        char** errptr) nogil

    leveldb_iterator_t* leveldb_create_iterator(
        leveldb_t* db,
        leveldb_readoptions_t* options) nogil

    leveldb_snapshot_t* leveldb_create_snapshot(
        leveldb_t* db) nogil

    void leveldb_release_snapshot(
        leveldb_t* db,
        leveldb_snapshot_t* snapshot) nogil

    char* leveldb_property_value(
        leveldb_t* db,
        char* propname) nogil

    void leveldb_approximate_sizes(
        leveldb_t* db,
        int num_ranges,
        char** range_start_key, size_t* range_start_key_len,
        char** range_limit_key, size_t* range_limit_key_len,
        uint64_t* sizes) nogil

    void leveldb_compact_range(
        leveldb_t* db,
        char* start_key, size_t start_key_len,
        char* limit_key, size_t limit_key_len) nogil

    void leveldb_destroy_db(
        leveldb_options_t* options,
        char* name,
        char** errptr) nogil

    void leveldb_repair_db(
        leveldb_options_t* options,
        char* name,
        char** errptr) nogil

    void leveldb_iter_destroy(leveldb_iterator_t*) nogil
    unsigned char leveldb_iter_valid(leveldb_iterator_t*) nogil
    void leveldb_iter_seek_to_first(leveldb_iterator_t*) nogil
    void leveldb_iter_seek_to_last(leveldb_iterator_t*) nogil
    void leveldb_iter_seek(leveldb_iterator_t*, char* k, size_t klen) nogil
    void leveldb_iter_next(leveldb_iterator_t*) nogil
    void leveldb_iter_prev(leveldb_iterator_t*) nogil
    char* leveldb_iter_key(leveldb_iterator_t*, size_t* klen) nogil
    char* leveldb_iter_value(leveldb_iterator_t*, size_t* vlen) nogil
    void leveldb_iter_get_error(leveldb_iterator_t*, char** errptr) nogil

    leveldb_writebatch_t* leveldb_writebatch_create() nogil
    void leveldb_writebatch_destroy(leveldb_writebatch_t*) nogil
    void leveldb_writebatch_clear(leveldb_writebatch_t*) nogil
    void leveldb_writebatch_put(
        leveldb_writebatch_t*,
        char* key, size_t klen,
        char* val, size_t vlen) nogil
    void leveldb_writebatch_delete(
        leveldb_writebatch_t*,
        char* key, size_t klen) nogil
    void leveldb_writebatch_iterate(
        leveldb_writebatch_t*,
        void* state,
        void (*put)(void*, char* k, size_t klen, char* v, size_t vlen),
        void (*deleted)(void*, char* k, size_t klen)) nogil

    leveldb_options_t* leveldb_options_create() nogil
    void leveldb_options_destroy(leveldb_options_t*) nogil
    void leveldb_options_set_comparator(
        leveldb_options_t*,
        leveldb_comparator_t*) nogil
    void leveldb_options_set_filter_policy(
        leveldb_options_t*,
        leveldb_filterpolicy_t*) nogil
    void leveldb_options_set_create_if_missing(
        leveldb_options_t*, unsigned char) nogil
    void leveldb_options_set_error_if_exists(
        leveldb_options_t*, unsigned char) nogil
    void leveldb_options_set_paranoid_checks(
        leveldb_options_t*, unsigned char) nogil
    void leveldb_options_set_env(leveldb_options_t*, leveldb_env_t*) nogil
    void leveldb_options_set_info_log(leveldb_options_t*, leveldb_logger_t*) nogil
    void leveldb_options_set_write_buffer_size(leveldb_options_t*, size_t) nogil
    void leveldb_options_set_max_open_files(leveldb_options_t*, int) nogil
    void leveldb_options_set_cache(leveldb_options_t*, leveldb_cache_t*) nogil
    void leveldb_options_set_block_size(leveldb_options_t*, size_t) nogil
    void leveldb_options_set_block_restart_interval(leveldb_options_t*, int) nogil

    void leveldb_options_set_compression(leveldb_options_t*, int) nogil

    leveldb_comparator_t* leveldb_comparator_create(
        void* state,
        void (*destructor)(void*),
        int (*compare)(
            void*,
            char* a, size_t alen,
            char* b, size_t blen),
        char* (*name)(void*)) nogil
    void leveldb_comparator_destroy(leveldb_comparator_t*) nogil

    leveldb_filterpolicy_t* leveldb_filterpolicy_create(
        void* state,
        void (*destructor)(void*),
        char* (*create_filter)(
            void*,
            char** key_array, size_t* key_length_array,
            int num_keys,
            size_t* filter_length),
        unsigned char (*key_may_match)(
            void*,
            char* key, size_t length,
            char* filter, size_t filter_length),
        char* (*name)(void*)) nogil
    void leveldb_filterpolicy_destroy(leveldb_filterpolicy_t*) nogil

    leveldb_filterpolicy_t* leveldb_filterpolicy_create_bloom(
        int bits_per_key) nogil

    leveldb_readoptions_t* leveldb_readoptions_create() nogil
    void leveldb_readoptions_destroy(leveldb_readoptions_t*) nogil
    void leveldb_readoptions_set_verify_checksums(
        leveldb_readoptions_t*,
        unsigned char) nogil
    void leveldb_readoptions_set_fill_cache(
        leveldb_readoptions_t*, unsigned char) nogil
    void leveldb_readoptions_set_snapshot(
        leveldb_readoptions_t*,
        leveldb_snapshot_t*) nogil

    leveldb_writeoptions_t* leveldb_writeoptions_create() nogil
    void leveldb_writeoptions_destroy(leveldb_writeoptions_t*) nogil
    void leveldb_writeoptions_set_sync(
        leveldb_writeoptions_t*, unsigned char) nogil

    leveldb_cache_t* leveldb_cache_create_lru(size_t capacity) nogil
    void leveldb_cache_destroy(leveldb_cache_t* cache) nogil

    leveldb_env_t* leveldb_create_default_env() nogil
    void leveldb_env_destroy(leveldb_env_t*) nogil

    void leveldb_free(void* ptr) nogil

    int leveldb_major_version() nogil

    int leveldb_minor_version() nogil


cdef class _CPointerRef:

    cdef void* ref
    cdef void (*_close)(void*)
    cdef object _referrers
    cdef object __weakref__

    def __init__(self):
        self.ref = NULL
        self._close = NULL
        self._referrers = weakref.WeakValueDictionary()

    cdef init(self, void* ref, void (*close_cb)(void*)):
        self.ref = ref
        self._close = close_cb

    cdef addReferrer(self, object referrer):
        self._referrers[id(referrer)] = referrer

    cpdef close(self):
        ref, self.ref = self.ref, NULL
        close, self._close = self._close, NULL
        referrers = self._referrers
        self._referrers = weakref.WeakValueDictionary()
        for referrer in referrers.valuerefs():
            referrer = referrer()
            if referrer is not None:
                referrer.close()
        if ref != NULL and close != NULL:
            close(ref)

    def __del__(self):
        self.close()


cdef _CPointerRef newPointerRef(void* ref, void (*close_cb)(void*)):
    o = _CPointerRef()
    o.init(ref, close_cb)
    return o


cdef _checkError(char* error):
    cdef bytes str_copy
    if error:
        str_copy = error
        leveldb_free(error)
        raise Error(str_copy)


cdef class _IteratorDbImpl:

    cdef _CPointerRef _ref

    def __init__(self, _CPointerRef iterator_ref):
        self._ref = iterator_ref

    cpdef valid(self):
        if leveldb_iter_valid(<leveldb_iterator_t*>self._ref.ref):
            return True
        return False

    cpdef key(self):
        cdef size_t length = 0
        cdef bytes val
        cdef char* val_p = leveldb_iter_key(<leveldb_iterator_t*>self._ref.ref,
            &length)
        val = val_p[:length]
        return val

    cpdef val(self):
        cdef size_t length = 0
        cdef bytes val
        cdef char* val_p = leveldb_iter_value(
            <leveldb_iterator_t*>self._ref.ref, &length)
        val = val_p[:length]
        return val

    cpdef seek(self, bytes key):
        leveldb_iter_seek(<leveldb_iterator_t*>self._ref.ref, key, len(key))
        self._checkError()

    cpdef seekFirst(self):
        leveldb_iter_seek_to_first(<leveldb_iterator_t*>self._ref.ref)
        self._checkError()

    cpdef seekLast(self):
        leveldb_iter_seek_to_last(<leveldb_iterator_t*>self._ref.ref)
        self._checkError()

    cpdef prev(self):
        leveldb_iter_prev(<leveldb_iterator_t*>self._ref.ref)
        self._checkError()

    cpdef next(self):
        leveldb_iter_next(<leveldb_iterator_t*>self._ref.ref)
        self._checkError()

    cdef _checkError(self):
        cdef char* error = NULL
        leveldb_iter_get_error(<leveldb_iterator_t*>self._ref.ref, &error)
        _checkError(error)

    cpdef close(self):
        self._ref.close()


def CythonDB(path, bloom_filter_size=10, create_if_missing=False,
       error_if_exists=False, paranoid_checks=False,
       write_buffer_size=(4 * 1024 * 1024), max_open_files=1000,
       block_cache_size=(8 * 1024 * 1024), block_size=(4 * 1024),
       default_sync=False, default_verify_checksums=False,
       default_fill_cache=True):
    """This is the expected way to open a database. Returns a DBInterface.
    """

    cdef _CPointerRef filter_policy = newPointerRef(
            <void*>leveldb_filterpolicy_create_bloom(bloom_filter_size),
            <void(*)(void*)>leveldb_filterpolicy_destroy)
    cdef _CPointerRef cache = newPointerRef(
            <void*>leveldb_cache_create_lru(block_cache_size),
            <void(*)(void*)>leveldb_cache_destroy)

    options = leveldb_options_create()
    leveldb_options_set_filter_policy(options,
            <leveldb_filterpolicy_t*>filter_policy.ref)
    leveldb_options_set_create_if_missing(options, create_if_missing)
    leveldb_options_set_error_if_exists(options, error_if_exists)
    leveldb_options_set_paranoid_checks(options, paranoid_checks)
    leveldb_options_set_write_buffer_size(options, write_buffer_size)
    leveldb_options_set_max_open_files(options, max_open_files)
    leveldb_options_set_cache(options, <leveldb_cache_t*>cache.ref)
    leveldb_options_set_block_size(options, block_size)

    cdef char* error = NULL
    cdef leveldb_t* db = leveldb_open(options, path, &error)
    leveldb_options_destroy(options)
    _checkError(error)

    py_db = newPointerRef(<void*>db, <void(*)(void*)>leveldb_close)
    filter_policy.addReferrer(py_db)
    cache.addReferrer(py_db)

    return DBInterface(_LevelDBImpl(py_db, other_objects=(filter_policy, cache)),
                       allow_close=True, default_sync=default_sync,
                       default_verify_checksums=default_verify_checksums,
                       default_fill_cache=default_fill_cache)


cdef class _LevelDBImpl:

    cdef object _objs
    cdef _CPointerRef _db
    cdef _CPointerRef _snapshot

    def __init__(self, db_ref, snapshot_ref=None, other_objects=()):
        self._objs = other_objects
        self._db = db_ref
        self._snapshot = snapshot_ref

    cpdef close(self):
        db, self._db = self._db, None
        objs, self._objs = self._objs, ()
        if db is not None:
            db.close()
        for obj in objs:
            obj.close()

    cpdef put(self, bytes key, bytes val, sync=False):
        if self._snapshot is not None:
            raise TypeError("cannot put on leveldb snapshot")
        cdef char* error = NULL
        options = leveldb_writeoptions_create()
        leveldb_writeoptions_set_sync(options, sync)
        leveldb_put(<leveldb_t*>self._db.ref, options, key, len(key), val,
                len(val), &error)
        leveldb_writeoptions_destroy(options)
        _checkError(error)

    cpdef delete(self, bytes key, sync=False):
        if self._snapshot is not None:
            raise TypeError("cannot delete on leveldb snapshot")
        cdef char* error = NULL
        options = leveldb_writeoptions_create()
        leveldb_writeoptions_set_sync(options, sync)
        leveldb_delete(<leveldb_t*>self._db.ref, options, key, len(key),
                &error)
        leveldb_writeoptions_destroy(options)
        _checkError(error)

    cpdef get(self, bytes key, verify_checksums=False, fill_cache=True):
        cdef char* error = NULL
        options = leveldb_readoptions_create()
        leveldb_readoptions_set_verify_checksums(options, verify_checksums)
        leveldb_readoptions_set_fill_cache(options, fill_cache)
        if self._snapshot is not None:
            leveldb_readoptions_set_snapshot(options,
                    <leveldb_snapshot_t*>self._snapshot.ref)
        cdef size_t size = 0
        cdef bytes val
        val_p = leveldb_get(<leveldb_t*>self._db.ref, options, key, len(key),
                &size, &error)
        if val_p:
            val = val_p[:size]
            leveldb_free(val_p)
        else:
            val = None
        leveldb_readoptions_destroy(options)
        _checkError(error)
        return val

    cpdef write(self, batch, sync=False):
        if self._snapshot is not None:
            raise TypeError("cannot delete on leveldb snapshot")
        real_batch = leveldb_writebatch_create()
        for key, val in batch._puts.iteritems():
            leveldb_writebatch_put(real_batch, key, len(key), val, len(val))
        for key in batch._deletes:
            leveldb_writebatch_delete(real_batch, key, len(key))
        cdef char* error = NULL
        options = leveldb_writeoptions_create()
        leveldb_writeoptions_set_sync(options, sync)
        leveldb_write(<leveldb_t*>self._db.ref, options, real_batch, &error)
        leveldb_writeoptions_destroy(options)
        leveldb_writebatch_destroy(real_batch)
        _checkError(error)

    cpdef iterator(self, verify_checksums=False, fill_cache=True):
        options = leveldb_readoptions_create()
        if self._snapshot is not None:
            leveldb_readoptions_set_snapshot(options,
                    <leveldb_snapshot_t*>self._snapshot.ref)
        leveldb_readoptions_set_verify_checksums(options, verify_checksums)
        leveldb_readoptions_set_fill_cache(options, fill_cache)
        it_ref = newPointerRef(
                <void*>leveldb_create_iterator(<leveldb_t*>self._db.ref,
                    options), <void(*)(void*)>leveldb_iter_destroy)
        leveldb_readoptions_destroy(options)
        self._db.addReferrer(it_ref)
        return _IteratorDbImpl(it_ref)

    def approximateDiskSizes(self, *ranges):
        if self._snapshot is not None:
            raise TypeError("cannot calculate disk sizes on leveldb snapshot")
        assert len(ranges) > 0

        cdef char** start_keys = NULL
        cdef char** end_keys = NULL
        cdef size_t* start_lens = NULL
        cdef size_t* end_lens = NULL
        cdef uint64_t* sizes = NULL
        try:
            start_keys = <char**>PyMem_Malloc(len(ranges)*sizeof(char*))
            if start_keys == NULL:
                raise MemoryError()
            end_keys = <char**>PyMem_Malloc(len(ranges)*sizeof(char*))
            if end_keys == NULL:
                raise MemoryError()
            start_lens = <size_t*>PyMem_Malloc(len(ranges)*sizeof(size_t))
            if start_lens == NULL:
                raise MemoryError()
            end_lens = <size_t*>PyMem_Malloc(len(ranges)*sizeof(size_t))
            if end_lens == NULL:
                raise MemoryError()
            sizes = <uint64_t*>PyMem_Malloc(len(ranges)*sizeof(uint64_t))
            if sizes == NULL:
                raise MemoryError()

            for i, range_ in enumerate(ranges):
                assert isinstance(range_, tuple) and len(range_) == 2
                assert isinstance(range_[0], str) and isinstance(range_[1], str)
                start_keys[i] = range_[0]
                end_keys[i] = range_[1]
                start_lens[i], end_lens[i] = len(range_[0]), len(range_[1])

            leveldb_approximate_sizes(<leveldb_t*>self._db.ref,
                    len(ranges), start_keys, start_lens, end_keys, end_lens,
                    sizes)

            return [sizes[i] for i in xrange(len(ranges))]
        finally:
            if start_keys != NULL:
                PyMem_Free(start_keys)
            if end_keys != NULL:
                PyMem_Free(end_keys)
            if start_lens != NULL:
                PyMem_Free(start_lens)
            if end_lens != NULL:
                PyMem_Free(end_lens)
            if sizes != NULL:
                PyMem_Free(sizes)

    cpdef compactRange(self, bytes start_key, bytes end_key):
        leveldb_compact_range(<leveldb_t*>self._db.ref, start_key,
                len(start_key), end_key, len(end_key))

    cpdef snapshot(self):
        snapshot_ref = newPointerRef(
                <void*>leveldb_create_snapshot(<leveldb_t*>self._db.ref),
                <void(*)(void*)>leveldb_release_snapshot)
        self._db.addReferrer(snapshot_ref)
        return _LevelDBImpl(self._db, snapshot_ref=snapshot_ref,
                            other_objects=self._objs)
