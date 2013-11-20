#!/usr/bin/env python
#
# Copyright (C) 2013 Space Monkey, Inc.
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

__author__ = "JT Olds"
__email__ = "jt@spacemonkey.com"

from .convenience import _makeDBFromImpl, Error


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
    void leveldb_options_set_info_log(
        leveldb_options_t*, leveldb_logger_t*) nogil
    void leveldb_options_set_write_buffer_size(
        leveldb_options_t*, size_t) nogil
    void leveldb_options_set_max_open_files(leveldb_options_t*, int) nogil
    void leveldb_options_set_cache(leveldb_options_t*, leveldb_cache_t*) nogil
    void leveldb_options_set_block_size(leveldb_options_t*, size_t) nogil
    void leveldb_options_set_block_restart_interval(
        leveldb_options_t*, int) nogil

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


cdef _checkError(char* error):
    cdef bytes str_copy
    if error:
        str_copy = error
        leveldb_free(error)
        raise Error(str_copy)


cdef class _IteratorDbImpl:

    cdef leveldb_iterator_t* ref

    def __cinit__(self):
        self.ref = NULL

    cdef _IteratorDbImpl init(self, leveldb_iterator_t* ref):
        self.ref = ref
        return self

    cpdef valid(self):
        if leveldb_iter_valid(self.ref):
            return True
        return False

    cpdef bytes key(self):
        cdef size_t length = 0
        cdef bytes val
        cdef char* val_p = leveldb_iter_key(self.ref, &length)
        val = val_p[:length]
        return val

    cpdef bytes val(self):
        cdef size_t length = 0
        cdef bytes val
        cdef char* val_p = leveldb_iter_value(self.ref, &length)
        val = val_p[:length]
        return val

    cpdef seek(self, bytes key):
        leveldb_iter_seek(self.ref, key, len(key))
        self._checkError()

    cpdef seekFirst(self):
        leveldb_iter_seek_to_first(self.ref)
        self._checkError()

    cpdef seekLast(self):
        leveldb_iter_seek_to_last(self.ref)
        self._checkError()

    cpdef prev(self):
        leveldb_iter_prev(self.ref)
        self._checkError()

    cpdef next(self):
        leveldb_iter_next(self.ref)
        self._checkError()

    cdef _checkError(self):
        cdef char* error = NULL
        leveldb_iter_get_error(self.ref, &error)
        _checkError(error)

    cpdef close(self):
        cdef leveldb_iterator_t* ref = self.ref
        self.ref = NULL
        if ref != NULL:
            leveldb_iter_destroy(ref)

    def __dealloc__(self):
        self.close()


def CythonDB(path, bloom_filter_size=10, create_if_missing=False,
       error_if_exists=False, paranoid_checks=False,
       write_buffer_size=(4 * 1024 * 1024), max_open_files=1000,
       block_cache_size=(8 * 1024 * 1024), block_size=(4 * 1024),
       default_sync=False, default_verify_checksums=False,
       default_fill_cache=True):
    """This is the expected way to open a database. Returns a DBInterface.
    """

    cdef leveldb_filterpolicy_t* filter_policy = \
            leveldb_filterpolicy_create_bloom(bloom_filter_size)
    cdef leveldb_cache_t* cache = leveldb_cache_create_lru(
            block_cache_size)

    options = leveldb_options_create()
    leveldb_options_set_filter_policy(options, filter_policy)
    leveldb_options_set_create_if_missing(options, create_if_missing)
    leveldb_options_set_error_if_exists(options, error_if_exists)
    leveldb_options_set_paranoid_checks(options, paranoid_checks)
    leveldb_options_set_write_buffer_size(options, write_buffer_size)
    leveldb_options_set_max_open_files(options, max_open_files)
    leveldb_options_set_cache(options, cache)
    leveldb_options_set_block_size(options, block_size)

    cdef char* error = NULL
    cdef leveldb_t* db_ptr = leveldb_open(options, path, &error)
    leveldb_options_destroy(options)
    db = _LevelDBImpl().init(db_ptr, filter_policy, cache, NULL)

    # okay, now all the allocated memory is in managed objects, safe to throw
    # errors
    _checkError(error)

    return _makeDBFromImpl(
            db, default_sync=default_sync,
            default_verify_checksums=default_verify_checksums,
            default_fill_cache=default_fill_cache)


cdef class _LevelDBImpl:

    cdef leveldb_t* _db
    cdef leveldb_filterpolicy_t* _filterpolicy
    cdef leveldb_cache_t* _cache
    cdef leveldb_snapshot_t* _snapshot

    def __cinit__(self):
        self._db = NULL
        self._filterpolicy = NULL
        self._cache = NULL
        self._snapshot = NULL

    cdef _LevelDBImpl init(
            self, leveldb_t* db, leveldb_filterpolicy_t* filterpolicy,
            leveldb_cache_t* cache, leveldb_snapshot_t* snapshot):
        self._db = db
        self._filterpolicy = filterpolicy
        self._cache = cache
        self._snapshot = snapshot
        return self

    cpdef close(self):
        cdef leveldb_t* db = self._db
        self._db = NULL
        cdef leveldb_filterpolicy_t* filterpolicy = self._filterpolicy
        self._filterpolicy = NULL
        cdef leveldb_cache_t* cache = self._cache
        self._cache = NULL
        cdef leveldb_snapshot_t* snapshot = self._snapshot
        self._snapshot = NULL

        if db != NULL:
            if snapshot != NULL:
                leveldb_release_snapshot(db, snapshot)
            else:
                leveldb_close(db)

        if filterpolicy != NULL:
            leveldb_filterpolicy_destroy(filterpolicy)
        if cache != NULL:
            leveldb_cache_destroy(cache)

    def __dealloc__(self):
        self.close()

    cpdef put(self, bytes key, bytes val, sync=False):
        if self._snapshot != NULL:
            raise TypeError("cannot put on leveldb snapshot")
        cdef char* error = NULL
        options = leveldb_writeoptions_create()
        leveldb_writeoptions_set_sync(options, sync)
        leveldb_put(self._db, options, key, len(key), val, len(val), &error)
        leveldb_writeoptions_destroy(options)
        _checkError(error)

    cpdef delete(self, bytes key, sync=False):
        if self._snapshot != NULL:
            raise TypeError("cannot delete on leveldb snapshot")
        cdef char* error = NULL
        options = leveldb_writeoptions_create()
        leveldb_writeoptions_set_sync(options, sync)
        leveldb_delete(self._db, options, key, len(key), &error)
        leveldb_writeoptions_destroy(options)
        _checkError(error)

    cpdef bytes get(self, bytes key, verify_checksums=False, fill_cache=True):
        cdef char* error = NULL
        options = leveldb_readoptions_create()
        leveldb_readoptions_set_verify_checksums(options, verify_checksums)
        leveldb_readoptions_set_fill_cache(options, fill_cache)
        if self._snapshot != NULL:
            leveldb_readoptions_set_snapshot(options, self._snapshot)
        cdef size_t size = 0
        cdef bytes val
        val_p = leveldb_get(self._db, options, key, len(key), &size, &error)
        if val_p:
            val = val_p[:size]
            leveldb_free(val_p)
        else:
            val = None
        leveldb_readoptions_destroy(options)
        _checkError(error)
        return val

    cpdef write(self, batch, sync=False):
        if self._snapshot != NULL:
            raise TypeError("cannot delete on leveldb snapshot")
        real_batch = leveldb_writebatch_create()
        for key, val in batch._puts.iteritems():
            leveldb_writebatch_put(real_batch, key, len(key), val, len(val))
        for key in batch._deletes:
            leveldb_writebatch_delete(real_batch, key, len(key))
        cdef char* error = NULL
        options = leveldb_writeoptions_create()
        leveldb_writeoptions_set_sync(options, sync)
        leveldb_write(self._db, options, real_batch, &error)
        leveldb_writeoptions_destroy(options)
        leveldb_writebatch_destroy(real_batch)
        _checkError(error)

    cpdef _IteratorDbImpl iterator(
            self, verify_checksums=False, fill_cache=True):
        options = leveldb_readoptions_create()
        if self._snapshot != NULL:
            leveldb_readoptions_set_snapshot(options, self._snapshot)
        leveldb_readoptions_set_verify_checksums(options, verify_checksums)
        leveldb_readoptions_set_fill_cache(options, fill_cache)
        it = _IteratorDbImpl().init(leveldb_create_iterator(self._db, options))
        leveldb_readoptions_destroy(options)
        return it

    def approximateDiskSizes(self, *ranges):
        if self._snapshot != NULL:
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
                assert isinstance(range_, tuple)
                assert len(range_) == 2
                assert isinstance(range_[0], str)
                assert isinstance(range_[1], str)
                start_keys[i] = range_[0]
                end_keys[i] = range_[1]
                start_lens[i], end_lens[i] = len(range_[0]), len(range_[1])

            leveldb_approximate_sizes(self._db, len(ranges), start_keys,
                    start_lens, end_keys, end_lens, sizes)

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
        leveldb_compact_range(self._db, start_key, len(start_key), end_key,
                len(end_key))

    cpdef _LevelDBImpl snapshot(self):
        if self._snapshot != NULL:
            return self
        return _LevelDBImpl().init(
                self._db, NULL, NULL, leveldb_create_snapshot(self._db))
