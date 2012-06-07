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

__author__ = "JT Olds"
__email__ = "jt@spacemonkey.com"

import sys
import time
import shutil
import leveldb
import argparse
import tempfile
import unittest


class LevelDBTestCases(unittest.TestCase):

    def setUp(self):
        self.db_path = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.db_path, ignore_errors=True)

    def testInit(self):
        self.assertRaises(leveldb.Error, leveldb.DB, self.db_path)
        leveldb.DB(self.db_path, create_if_missing=True).close()
        leveldb.DB(self.db_path, create_if_missing=True).close()
        leveldb.DB(self.db_path).close()
        self.assertRaises(leveldb.Error, leveldb.DB, self.db_path,
                create_if_missing=True, error_if_exists=True)

    def testPutGet(self):
        db = leveldb.DB(self.db_path, create_if_missing=True)
        db.put("key1", "val1")
        db.put("key2", "val2", sync=True)
        self.assertEqual(db.get("key1"), "val1")
        self.assertEqual(db.get("key2"), "val2")
        self.assertEqual(db.get("key1", verify_checksums=True), "val1")
        self.assertEqual(db.get("key2", verify_checksums=True), "val2")
        self.assertEqual(db.get("key1", fill_cache=False), "val1")
        self.assertEqual(db.get("key2", fill_cache=False), "val2")
        self.assertEqual(db.get("key1", verify_checksums=True,
                fill_cache=False), "val1")
        self.assertEqual(db.get("key2", verify_checksums=True,
                fill_cache=False), "val2")
        self.assertEqual(db.get("key1"), "val1")
        self.assertEqual(db.get("key2"), "val2")

    def testDelete(self):
        db = leveldb.DB(self.db_path, create_if_missing=True)
        self.assertTrue(db.get("key1") is None)
        self.assertTrue(db.get("key2") is None)
        self.assertTrue(db.get("key3") is None)
        db.put("key1", "val1")
        db.put("key2", "val2")
        db.put("key3", "val3")
        self.assertEqual(db.get("key1"), "val1")
        self.assertEqual(db.get("key2"), "val2")
        self.assertEqual(db.get("key3"), "val3")
        db.delete("key1")
        db.delete("key2", sync=True)
        self.assertTrue(db.get("key1") is None)
        self.assertTrue(db.get("key2") is None)
        self.assertEqual(db.get("key3"), "val3")

    def testPutSync(self, size=100):
        db = leveldb.DB(self.db_path, create_if_missing=True)
        for i in xrange(size):
            db.put(str(i), str(i + 1))
        start_sync_time = time.time()
        for i in xrange(size):
            db.put(str(i), str(i + 1), sync=True)
        start_unsync_time = time.time()
        for i in xrange(size):
            db.put(str(i), str(i + 1))
        end_time = time.time()
        sync_time = start_unsync_time - start_sync_time
        unsync_time = end_time - start_unsync_time
        self.assertTrue(sync_time > 10 * unsync_time)

    def testDeleteSync(self, size=100):
        db = leveldb.DB(self.db_path, create_if_missing=True)
        for i in xrange(size):
            db.put(str(i), str(i + 1))
        start_sync_time = time.time()
        for i in xrange(size):
            db.delete(str(i), sync=True)
        end_sync_time = time.time()
        for i in xrange(size):
            db.put(str(i), str(i + 1))
        start_unsync_time = time.time()
        for i in xrange(size):
            db.delete(str(i))
        end_unsync_time = time.time()
        sync_time = end_sync_time - start_sync_time
        unsync_time = end_unsync_time - start_unsync_time
        self.assertTrue(sync_time > 10 * unsync_time)


def main():
    parser = argparse.ArgumentParser("run tests")
    parser.add_argument("--runs", type=int, default=1)
    args = parser.parse_args()
    for _ in xrange(args.runs):
        unittest.main(argv=sys.argv[:1], exit=False)


if __name__ == "__main__":
    main()
