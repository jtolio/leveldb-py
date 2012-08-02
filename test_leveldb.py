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
        db.close()

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
        db.close()

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
        db.close()

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
        db.close()

    def testRange(self):
        db = leveldb.DB(self.db_path, create_if_missing=True)

        def keys(alphabet, length=5):
            if length == 0:
                yield ""
                return
            for char in alphabet:
                for prefix in keys(alphabet, length - 1):
                    yield prefix + char

        for val, key in enumerate(keys(map(chr, xrange(ord('a'), ord('f'))))):
            db.put(key, str(val))

        self.assertEquals([row.key for row in db.range("bbbb", "bbcb")],
            ['bbbba', 'bbbbb', 'bbbbc', 'bbbbd', 'bbbbe', 'bbbca', 'bbbcb',
             'bbbcc', 'bbbcd', 'bbbce', 'bbbda', 'bbbdb', 'bbbdc', 'bbbdd',
             'bbbde', 'bbbea', 'bbbeb', 'bbbec', 'bbbed', 'bbbee', 'bbcaa',
             'bbcab', 'bbcac', 'bbcad', 'bbcae'])
        self.assertEquals([row.key for row in db.range("bbbbb", "bbcbb")],
            ['bbbbb', 'bbbbc', 'bbbbd', 'bbbbe', 'bbbca', 'bbbcb', 'bbbcc',
             'bbbcd', 'bbbce', 'bbbda', 'bbbdb', 'bbbdc', 'bbbdd', 'bbbde',
             'bbbea', 'bbbeb', 'bbbec', 'bbbed', 'bbbee', 'bbcaa', 'bbcab',
             'bbcac', 'bbcad', 'bbcae', 'bbcba'])
        self.assertEquals([r.key for r in db.scope("dd").range("bb", "cb")],
            ['bba', 'bbb', 'bbc', 'bbd', 'bbe', 'bca', 'bcb', 'bcc', 'bcd',
             'bce', 'bda', 'bdb', 'bdc', 'bdd', 'bde', 'bea', 'beb', 'bec',
             'bed', 'bee', 'caa', 'cab', 'cac', 'cad', 'cae'])
        self.assertEquals([r.key for r in db.scope("dd").range("bbb", "cbb")],
            ['bbb', 'bbc', 'bbd', 'bbe', 'bca', 'bcb', 'bcc', 'bcd', 'bce',
             'bda', 'bdb', 'bdc', 'bdd', 'bde', 'bea', 'beb', 'bec', 'bed',
             'bee', 'caa', 'cab', 'cac', 'cad', 'cae', 'cba'])

    def testRangeOptionalEndpoints(self):
        db = leveldb.DB(self.db_path, create_if_missing=True)
        db.put("aa", "1")
        db.put("bb", "2")
        db.put("cc", "3")
        db.put("dd", "4")
        db.put("ee", "5")

        self.assertEquals([r.key for r in db.iterator().seek("d").range()],
                ["aa", "bb", "cc", "dd", "ee"])
        self.assertEquals([r.key for r in db.iterator().seek("d").range(
                start_key="bb")], ["bb", "cc", "dd", "ee"])
        self.assertEquals([r.key for r in db.iterator().seek("d").range(
                end_key="cc")], ["aa", "bb"])
        self.assertEquals([r.key for r in db.iterator().seek("d").range(
                start_key="bb", end_key="cc")], ["bb"])
        self.assertEquals([r.key for r in db.iterator().seek("d").range(
                start_key="b")], ["bb", "cc", "dd", "ee"])
        self.assertEquals([r.key for r in db.iterator().seek("d").range(
                end_key="c")], ["aa", "bb"])
        self.assertEquals([r.key for r in db.iterator().seek("d").range(
                start_key="b", end_key="c")], ["bb"])

        self.assertEquals([r.key for r in db.iterator().seek("d").range(
                start_key="bb", start_inclusive=True)],
                ["bb", "cc", "dd", "ee"])
        self.assertEquals([r.key for r in db.iterator().seek("d").range(
                start_key="bb", start_inclusive=False)],
                ["cc", "dd", "ee"])

        self.assertEquals([r.key for r in db.iterator().seek("d").range(
                end_key="cc", end_inclusive=True)], ["aa", "bb", "cc"])
        self.assertEquals([r.key for r in db.iterator().seek("d").range(
                end_key="cc", end_inclusive=False)], ["aa", "bb"])

        self.assertEquals([r.key for r in db.iterator().seek("d").range(
                start_key="bb", end_key="cc", start_inclusive=True,
                end_inclusive=True)], ["bb", "cc"])
        self.assertEquals([r.key for r in db.iterator().seek("d").range(
                start_key="bb", end_key="cc", start_inclusive=True,
                end_inclusive=False)], ["bb"])
        self.assertEquals([r.key for r in db.iterator().seek("d").range(
                start_key="bb", end_key="cc", start_inclusive=False,
                end_inclusive=True)], ["cc"])
        self.assertEquals([r.key for r in db.iterator().seek("d").range(
                start_key="bb", end_key="cc", start_inclusive=False,
                end_inclusive=False)], [])

        self.assertEquals([r.key for r in db.iterator().seek("d").range(
                start_key="b", start_inclusive=True)],
                ["bb", "cc", "dd", "ee"])
        self.assertEquals([r.key for r in db.iterator().seek("d").range(
                start_key="b", start_inclusive=False)],
                ["bb", "cc", "dd", "ee"])
        self.assertEquals([r.key for r in db.iterator().seek("d").range(
                end_key="c", end_inclusive=True)], ["aa", "bb"])
        self.assertEquals([r.key for r in db.iterator().seek("d").range(
                end_key="c", end_inclusive=False)], ["aa", "bb"])
        self.assertEquals([r.key for r in db.iterator().seek("d").range(
                start_key="b", end_key="c", start_inclusive=True,
                end_inclusive=True)], ["bb"])
        self.assertEquals([r.key for r in db.iterator().seek("d").range(
                start_key="b", end_key="c", start_inclusive=False,
                end_inclusive=True)], ["bb"])
        self.assertEquals([r.key for r in db.iterator().seek("d").range(
                start_key="b", end_key="c", start_inclusive=True,
                end_inclusive=False)], ["bb"])
        self.assertEquals([r.key for r in db.iterator().seek("d").range(
                start_key="b", end_key="c", start_inclusive=False,
                end_inclusive=False)], ["bb"])

    def testScopedDB(self, use_writebatch=False):
        db = leveldb.DB(self.db_path, create_if_missing=True)
        scoped_db_1 = db.scope("prefix1_")
        scoped_db_2 = db.scope("prefix2_")
        scoped_db_2a = scoped_db_2.scope("a_")
        scoped_db_2b = scoped_db_2.scope("b_")
        scoped_db_3 = db.scope("prefix3_")

        def mod(op, db, ops):
            if use_writebatch:
                batch = leveldb.WriteBatch()
                for args in ops:
                    getattr(batch, op)(*args)
                db.write(batch)
            else:
                for args in ops:
                    getattr(db, op)(*args)

        mod("put", db, [("1", "2"), ("prefix2_a_13", "14")])
        mod("put", scoped_db_1, [("3", "4")])
        mod("put", scoped_db_2, [("5", "6")])
        mod("put", scoped_db_2a, [("7", "8")])
        mod("put", scoped_db_2b, [("9", "10")])
        mod("put", scoped_db_3, [("11", "12")])
        db_data = [("1", "2"), ("prefix1_3", "4"), ("prefix2_5", "6"),
                   ("prefix2_a_13", "14"), ("prefix2_a_7", "8"),
                   ("prefix2_b_9", "10"), ("prefix3_11", "12")]
        self.assertEquals(list(db), db_data)
        self.assertEquals(list(scoped_db_1), [("3", "4")])
        scoped_db_2_data = [("5", "6"), ("a_13", "14"), ("a_7", "8"),
                ("b_9", "10")]
        self.assertEquals(list(scoped_db_2), scoped_db_2_data)
        self.assertEquals(list(scoped_db_2a), [("13", "14"), ("7", "8")])
        self.assertEquals(list(scoped_db_2b), [("9", "10")])
        self.assertEquals(list(scoped_db_3), [("11", "12")])
        for key, val in db_data:
            self.assertEquals(db.get(key), val)
        for key, val in scoped_db_2_data:
            self.assertEquals(scoped_db_2.get(key), val)
        self.assertEquals(scoped_db_1.get("3"), "4")
        self.assertEquals(scoped_db_2a.get("7"), "8")
        self.assertEquals(scoped_db_2b.get("9"), "10")
        self.assertEquals(scoped_db_3.get("11"), "12")
        self.assertEqual(scoped_db_2a.get("13"), "14")
        mod("delete", db, [["1"], ["prefix2_a_7"]])
        mod("delete", scoped_db_1, [["3"]])
        mod("delete", scoped_db_2, [["5"]])
        mod("delete", scoped_db_2a, [["13"]])
        mod("delete", scoped_db_2b, [["9"]])
        mod("delete", scoped_db_3, [["11"]])
        self.assertEquals(list(db), [])
        self.assertEquals(list(scoped_db_1), [])
        self.assertEquals(list(scoped_db_2), [])
        self.assertEquals(list(scoped_db_2a), [])
        self.assertEquals(list(scoped_db_2b), [])
        self.assertEquals(list(scoped_db_3), [])
        for key, val in db_data:
            self.assertEquals(db.get(key), None)
        for key, val in scoped_db_2_data:
            self.assertEquals(scoped_db_2.get(key), None)
        self.assertEquals(scoped_db_1.get("3"), None)
        self.assertEquals(scoped_db_2a.get("7"), None)
        self.assertEquals(scoped_db_2b.get("9"), None)
        self.assertEquals(scoped_db_3.get("11"), None)
        self.assertEqual(scoped_db_2a.get("13"), None)
        db.close()

    def testScopedDB_WriteBatch(self):
        self.testScopedDB(use_writebatch=True)

    def testKeysWithZeroBytes(self):
        db = leveldb.DB(self.db_path, create_if_missing=True)
        key_with_zero_byte = ("\x01\x00\x02\x03\x04")
        db.put(key_with_zero_byte, "hey")
        self.assertEqual(db.get(key_with_zero_byte), "hey")
        it = db.iterator().seekFirst()
        self.assertTrue(it.valid())
        self.assertEqual(it.value(), "hey")
        self.assertEqual(it.key(), key_with_zero_byte)
        self.assertEqual(db.get(it.key()), "hey")
        db.close()

    def testValuesWithZeroBytes(self):
        db = leveldb.DB(self.db_path, create_if_missing=True)
        value_with_zero_byte = ("\x01\x00\x02\x03\x04")
        db.put("hey", value_with_zero_byte)
        self.assertEqual(db.get("hey"), value_with_zero_byte)
        it = db.iterator().seekFirst()
        self.assertTrue(it.valid())
        self.assertEqual(it.key(), "hey")
        self.assertEqual(it.value(), value_with_zero_byte)
        db.close()

def main():
    parser = argparse.ArgumentParser("run tests")
    parser.add_argument("--runs", type=int, default=1)
    args = parser.parse_args()
    for _ in xrange(args.runs):
        unittest.main(argv=sys.argv[:1], exit=False)


if __name__ == "__main__":
    main()
