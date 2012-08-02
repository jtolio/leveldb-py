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

__author__ = "Shane Hansen"
__email__ = "shanemhansen@gmail.com"

import os
import shutil
import leveldb
import tempfile
import unittest


class LevelDBIteratorTest(unittest.TestCase):
    """Test that leveldb is iterable."""

    def setUp(self):
        self.db_path = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.db_path)

    def test_iteration(self):
        db = leveldb.DB(self.db_path, create_if_missing=True)
        db.put('a', 'b')
        db.put('c', 'd')
        iterator = iter(db)
        self.assertEqual(iterator.next(), ('a', 'b'))
        self.assertEqual(iterator.next(), ('c', 'd'))
        self.assertRaises(StopIteration, iterator.next)
        db.close()

    def test_iteration_with_break(self):
        db = leveldb.DB(self.db_path, create_if_missing=True)
        db.put('a', 'b')
        db.put('c', 'd')
        for key, value in db:
            self.assertEqual((key, value), ('a', 'b'))
            break
        db.close()

    def test_iteration_empty_db(self):
        """
        Test the null condition, no entries in the database.
        """
        db = leveldb.DB(self.db_path, create_if_missing=True)
        for _ in db:
            self.fail("shouldn't happen")
        db.close()

    def test_seek(self):
        """
        Test seeking forwards and backwards
        """
        db = leveldb.DB(self.db_path, create_if_missing=True)
        db.put('a', 'b')
        db.put('b', 'b')
        db.put('ca', 'a')
        db.put('cb', 'b')
        db.put('d', 'd')
        iterator = iter(db).seek("c")
        self.assertEqual(iterator.next(), ('ca', 'a'))
        self.assertEqual(iterator.next(), ('cb', 'b'))
        # seek backwards
        iterator.seek('a')
        self.assertEqual(iterator.next(), ('a', 'b'))
        db.close()

    def test_prefix(self):
        """
        Test iterator prefixes
        """
        batch = leveldb.WriteBatch()
        batch.put('a', 'b')
        batch.put('b', 'b')
        batch.put('cd', 'a')
        batch.put('ce', 'a')
        batch.put('c', 'a')
        batch.put('f', 'b')
        db = leveldb.DB(self.db_path, create_if_missing=True)
        db.write(batch)
        iterator = db.iterator(prefix="c")
        iterator.seekFirst()
        self.assertEqual(iterator.next(), ('', 'a'))
        self.assertEqual(iterator.next(), ('d', 'a'))
        self.assertEqual(iterator.next(), ('e', 'a'))
        self.assertRaises(StopIteration, iterator.next)
        db.close()

    def test_multiple_iterators(self):
        """
        Make sure that things work with multiple iterator objects
        alive at one time.
        """
        db = leveldb.DB(self.db_path, create_if_missing=True)
        entries = [('a', 'b'), ('b', 'b')]
        db.put(*entries[0])
        db.put(*entries[1])
        iter1 = iter(db)
        iter2 = iter(db)
        self.assertEqual(iter1.next(), entries[0])
        # garbage collect iter1, seek iter2 past the end of the db. Make sure
        # everything works.
        del iter1
        iter2.seek('z')
        self.assertRaises(StopIteration, iter2.next)
        db.close()

    def test_prev(self):
        db = leveldb.DB(self.db_path, create_if_missing=True)
        db.put('a', 'b')
        db.put('b', 'b')
        iterator = iter(db)
        entry = iterator.next()
        iterator.prev()
        self.assertEqual(entry, iterator.next())
        # it's ok to call prev when the iterator is at position 0
        iterator.prev()
        self.assertEqual(entry, iterator.next())
        db.close()

    def test_seek_first_last(self):
        db = leveldb.DB(self.db_path, create_if_missing=True)
        db.put('a', 'b')
        db.put('b', 'b')
        iterator = iter(db)
        iterator.seekLast()
        self.assertEqual(iterator.next(), ('b', 'b'))
        iterator.seekFirst()
        self.assertEqual(iterator.next(), ('a', 'b'))
        db.close()

    def test_scoped_seek_first(self):
        db = leveldb.DB(os.path.join(self.db_path, "1"),
                create_if_missing=True)
        db.put("ba", "1")
        db.put("bb", "2")
        db.put("cc", "3")
        db.put("cd", "4")
        db.put("de", "5")
        db.put("df", "6")
        it = db.scope("a").iterator().seekFirst()
        self.assertFalse(it.valid())
        it = db.scope("b").iterator().seekFirst()
        self.assertTrue(it.valid())
        self.assertEqual(it.key(), "a")
        it = db.scope("c").iterator().seekFirst()
        self.assertTrue(it.valid())
        self.assertEqual(it.key(), "c")
        it = db.scope("d").iterator().seekFirst()
        self.assertTrue(it.valid())
        self.assertEqual(it.key(), "e")
        it = db.scope("e").iterator().seekFirst()
        self.assertFalse(it.valid())
        db.close()

    def test_scoped_seek_last(self):
        db = leveldb.DB(os.path.join(self.db_path, "1"),
                create_if_missing=True)
        db.put("ba", "1")
        db.put("bb", "2")
        db.put("cc", "3")
        db.put("cd", "4")
        db.put("de", "5")
        db.put("df", "6")
        it = db.scope("a").iterator().seekLast()
        self.assertFalse(it.valid())
        it = db.scope("b").iterator().seekLast()
        self.assertTrue(it.valid())
        self.assertEqual(it.key(), "b")
        it = db.scope("c").iterator().seekLast()
        self.assertTrue(it.valid())
        self.assertEqual(it.key(), "d")
        it = db.scope("d").iterator().seekLast()
        self.assertTrue(it.valid())
        self.assertEqual(it.key(), "f")
        it = db.scope("e").iterator().seekLast()
        self.assertFalse(it.valid())
        db.close()
        db = leveldb.DB(os.path.join(self.db_path, "2"),
                create_if_missing=True)
        db.put("\xff\xff\xffab", "1")
        db.put("\xff\xff\xffcd", "2")
        it = db.scope("\xff\xff\xff").iterator().seekLast()
        self.assertTrue(it.valid())
        self.assertEqual(it.key(), "cd")
        db.close()
        db = leveldb.DB(os.path.join(self.db_path, "3"),
                create_if_missing=True)
        db.put("\xff\xff\xfeab", "1")
        db.put("\xff\xff\xfecd", "2")
        it = db.scope("\xff\xff\xff").iterator().seekLast()
        self.assertFalse(it.valid())
        db.close()
        db = leveldb.DB(os.path.join(self.db_path, "4"),
                create_if_missing=True)
        db.put("\xff\xff\xfeab", "1")
        db.put("\xff\xff\xfecd", "2")
        it = db.scope("\xff\xff\xfe").iterator().seekLast()
        self.assertTrue(it.valid())
        self.assertEqual(it.key(), "cd")
        db.close()
        db = leveldb.DB(os.path.join(self.db_path, "5"),
                create_if_missing=True)
        db.put("\xff\xff\xfeab", "1")
        db.put("\xff\xff\xfecd", "2")
        db.put("\xff\xff\xffef", "1")
        db.put("\xff\xff\xffgh", "2")
        it = db.scope("\xff\xff\xfe").iterator().seekLast()
        self.assertTrue(it.valid())
        self.assertEqual(it.key(), "cd")
        db.close()
