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
        self.dir = tempfile.mkdtemp()
        self.db = leveldb.DB(os.path.join(self.dir, "db"),
                create_if_missing=True, error_if_exists=True)

    def tearDown(self):
        self.db.close()
        shutil.rmtree(self.dir)

    def test_iteration(self):
        self.db.put('a', 'b')
        self.db.put('c', 'd')
        iterator = iter(self.db)
        self.assertEqual(iterator.next(), ('a', 'b'))
        self.assertEqual(iterator.next(), ('c', 'd'))
        self.assertRaises(StopIteration, iterator.next)

    def test_iteration_with_break(self):
        self.db.put('a', 'b')
        self.db.put('c', 'd')
        for key, value in self.db:
            self.assertEqual((key, value), ('a', 'b'))
            break

    def test_iteration_empty_db(self):
        """
        Test the null condition, no entries in the database.
        """
        for _ in self.db:
            self.fail("shouldn't happen")

    def test_seek(self):
        """
        Test seeking forwards and backwards
        """
        self.db.put('a', 'b')
        self.db.put('b', 'b')
        self.db.put('ca', 'a')
        self.db.put('cb', 'b')
        self.db.put('d', 'd')
        iterator = iter(self.db).seek("c")
        self.assertEqual(iterator.next(), ('ca', 'a'))
        self.assertEqual(iterator.next(), ('cb', 'b'))
        # seek backwards
        iterator.seek('a')
        self.assertEqual(iterator.next(), ('a', 'b'))

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
        self.db.write(batch)
        iterator = self.db.iterator(prefix="c")
        iterator.seekFirst()
        self.assertEqual(iterator.next(), ('', 'a'))
        self.assertEqual(iterator.next(), ('d', 'a'))
        self.assertEqual(iterator.next(), ('e', 'a'))
        self.assertRaises(StopIteration, iterator.next)

    def test_multiple_iterators(self):
        """
        Make sure that things work with multiple iterator objects
        alive at one time.
        """
        entries = [('a', 'b'), ('b', 'b')]
        self.db.put(*entries[0])
        self.db.put(*entries[1])
        iter1 = iter(self.db)
        iter2 = iter(self.db)
        self.assertEqual(iter1.next(), entries[0])
        # garbage collect iter1, seek iter2 past the end of the db. Make sure
        # everything works.
        del iter1
        iter2.seek('z')
        self.assertRaises(StopIteration, iter2.next)

    def test_prev(self):
        self.db.put('a', 'b')
        self.db.put('b', 'b')
        iterator = iter(self.db)
        entry = iterator.next()
        iterator.prev()
        self.assertEqual(entry, iterator.next())
        # it's ok to call prev when the iterator is at position 0
        iterator.prev()
        self.assertEqual(entry, iterator.next())

    def test_seek_first_last(self):
        self.db.put('a', 'b')
        self.db.put('b', 'b')
        iterator = iter(self.db)
        iterator.seekLast()
        self.assertEqual(iterator.next(), ('b', 'b'))
        iterator.seekFirst()
        self.assertEqual(iterator.next(), ('a', 'b'))
