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

import time
import random
import shutil
import leveldb
import leveldb.convenience
import tempfile
from collections import defaultdict


def benchClass(path, cls):
    db = cls(path, create_if_missing=True, error_if_exists=True)
    for i in xrange(500000):
        db.put(str(i), str(~i))
    for i in xrange(500000):
        assert db.get(str(i)) == str(~i)
    db.close()


def timeEach(*classes):
    total_times = defaultdict(lambda: 0)
    total_counts = defaultdict(lambda: 0)
    for i in xrange(15):
        cls = random.choice(classes)
        print cls
        start_time = time.time()
        tempdir = tempfile.mkdtemp()
        benchClass(tempfile.mkdtemp(), cls)
        total_times[cls] += (time.time() - start_time)
        total_counts[cls] += 1
        shutil.rmtree(tempdir)
    print
    min_speed = total_times[classes[0]] / total_counts[classes[0]]
    for cls in classes:
        speed = total_times[cls] / total_counts[cls]
        if speed < min_speed:
            min_speed = speed
    for cls in classes:
        speed = total_times[cls] / total_counts[cls]
        print cls, "%fx" % (speed / min_speed), total_counts[cls]


timeEach(leveldb.CythonDB, leveldb.DB)
