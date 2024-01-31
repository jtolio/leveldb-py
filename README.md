# ctypes bindings for LevelDB 

The existing three [LevelDB](http://code.google.com/p/leveldb/) interfaces ([py-leveldb](http://code.google.com/p/py-leveldb/), [cpy-leveldb](https://github.com/forhappy/cpy-leveldb), [plyvel](https://github.com/wbolster/plyvel/)) use the Python C API and compile Python modules that work with LevelDB's C++ interface. This Python module simply uses the ctypes library to LevelDB's C interface - making it more portable across Python implementations and easier to install and distribute. 

lvldb:
  * Python 2-3 compatible
  * supports get/put/delete (with standard read/write options)
  * supports bloom filters
  * supports leveldb LRU cache
  * allows for manual or automatic database closing (compare with py-leveldb)
  * provides write batches
  * provides iterators (full control of leveldb iteration, with additional idiomatic python iterator support)
  * provides prefix-based iteration (returns iterators that work as if all keys with a shared prefix had the prefix stripped and were dumped into their own database)
  * provides scoped sub-databases (presents a new database wrapper backed by an existing database with all keys prefixed by some prefix)
  * provides range iterators (for idioms like give me all keys between start and end)
  * provides an in-memory db implementation (for faster unit tests)
  * supports snapshots
  * fits in one file
  * requires no compilation
  
## Sample Usage 

```
# Python3
import lvldb

db = lvldb.DB("/path/to/db", create_if_missing=True)
db.put("key1", "value1")
db.put("key2", "value2")
print(f"key1 => {db.get('key1')} ")

for key, value in db:
    print(f"{key} => {value}")
```

