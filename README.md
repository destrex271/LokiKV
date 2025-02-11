# LokiKV :  ⚠️  WIP
LokiKV is intended to be a simple to use in memory Key-Value Store that can also persist data on disk.
This project is specifically for learning purposes only.


## Current Features

### Collections
 - Create multiple Collections(similar to tables)
 - Collections are of the following types:
   - Hashmap
   - BTreeMap
   - Custom BTree
 - List collections
 - Select one collection at a time

### Data Types
 - Blob: `"[BLOB_BEGINS]data of blob[BLOB_ENDS]"`
 - Integer
 - Boolean
 - Float
 - String

### Operations
 - Set key values
 - Get value for key
 - Print all values in collection as a string
 - Create multiple types of collections ```(\c_bcol, \c_hcol, \c_bcust)```
 - Select Collections
 - List all available collections

## TODO

 - Add support for distributed setup via Paxos Algorithm

