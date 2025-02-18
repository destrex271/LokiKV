# LokiKV :  ⚠️  WIP
LokiKV is intended to be a simple to use in memory Key-Value Store that can also persist data on disk.
This project is specifically for learning purposes only.

To try out loki-kv you can follow these steps:

```bash
git clone https://github.com/destrex271/LokiKV


cargo run --bin server-db # in a separate terminal
# runs on localhost:8765 by default

# in a separate terminal to start CLI
cargo run  --bin client -- localhost 8765
```

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

<hr/>

# Supported Operations


LokiQL is a custom query language for interacting with the LokiKV database. This document describes the supported commands and their syntax.

## **Literals**

### **Whitespace**
- Spaces, newlines, carriage returns, and tabs are ignored where applicable.

### **Data Types**
- **Integer (`INT`)**: Signed or unsigned integer numbers.
- **Float (`FLOAT`)**: Signed or unsigned floating point numbers.
- **Boolean (`BOOL`)**: `true` or `false`.
- **String (`STRING`)**: Enclosed in single quotes (`'example'`).
- **Blob (`BLOB`)**: Enclosed in `[BLOB_BEGINS]` and `[BLOB_ENDS]`.

## **Identifiers**
- **ID**: Any string without whitespace.

## **Command Syntax**

LokiQL supports three types of commands:

### **Duo Commands (Require a Key and a Value)**
| Command | Syntax |
|---------|--------|
| `SET`   | `SET <ID> <STRING | INT | BOOL | FLOAT | BLOB>` |

#### **Examples**:
```plaintext
SET mykey 'hello'
SET count 42
SET enabled true
SET temperature 98.6
SET file [BLOB_BEGINS]aGVsbG8=[BLOB_ENDS]
```

### **Uni Commands (Require a Key Only)**
| Command  | Syntax |
|----------|--------|
| `GET`    | `GET <ID>` |
| `INCR`   | `INCR <ID>` |
| `DECR`   | `DECR <ID>` |
| `/c_hcol`  | `/c_hcol <ID>` |
| `/c_bcol`  | `/c_bcol <ID>` |
| `/c_bcust` | `/c_bcust <ID>` |
| `/selectcol` | `/selectcol <ID>` |

#### **Examples**:
```plaintext
GET mykey
INCR count
DECR count
/selectcol users
```

### **Solo Commands (Do Not Require Arguments)**
| Command  | Syntax |
|----------|--------|
| `DISPLAY`  | `DISPLAY` |
| `/getcur_colname` | `/getcur_colname` |
| `/listcolnames`   | `/listcolnames` |

#### **Examples**:
```plaintext
DISPLAY
/getcur_colname
/listcolnames
```

## **Command File Structure**
A LokiQL command file follows this structure:

```plaintext
COMMAND; COMMAND; COMMAND;
```

### **Example**
```plaintext
SET mykey 'hello';
GET mykey;
DISPLAY;
```

Each command must be **separated by `;`** if used in a script.

---

This document outlines the LokiQL commands supported by LokiKV. Ensure all commands follow the required syntax to avoid parsing errors.

<hr/>

# TODO

 - Add support for distributed setup via Paxos Algorithm
