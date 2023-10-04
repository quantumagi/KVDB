# KVDB.DBreezeDb

## Overview

`KVDB` provides an `IDb` unified interface and implementations for RocksDB, LevelDB, and DBreeze. 
This makes it easy swap out the underlying store without changing your code, enabling flexibility and reducing vendor lock-in.

## Features

- **Unified Interface**: Use a single API for different key/value databases.
- **Table Support**: Perform operations at the granularity of tables.
- **Iterator Support**: Iterate over keys within single tables or across the database.
- **Batch Operations**: Perform multiple operations atomically.

## Installation

To install the package, use NuGet Package Manager and run:

```
Install-Package KVDB
Install-Package KVDB.DBreezeDb
```

Or via the dotnet CLI:

```
dotnet add package KVDB
dotnet add package KVDB.DBreezeDb
```

## Usage

Implementing the `IDb` interface gives you the following capabilities:

- `Open(string dbPath)`: Open the database located at the specified path.
- `Get(byte table, byte[] key)` and `Get(byte[] key)`: Retrieve values by key, optionally within a specific table.
- `GetIterator(byte table)` and `GetIterator()`: Retrieve iterators to navigate keys.
- `GetWriteBatch(params byte[] tables)`: Retrieve a batch object to perform multiple write operations atomically.
- `Clear()`: Clear all tables and their contents.

### Example

```csharp
using KVDB;
using KVDB.DBreezeDb;

// Usage
IDb myDb = new DBreezeDb();
myDb.Open("path/to/db");
byte[] value = myDb.Get(1, new byte[] {0x01});
```
