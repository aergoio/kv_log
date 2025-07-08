[![Build Status](https://github.com/aergoio/kv_log/actions/workflows/ci.yml/badge.svg)](https://github.com/aergoio/kv_log/actions/workflows/ci.yml)

# kv_log

A high-performance embedded key-value database with a radix trie index structure

## Overview

kv_log is a persistent key-value store designed for high performance and reliability. It uses a combination of append-only logging for data storage and a radix trie for indexing.

## Features

- **Append-only log file**: Data is written sequentially for optimal write performance
- **Radix trie indexing**: Fast key lookups with O(k) complexity where k is key length
- **ACID transactions**: Support for atomic operations with commit/rollback
- **Write-Ahead Logging (WAL)**: Ensures durability and crash recovery
- **Configurable write modes**: Balance between performance and durability
- **Efficient page cache**: Minimizes disk I/O with intelligent caching
- **Concurrent access**: Thread-safe operations with appropriate locking

## Architecture

kv_log databases consist of three files:

1. **Main file**: Stores all key-value data in an append-only log format
2. **Index file**: Contains the radix trie structure for efficient key lookups
3. **WAL file**: Contains flushed index pages, for high durability

### Radix Trie Structure

The index uses a radix trie (also known as a patricia trie) to efficiently locate keys:

- Each node in the trie represents a part of the key
- The trie is organized by byte values (0-255)
- Leaf pages store entries with the same prefix
- When a leaf page becomes full, it's converted to a radix page

### Page Types

1. **Radix Pages**: Internal nodes of the trie, containing pointers to other pages
2. **Leaf Pages**: Terminal nodes containing key suffixes and pointers to data

## Usage

### Basic Operations

```go
// Open or create a database
db, err := kv_log.Open("path/to/database")
if err != nil {
    // Handle error
}
defer db.Close()

// Set a key-value pair
err = db.Set([]byte("key"), []byte("value"))

// Get a value
value, err := db.Get([]byte("key"))

// Delete a key
err = db.Delete([]byte("key"))
```

### Transactions

```go
// Begin a transaction
tx, err := db.Begin()
if err != nil {
    // Handle error
}

// Perform operations within the transaction
err = tx.Set([]byte("key1"), []byte("value1"))
err = tx.Set([]byte("key2"), []byte("value2"))

// Commit or rollback
if everythingOk {
    err = tx.Commit()
} else {
    err = tx.Rollback()
}
```

### Configuration Options

```go
options := kv_log.Options{
    "ReadOnly": true,                    // Open in read-only mode
    "CacheSizeThreshold": 10000,         // Maximum number of pages in cache
    "DirtyPageThreshold": 5000,          // Maximum dirty pages before flush
    "CheckpointThreshold": 1024 * 1024,  // WAL size before checkpoint (1MB)
}

db, err := kv_log.Open("path/to/database", options)
```

## Performance Considerations

- **Write Modes**: Choose between durability and performance
  - `CallerThread_WAL_Sync`: Maximum durability, lowest performance
  - `WorkerThread_WAL`: Good performance and durability (default)
  - `WorkerThread_NoWAL_NoSync`: Maximum performance, lowest durability

- **Cache Size**: Adjust based on available memory and workload
  - Larger cache improves read performance but uses more memory

- **Checkpoint Threshold**: Controls WAL file size before checkpoint
  - Smaller values reduce recovery time but may impact performance

## Implementation Details

- Keys are limited to 2KB
- Values are limited to 128MB
- The database uses a page size of 4KB

## Recovery

The database automatically recovers from crashes by:

1. Reading the main file header
2. Checking for a valid index file
3. Scanning for commit markers in the main file
4. Rebuilding the index if necessary

## Limitations

- Single connection per database file: Only one process can open the database in write mode at a time
- Concurrent access model: Supports one writer thread or multiple reader threads simultaneously

## Pros and Cons

- **Pro:** It is extremely fast on reads for a disk-based database engine
- **Con:** The index uses A LOT of disk space for bigger databases

The index does not always grow linearly but in phases.

Example case: The index file reaches ~25GB when the main db grows above ~4GB

But it is more than 3.5x faster than BadgerDB on reads

## Performance

| Metric | LevelDB | BadgerDB | ForestDB | KV_Log |
|--------|---------|----------|----------|--------|
| Set 2M values | 2m 44.45s | 13.81s | 18.95s | 6.98s |
| 20K txns (10 items each) | 1m 0.09s | 1.32s | 2.78s | 1.70s |
| Space after write | 1052.08 MB | 2002.38 MB | 1715.76 MB | 1501.09 MB |
| Space after close | 1158.78 MB | 1203.11 MB | 2223.16 MB | 1899.50 MB |
| Read 2M values (fresh) | 1m 26.87s | 35.14s | 17.21s | 11.20s |
| Read 2M values (cold) | 1m 34.46s | 38.36s | 16.84s | 10.81s |

Benchmark writting 2 million records (key: 33 random bytes, value: 750 random bytes) in a single transaction and then reading them in non-sequential order

Also insertion using 20 thousand transactions

## License

Apache 2.0
