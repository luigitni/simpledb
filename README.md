# SimpleDB

## Overview

SimpleDB is a Go implementation of a basic database system, originally implemented as described by the book ["Database Design and Implementation"](https://link.springer.com/book/10.1007/978-3-030-33836-7) by Edward Sciore.

The goal of the project is to serve as a learning tool for understanding fundamental database concepts as 
we slowly diverge and expand from the original design by adding features and architectural sophistications.

The project started as a Go port of the original Java implementation described in the book. 
While maintaining the educational value of SimpleDB, we are continuously refactoring the codebase to better align with Go project structures and idioms.

## Major divergencies from the original SimpleDB

### Storage engine
The storage engine has been completely redesigned around a slotted page architecture. 
Each 8KB page is divided into three sections: 
- a header containing metadata and slot entries
- a data section growing from the end towards the beginning
- a special section for index-specific data. 

The page header tracks block numbers, page types (heap/btree), slot counts, and space management information.

Records are stored with transaction-aware headers containing xmin/xmax information, providing the foundation for future MVCC implementation. 
The engine uses Go's unsafe package for zero-copy reading of fixed-length values, optimizing performance through direct memory access.

### Write-Ahead Logging and Buffer Management
The buffer manager maintains a pre-allocated pool of buffers, coordinating page access through a pin/unpin reference counting mechanism.
When buffers are exhausted, a mark-and-sweep strategy identifies and reclaims unpinned buffers.

The WAL implementation currently maintains the original SimpleDB page format where records are prepended from the end of the buffer towards the beginning. 
Each log record is assigned a Log Sequence Number (LSN), which the buffer manager uses to ensure proper Write-Ahead Logging protocol.
No modified page is written to disk before its corresponding log records are persisted.

### B-tree Implementation
The B-tree index structure is built on the same slotted page architecture, supporting both fixed and variable-length keys.
The current implementation handles basic operations, with planned improvements for concurrent access patterns.

### Type System
The database provides native support for INT (fixed-length) and TEXT (variable-length) types, with type-safe operations and comparisons. 
The type system is designed for extensibility to support additional types in future releases.

## Features

- File-based storage: Each table and index is stored in separate files
- SQL parsing and execution
- Table scans and B-tree indexing
- Log management for recovery
- TCP server for client connections

## Supported Data Types

SimpleDB currently supports the following data types:

- INT: Integer values
- TEXT: Variable-length character strings

Additional data types will be added in future updates to enhance the database's capabilities.

## Supported SQL Statements

SimpleDB currently supports a subset of SQL statements. This list will expand as the project progresses:

- CREATE TABLE
- INSERT INTO
- SELECT
- UPDATE
- DELETE
- CREATE INDEX
- ORDER BY

More complex queries and additional SQL statements will be added in future updates.

## Getting Started

### Prerequisites

- Go (version 1.22 or later)

### Installation

1. Clone the repository:
   ```
   git clone https://github.com/yourusername/simpledb.git
   ```
2. Navigate to the project directory:
   ```
   cd simpledb
   ```
3. Build the project:
   ```
   go build ./cmd/simpledb
   ```

### Usage

Run the SimpleDB server:

```
./simpledb
```

The server will start and listen on port 8765 by default.

Connect to the server using a TCP client (e.g., telnet):

```
telnet localhost 8765
```

You will be greeted with a welcome message and a prompt. \
You can then enter SQL commands followed by a semicolon (;).

Example session:
```
Hello user! Thanks for using SimpleDB!
> CREATE TABLE users (id INT, name TEXT);
0
(5.69 ms)
> INSERT INTO users (id, name) VALUES (1, 'Alice');
1
(9.81 ms)
> SELECT id, name FROM users;

| id |  name   |
|----|---------|
| 1  | 'alice' |
---
1 record found.
(2.27 ms)
> exit
bye!
```

## Project Structure

- `cmd/simpledb`: Main application entry point
- `sql`: SQL parsing and tokenization
- `record`: Record management and table operations
- `log`: Log management for recovery
- `file`: File management for database storage
- `tx`: Transaction and buffer management

## Roadmap

- [ ] Implement overflow pages for off-page data
- [ ] Implement garbage collection for dead heap tuples
- [ ] Implement MVCC and snapshot isolation for concurrency control
- [ ] Improve B-tree indexing and concurrent index operations
- [ ] Improve planner and executor for better query performance
- [ ] Implement additional data types and SQL statements

## License

This project is licensed under the [MIT License](https://opensource.org/license/mit).

## Acknowledgments

- Edward Sciore for the book "Database Design and Implementation"
- All contributors to this project
