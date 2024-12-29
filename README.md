# SimpleDB

SimpleDB is a Go implementation of a basic database system, as described in the book ["Database Design and Implementation"](https://link.springer.com/book/10.1007/978-3-030-33836-7) by Edward Sciore.

## Overview

This project aims to create a simple, educational database system that demonstrates core concepts of database management systems. It includes components for file management, SQL parsing, query execution, and more.

## Features

- File-based storage: Each table and index is stored in separate files
- SQL parsing and execution
- Table scans and B-tree indexing
- Log management for recovery
- TCP server for client connections

## Supported Data Types

SimpleDB currently supports the following data types:

- INT: Integer values
- VARCHAR: Variable-length character strings

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
> CREATE TABLE users (id INT, name VARCHAR(20));
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

## License

This project is licensed under the [MIT License](https://opensource.org/license/mit).

## Acknowledgments

- Edward Sciore for the book "Database Design and Implementation"
- All contributors to this project
