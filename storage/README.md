# Storage Package

The storage package provides core data types and operations for efficient binary data handling in SimpleDB. 
With few exceptions, it implements zero-copy conversions between Go types and their binary representations using unsafe pointer operations.

## Key Types

- `FixedLen`: Fixed length binary data (integers, offsets etc)
- `Varlen`: Variable length binary data (strings, blobs etc) 
- `Page`: 8KB page for database storage
- Integer types: `TinyInt`, `SmallInt`, `Int`, `Long`

## Binary Conversions

The package provides fast zero-copy conversions between Go types and binary data:

```go
// Convert binary to integer
val := FixedToInteger[Int](data)

// Convert integer to binary
bytes := IntegerToFixedlen[Int](size, val)

// Convert string to varlen
varlen := NewVarlenFromGoString(str)

// Convert varlen to string 
str := VarlenToGoString(varlen)
```

These conversions use unsafe pointer operations to avoid allocations and copies. 
They are designed for internal engine use where performance is critical.

## Pages

Pages are fixed 8KB blocks used for database storage. They provide methods for reading and writing binary data at specific offsets:

```go
page := NewPage()
page.SetFixedlen(offset, size, val)
page.GetFixedlen(offset, size) 
```

The page methods handle bounds checking and ensure safe access to the underlying storage.
