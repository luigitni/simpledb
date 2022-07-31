# SimpleDB

This project is a Go implementation of SimpleDB, as described by **Database design and implementation** by *Edward Sciore*

# File manager

A SimpleDB database is stored in multiple files, one for each table and each index, plus a log file and several catalog files.

The SimpleDB file manager provides block-level access to these files.