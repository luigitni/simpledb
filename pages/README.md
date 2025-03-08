### Slotted Page Architecture

The database uses a slotted page design where each page is divided into three sections:
- A header containing page metadata and an array of slots
- A data section growing from the end of the page towards the beginning
- A special section at the end of the page for index-specific data

The page header stores the following metadata:
1. Block Number (Long) - identifies the page in the file
2. Page Type (TinyInt) - indicates if the page is a heap or btree page
3. Number of Slots (SmallInt) - tracks active record slots
4. Free Space End (SmallInt) - points to the end of free space
5. Special Space Start (Offset) - marks the beginning of special storage area

6. Entries Array - array of slot entries, each containing:
   - Record offset (2 bytes)
   - Record length (2 bytes) 
   - Record flags (4 bytes)

Records are stored with their own headers containing:
- Transaction information (xmin/xmax)
- Operation flags
- Record status
