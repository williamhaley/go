# Pipeline

The processing pipeline works in these stages:

- Collections: Read the config that define each collection
- Collections Manifest: For each collection, process (in-order) the files specified
- Unzip: Unzip a file for a collection
- Read: Read/parse the content of a file
- Generate: Generate meaningful records from parsed file content
- Persist: Persist valid records
