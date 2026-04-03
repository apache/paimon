# Paimon Tantivy Index

Full-text search global index for Apache Paimon, powered by [Tantivy](https://github.com/quickwit-oss/tantivy) (a Rust full-text search engine).

## Overview

This module provides full-text search capabilities for Paimon's Data Evolution (append) tables through the Global Index framework. It consists of two sub-modules:

- **paimon-tantivy-jni**: Rust/JNI bridge that wraps Tantivy's indexing and search APIs as native methods callable from Java.
- **paimon-tantivy-index**: Java integration layer that implements Paimon's `GlobalIndexer` SPI, handling index building, archive packing, and query execution.

### Architecture

```
┌─────────────────────────────────────────────────────┐
│                   Paimon Engine                      │
│  (FullTextSearchBuilder / FullTextScan / FullTextRead)│
└──────────────────────┬──────────────────────────────┘
                       │ GlobalIndexer SPI
┌──────────────────────▼──────────────────────────────┐
│              paimon-tantivy-index                     │
│  TantivyFullTextGlobalIndexWriter  (build index)     │
│  TantivyFullTextGlobalIndexReader  (search index)    │
└──────────────────────┬──────────────────────────────┘
                       │ JNI
┌──────────────────────▼──────────────────────────────┐
│              paimon-tantivy-jni                       │
│  TantivyIndexWriter   (write docs via JNI)           │
│  TantivySearcher      (search via JNI / stream I/O)  │
└──────────────────────┬──────────────────────────────┘
                       │ FFI
┌──────────────────────▼──────────────────────────────┐
│              Rust (lib.rs + jni_directory.rs)         │
│  Tantivy index writer / reader / query parser        │
└─────────────────────────────────────────────────────┘
```

### Index Schema

Tantivy index uses a fixed two-field schema:

| Field     | Tantivy Type | Description                                      |
|-----------|-------------|--------------------------------------------------|
| `row_id`  | u64 (stored, indexed) | Paimon's global row ID, used to map search results back to table rows |
| `text`    | TEXT (tokenized, indexed) | The text content from the indexed column         |

## Archive File Format

The writer produces a **single archive file** that bundles all Tantivy segment files into one sequential stream. This format is designed to be stored on any Paimon-supported file system (HDFS, S3, OSS, etc.) and read back without extracting to local disk.

### Layout

All integers are **big-endian**.

```
┌─────────────────────────────────────────────────┐
│  File Count (4 bytes, int32)                    │
├─────────────────────────────────────────────────┤
│  File Entry 1                                   │
│  ┌─────────────────────────────────────────────┐│
│  │ Name Length  (4 bytes, int32)               ││
│  │ Name         (N bytes, UTF-8)               ││
│  │ Data Length  (8 bytes, int64)               ││
│  │ Data         (M bytes, raw)                 ││
│  └─────────────────────────────────────────────┘│
├─────────────────────────────────────────────────┤
│  File Entry 2                                   │
│  ┌─────────────────────────────────────────────┐│
│  │ Name Length  (4 bytes, int32)               ││
│  │ Name         (N bytes, UTF-8)               ││
│  │ Data Length  (8 bytes, int64)               ││
│  │ Data         (M bytes, raw)                 ││
│  └─────────────────────────────────────────────┘│
├─────────────────────────────────────────────────┤
│  ...                                            │
└─────────────────────────────────────────────────┘
```

### Field Details

| Field        | Size    | Type   | Description                                    |
|-------------|---------|--------|------------------------------------------------|
| File Count  | 4 bytes | int32  | Number of files in the archive                 |
| Name Length | 4 bytes | int32  | Byte length of the file name                   |
| Name        | N bytes | UTF-8  | Tantivy segment file name (e.g. `meta.json`, `*.term`, `*.pos`, `*.store`) |
| Data Length | 8 bytes | int64  | Byte length of the file data                   |
| Data        | M bytes | raw    | Raw file content                               |

### Write Path

1. `TantivyFullTextGlobalIndexWriter` receives text values via `write(Object)`, one per row.
2. Each non-null text is passed to `TantivyIndexWriter` (JNI) as `addDocument(rowId, text)`, where `rowId` is a 0-based sequential counter.
3. On `finish()`, the Tantivy index is committed and all files in the local temp directory are packed into the archive format above.
4. The archive is written as a single file to Paimon's global index file system.
5. The local temp directory is deleted.

### Read Path

1. `TantivyFullTextGlobalIndexReader` opens the archive file as a `SeekableInputStream`.
2. The archive header is parsed to build a file layout table (name → offset, length).
3. A `TantivySearcher` is created with the layout and a `StreamFileInput` callback — Tantivy reads file data on demand via JNI callbacks to `seek()` + `read()` on the stream. No temp files are created.
4. Search queries are executed via Tantivy's `QueryParser` with BM25 scoring, returning `(rowId, score)` pairs.

## Usage

### Build Index

```sql
CALL sys.create_global_index(
    table => 'db.my_table',
    index_column => 'content',
    index_type => 'tantivy-fulltext'
);
```

### Search

```sql
SELECT * FROM full_text_search('my_table', 'content', 'search query', 10);
```

### Java API

```java
Table table = catalog.getTable(identifier);

GlobalIndexResult result = table.newFullTextSearchBuilder()
        .withQueryText("search query")
        .withLimit(10)
        .withTextColumn("content")
        .executeLocal();

ReadBuilder readBuilder = table.newReadBuilder();
TableScan.Plan plan = readBuilder.newScan()
        .withGlobalIndexResult(result).plan();
try (RecordReader<InternalRow> reader = readBuilder.newRead().createReader(plan)) {
    reader.forEachRemaining(row -> System.out.println(row));
}
```

## SPI Registration

The index type `tantivy-fulltext` is registered via Java SPI:

```
META-INF/services/org.apache.paimon.globalindex.GlobalIndexerFactory
  → org.apache.paimon.tantivy.index.TantivyFullTextGlobalIndexerFactory
```
