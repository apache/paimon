# Paimon Tantivy

Full-text search global index for Apache Paimon, powered by [Tantivy](https://github.com/quickwit-oss/tantivy) (a Rust full-text search engine).

## Overview

This module provides full-text search capabilities for Paimon's Data Evolution (append) tables through the Global Index framework. It contains only the Paimon integration layer. Native Tantivy access, JNI, FFI, index archive handling, and query parsing are provided by the separate `paimon-full-text` dependency.

### Architecture

```
┌─────────────────────────────────────────────────────┐
│                   Paimon Engine                      │
│  (FullTextSearchBuilder / FullTextScan / FullTextRead)│
└──────────────────────┬──────────────────────────────┘
                       │ GlobalIndexer SPI
┌──────────────────────▼──────────────────────────────┐
│                 paimon-tantivy                        │
│  GlobalIndexer SPI / Paimon stream adapters          │
└──────────────────────┬──────────────────────────────┘
                       │ Java API
┌──────────────────────▼──────────────────────────────┐
│              paimon-full-text                         │
│  JNI / FFI / Tantivy index writer and reader         │
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
2. Each non-null text is passed to `paimon-full-text`'s `FullTextIndexWriter`, where `rowId` is a 0-based sequential counter.
3. On `finish()`, `paimon-full-text` commits the Tantivy index and packs it into the archive format above.
4. The archive is written as a single file to Paimon's global index file system.
5. Temporary native resources are owned and cleaned up by `paimon-full-text`.

### Read Path

1. `TantivyFullTextGlobalIndexReader` opens the archive file as a `SeekableInputStream`.
2. The stream is passed to `paimon-full-text`'s `FullTextIndexReader` through a positional read adapter.
3. Tantivy reads file data on demand through `pread` callbacks backed by Paimon's file IO. No temp files are created in Paimon.
4. Search queries are converted to the native single-column query JSON and executed with BM25 scoring, returning `(rowId, score)` pairs.

## Usage

### Build Index

```sql
CALL sys.create_global_index(
    table => 'db.my_table',
    index_column => 'content',
    index_type => 'tantivy-fulltext'
);
```

### Tokenizers

By default, Tantivy uses its built-in tokenizer. For Chinese or other languages where users often
search by short character fragments, build the index with the `ngram` tokenizer:

```sql
CALL sys.create_global_index(
    table => 'db.my_table',
    index_column => 'content',
    index_type => 'tantivy-fulltext',
    options => 'tantivy.tokenizer=ngram,tantivy.ngram.min-gram=2,tantivy.ngram.max-gram=2'
);
```

For Chinese word segmentation, build the index with the `jieba` tokenizer:

```sql
CALL sys.create_global_index(
    table => 'db.my_table',
    index_column => 'content',
    index_type => 'tantivy-fulltext',
    options => 'tantivy.tokenizer=jieba'
);
```

Available tokenizer options:

| Option | Default | Description |
|--------|---------|-------------|
| `tantivy.tokenizer` | `default` | Tokenizer used by the full-text index. Supported values: `default`, `simple`, `whitespace`, `raw`, `ngram`, `jieba`. |
| `tantivy.ngram.min-gram` | `2` | Minimum gram length for the `ngram` tokenizer. |
| `tantivy.ngram.max-gram` | `2` | Maximum gram length for the `ngram` tokenizer. |
| `tantivy.ngram.prefix-only` | `false` | Whether the `ngram` tokenizer only emits prefix ngrams. |
| `tantivy.lower-case` | `true` | Whether configurable tokenizers lowercase emitted tokens. |
| `tantivy.max-token-length` | `40` | Maximum token length kept by configurable tokenizers. |
| `tantivy.ascii-folding` | `false` | Whether to normalize non-ASCII Latin characters to ASCII. |
| `tantivy.stem` | `false` | Whether to apply stemming to emitted tokens. |
| `tantivy.language` | `english` | Language used by stemming and built-in stop word filters. |
| `tantivy.remove-stop-words` | `false` | Whether to remove built-in stop words for the configured language. |
| `tantivy.stop-words` | ` ` | Semicolon-separated custom stop words to remove. |
| `tantivy.with-position` | `true` | Whether to store term positions for phrase queries. |

Tokenizer settings are persisted in each global index file's metadata. Readers use that metadata
when reopening an index, so changing table/procedure options later does not make existing index
files use a different analyzer.
Custom analysis is provided by composing the supported tokenizer and filter options above; Paimon
does not load arbitrary Rust tokenizer plugins from configuration.
PyPaimon can query `jieba` indexes when the Python `jieba` package is installed.

### Search

```sql
SELECT * FROM full_text_search(
    'my_table',
    '{"match":{"column":"content","terms":"search query"}}',
    10
);
```

### Java API

```java
Table table = catalog.getTable(identifier);

GlobalIndexResult result = table.newFullTextSearchBuilder()
        .withQuery(FullTextQuery.match("search query", "content"))
        .withLimit(10)
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
