# Paimon Full Text

Full-text search global index for Apache Paimon, backed by the native `paimon-full-text-index` engine.

## Overview

This module provides full-text search for both Data Evolution (append) tables through the Global
Index framework and compaction-visible primary-key tables through file-aligned index archives. It
contains only the Paimon integration layer. Native full-text access, JNI, FFI, index archive
handling, and query parsing are provided by the separate `paimon-full-text-index` dependency.

### Architecture

```
┌─────────────────────────────────────────────────────┐
│                   Paimon Engine                      │
│  (FullTextSearchBuilder / FullTextScan / FullTextRead)│
└──────────────────────┬──────────────────────────────┘
                       │ GlobalIndexer SPI
┌──────────────────────▼──────────────────────────────┐
│                 paimon-full-text                     │
│  GlobalIndexer SPI / Paimon stream adapters          │
└──────────────────────┬──────────────────────────────┘
	                       │ Java API
┌──────────────────────▼──────────────────────────────┐
│              paimon-full-text-index                  │
│  JNI / FFI / native full-text index writer and reader         │
└─────────────────────────────────────────────────────┘
```

### Index Schema

Native full-text index uses a fixed two-field schema:

| Field     | Native Type | Description                                      |
|-----------|-------------|--------------------------------------------------|
| `row_id`  | u64 (stored, indexed) | Paimon's global row ID, used to map search results back to table rows |
| `text`    | TEXT (tokenized, indexed) | The text content from the indexed column         |

## Archive File Format

The writer produces a **single archive file** that bundles all native full-text segment files into one sequential stream. This format is designed to be stored on any Paimon-supported file system (HDFS, S3, OSS, etc.) and read back without extracting to local disk.

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
| Name        | N bytes | UTF-8  | native segment file name (e.g. `meta.json`, `*.term`, `*.pos`, `*.store`) |
| Data Length | 8 bytes | int64  | Byte length of the file data                   |
| Data        | M bytes | raw    | Raw file content                               |

### Write Path

1. `NativeFullTextGlobalIndexWriter` receives text values via `write(Object)`, one per row.
2. Each non-null text is passed to `paimon-full-text-index`'s `FullTextIndexWriter`, where `rowId` is a 0-based sequential counter.
3. On `finish()`, `paimon-full-text-index` commits the native full-text index and packs it into the archive format above.
4. The archive is written as a single file to Paimon's global index file system.
5. Temporary native resources are owned and cleaned up by `paimon-full-text-index`.

### Read Path

1. `NativeFullTextGlobalIndexReader` opens the archive file as a `SeekableInputStream`.
2. The stream is passed to `paimon-full-text-index`'s `FullTextIndexReader` through a positional read adapter.
3. The native reader reads file data on demand through `pread` callbacks backed by Paimon's file IO. No temp files are created in Paimon.
4. Search queries are converted to the native single-column query JSON and executed with BM25 scoring, returning `(rowId, score)` pairs.

## Usage

### Primary-Key Tables

Primary-key full-text indexing uses the fixed `full-text` SPI automatically. Configure the text
column directly on the table; no implementation selector is needed.

```sql
CREATE TABLE articles (
    id BIGINT,
    content STRING,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'bucket' = '16',
    'deletion-vectors.enabled' = 'true',
    'pk-full-text.index.columns' = 'content',
    'fields.content.pk-full-text.index.options' = '{"tokenizer":"jieba"}'
);
```

Paimon creates one native archive for the complete set of eligible `COMPACT` data files in each
Level-1-or-higher data level. Data compaction replaces the affected level archive atomically. One
archive can cover multiple ordered source files; its row IDs concatenate their physical row
positions.

Primary-key full-text search currently supports only `global-index.search-mode=fast`. Level-0 and
other uncovered files are not searched; their rows become searchable after compaction publishes
an eligible data file and persistent archive. Search applies each source file's deletion vector,
preserves native relevance scores, and selects a global Top-K. Only Hybrid search rewrites route
scores through its configured `rrf`, `weighted_score`, or `mrr` ranker.

```sql
CALL sys.full_text_search(
    `table` => 'default.articles',
    `column` => 'content',
    query => '{"match":{"column":"content","terms":"paimon lake"}}',
    top_k => 10,
    projection => 'id,content,__paimon_search_score'
);
```

See [Primary-Key Indexes](../docs/docs/primary-key-table/global-index.mdx) for requirements,
Spark and Java examples, Hybrid search, and current limitations.

### Build a Global Index

```sql
CALL sys.create_global_index(
    table => 'db.my_table',
    index_column => 'content',
    index_type => 'full-text'
);
```

### Tokenizers

By default, the native full-text index uses its built-in tokenizer. For Chinese or other languages where users often
search by short character fragments, build the index with the `ngram` tokenizer:

```sql
CALL sys.create_global_index(
    table => 'db.my_table',
    index_column => 'content',
    index_type => 'full-text',
    options => 'full-text.tokenizer=ngram,full-text.ngram.min-gram=2,full-text.ngram.max-gram=2'
);
```

For Chinese word segmentation, build the index with the `jieba` tokenizer:

```sql
CALL sys.create_global_index(
    table => 'db.my_table',
    index_column => 'content',
    index_type => 'full-text',
    options => 'full-text.tokenizer=jieba'
);
```

Available tokenizer options:

| Option | Default | Description |
|--------|---------|-------------|
| `full-text.tokenizer` | `default` | Tokenizer used by the full-text index. Supported values: `default`, `simple`, `whitespace`, `raw`, `ngram`, `jieba`. |
| `full-text.ngram.min-gram` | `2` | Minimum gram length for the `ngram` tokenizer. |
| `full-text.ngram.max-gram` | `2` | Maximum gram length for the `ngram` tokenizer. |
| `full-text.ngram.prefix-only` | `false` | Whether the `ngram` tokenizer only emits prefix ngrams. |
| `full-text.lower-case` | `true` | Whether configurable tokenizers lowercase emitted tokens. |
| `full-text.max-token-length` | `40` | Maximum token length kept by configurable tokenizers. |
| `full-text.ascii-folding` | `false` | Whether to normalize non-ASCII Latin characters to ASCII. |
| `full-text.stem` | `false` | Whether to apply stemming to emitted tokens. |
| `full-text.language` | `english` | Language used by stemming and built-in stop word filters. |
| `full-text.remove-stop-words` | `false` | Whether to remove built-in stop words for the configured language. |
| `full-text.stop-words` | ` ` | Semicolon-separated custom stop words to remove. |
| `full-text.with-position` | `true` | Whether to store term positions for phrase queries. |

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
    'content',
    '{"match":{"query":"search query"}}',
    10
);
```

### Java API

```java
Table table = catalog.getTable(identifier);

GlobalIndexResult result = table.newFullTextSearchBuilder()
        .withQuery("content", "{\"match\":{\"query\":\"search query\"}}")
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

The index type `full-text` is registered via Java SPI:

```
META-INF/services/org.apache.paimon.globalindex.GlobalIndexerFactory
  → org.apache.paimon.fulltext.index.NativeFullTextGlobalIndexerFactory
```
