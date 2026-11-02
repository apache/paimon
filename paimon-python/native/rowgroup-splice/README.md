# Sparse row-group reuse (experimental)

This opt-in path copies unchanged compressed Parquet row groups when a row-ID update changes only a few groups. Build the helper with `cargo build --release`, then set `PYPAIMON_ROW_GROUP_SPLICE_BIN` to its absolute path and set the table option `data-evolution.row-group-reuse.enabled=true`. Both are required; otherwise the existing writer is used.

The fast path requires one latest physical Parquet file to own exactly the updated columns and the complete row-ID range. Updates spanning multiple column sources are grouped by source; each group independently uses reuse when eligible and the existing writer otherwise. All groups remain in one commit. It falls back for overlapping newer column files, deletion vectors, sidecars, nested special formats, schema changes, statistics-enabled tables, files over 2 GiB, or insufficient temporary disk. Unchanged row groups are copied as compressed column chunks. Changed row groups are read and rewritten in batches, which may split one source row group into several output groups. It downloads the complete source file before copying compressed row groups, so it reduces decode/re-encode work but does not reduce source download bytes. The output is still a complete new Parquet file. Conflict checks and commits use the existing row-ID update path.

Local tests:

```shell
PAIMON_ROWGROUP_SPLICE_TEST_BIN=/absolute/path/to/paimon-rowgroup-splice python native/rowgroup-splice/test_splice.py
PYPAIMON_ROW_GROUP_SPLICE_BIN=/absolute/path/to/paimon-rowgroup-splice python -m unittest pypaimon.tests.row_group_reuse_test
```

This is an isolated prototype. Production rollout still needs representative OSS measurements, Java/VVR compaction compatibility, failure-injection and packaging tests.
