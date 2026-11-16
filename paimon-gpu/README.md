# Paimon GPU compaction

`paimon-gpu` runs primary-key compactions on NVIDIA GPUs through [TornadoVM](https://github.com/beehive-lab/TornadoVM)
and cuDF. It is a `CompactRewriterFactory`: Paimon keeps choosing what to compact, when, and how to
commit, and only the merge of a compaction's files runs on the device.

```java
try (GpuCompactRewriterFactory gpu = new GpuCompactRewriterFactory(table)) {
    write.withCompactRewriterFactory(gpu);
    write.compact(partition, bucket, true);
}
```

## What runs on the device

For each batch of sections:
1. cuDF reads the Parquet files; keys and `_SEQUENCE_NUMBER` go into INT64 columns.
2. cuDF sorts the rows by key, then by sequence number.
3. A kernel keeps the last row of each key, and drops retractions when the output level drops
   deletes.
4. The survivors are gathered and written by cuDF to one Parquet file.

The `DataFileMeta` is built from the written footer by Paimon's own Parquet stats extractor.

Supported tables:
- the `deduplicate` merge engine, without `sequence.field`, `ignore-delete`, record-level
  expiration, thin mode, deletion vectors, lookup changelog or file indexes;
- Parquet with zstd, snappy or no compression;
- INT or BIGINT keys; INT, BIGINT, DOUBLE, DATE and STRING values;
- local files.

Everything else, and any device failure, goes to Paimon's rewriter. The table option
`gpu.compaction.max-batch-rows` caps the rows of one device batch.

## Build and test

The module needs JDK 22 or later and is built with `-Pgpu`. The device test runs in a TornadoVM JVM.
That means TornadoVM 8.1 with beehive-lab/TornadoVM#1329, and libcudf 26.08 on `LD_LIBRARY_PATH`.
Run it on its own:

```bash
mvn -Pgpu -pl paimon-gpu -am install -DskipTests
mvn -Pgpu -pl paimon-gpu test -Dtest=GpuCompactionITCase \
    -Dpaimon.gpu.testArgs="@$TORNADOVM_HOME/tornado-argfile -Dtornado.device.memory=8GB"
```

Without a TornadoVM JVM the device test is skipped.
