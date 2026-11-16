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

## Setting up TornadoVM and libcudf

The module runs in a JVM that has TornadoVM on its module path and libcudf on its library path.
These steps were checked on Linux x86_64 with an RTX 4090, NVIDIA driver 610, libcudf 26.08 (CUDA 13), and JDK 25.

**1. libcudf 26.08 for CUDA 13.** Use the RAPIDS wheels, unpacked into one directory; nothing else
is installed:

```bash
export CUDF_DIR=$HOME/cudf-cu13
mkdir -p $CUDF_DIR/wheels
python3 -m pip download --only-binary=:all: --python-version 3.12 \
    --platform manylinux_2_28_x86_64 --platform manylinux_2_17_x86_64 \
    --extra-index-url https://pypi.nvidia.com -d $CUDF_DIR/wheels "libcudf-cu13==26.8.*"
for w in $CUDF_DIR/wheels/*.whl; do python3 -m zipfile -e "$w" $CUDF_DIR; done
export CUDF_HOME=$CUDF_DIR/libcudf RMM_HOME=$CUDF_DIR/librmm
export LD_LIBRARY_PATH=$(find $CUDF_DIR -name '*.so*' -type f -printf '%h\n' | sort -u | paste -sd:)
```

**2. A TornadoVM SDK with the cuDF tasks.** The Parquet tasks this module uses need
beehive-lab/TornadoVM#1329: Paimon's `_VALUE_KIND` is an `INT(8)` column, and compactions mix files
written by parquet-mr and by libcudf. Until a release contains it, build the SDK from that PR, with
the environment from step 1 so that the cuDF library is built too:

```bash
git clone https://github.com/beehive-lab/TornadoVM.git && cd TornadoVM
git fetch origin pull/1329/head:cudf-1329 && git checkout cudf-1329
export JAVA_HOME=/path/to/jdk-25
make BACKEND=cuda
source setvars.sh            # sets TORNADOVM_HOME to the SDK under dist/
tornado --devices            # lists the GPU
ls $TORNADOVM_HOME/lib/libtornado-cudf.so
```

Once a release contains #1329, `sdk install tornadovm <version>-jdk22plus-cuda` (SDKMAN) replaces
this step. Always point `TORNADOVM_HOME` at a versioned SDK directory, not at SDKMAN's `current`.

**3. Run the JVM with TornadoVM.** Add the SDK's argument file, and give TornadoVM device memory for
the largest batch (about 150 bytes a row for six columns):

```bash
java @$TORNADOVM_HOME/tornado-argfile -Dtornado.device.memory=16GB \
    -cp <your job with paimon-bundle and paimon-gpu> YourCompactionJob
```

`LD_LIBRARY_PATH` must hold the libcudf directories from step 1 in that JVM's environment.

## Using it

The rewriter is installed on a table write through `TableWrite.withCompactRewriterFactory`, before the
write opens any bucket:

```java
FileStoreTable table = (FileStoreTable) catalog.getTable(identifier);
try (GpuCompactRewriterFactory gpu = new GpuCompactRewriterFactory(table);
        TableWriteImpl<?> write = table.newWrite(commitUser).withCompactRewriterFactory(gpu);
        TableCommitImpl commit = table.newCommit(commitUser)) {
    write.compact(partition, bucket, true);           // for each bucket to compact
    commit.commit(commitIdentifier, write.prepareCommit(true, commitIdentifier));
    LOG.info(gpu.stats());                            // device batches, rows, and any CPU fallbacks
}
```

`stats()` reports how many compactions ran on the device and why any went to Paimon's rewriter
instead. Flink's and Spark's compaction actions do not install a factory yet, so for now the
rewriter is used from Java jobs such as the one above.

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
