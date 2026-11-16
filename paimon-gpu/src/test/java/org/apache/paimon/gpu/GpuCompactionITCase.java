/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.gpu;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.schema.FileSystemSchemaManager;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaUtils;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SplittableRandom;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Compacts two copies of one table, one with Paimon's rewriter and one on the device, and requires
 * the same rows and the same data-file metadata after every step. Runs only in a TornadoVM JVM with
 * the cuDF Parquet tasks (see the module's pom); skipped otherwise.
 */
public class GpuCompactionITCase {

    private static final int KEYS = 200_000;
    private static final int BUCKETS = 2;

    @TempDir java.nio.file.Path tempDir;

    private long commitId;

    @BeforeEach
    public void deviceMustBeAvailable() {
        boolean available;
        try {
            available =
                    System.getProperties().getProperty("tornado.load.runtime.implementation")
                                    != null
                            && uk.ac.manchester.tornado.cudf.Cudf.isParquetAvailable()
                            && uk.ac.manchester.tornado.cudf.Cudf.isWriterAvailable();
        } catch (Throwable e) {
            available = false;
        }
        assumeTrue(available, "no TornadoVM device with the cuDF Parquet tasks");
    }

    @Test
    public void testDeviceCompactionsMatchPaimons() throws Exception {
        java.nio.file.Path cpuDir = tempDir.resolve("cpu");
        java.nio.file.Path gpuDir = tempDir.resolve("gpu");
        FileStoreTable cpu = createTable(cpuDir);
        SplittableRandom random = new SplittableRandom(42);
        write(cpu, true, random, 1.0, 0);
        for (int round = 1; round <= 2; round++) {
            write(cpu, false, random, 0.3, round);
        }
        copy(cpuDir, gpuDir);
        FileStoreTable gpu = load(gpuDir);

        try (GpuCompactRewriterFactory factory = new GpuCompactRewriterFactory(gpu)) {
            compact(cpu, null, true);
            compact(gpu, factory, true);
            assertSame(cpu, gpu);

            // Trigger 2 makes Paimon write level-1 outputs too, which keep retractions; the inputs
            // now mix files Paimon wrote with files the device wrote.
            for (int step = 0; step < 3; step++) {
                write(cpu, false, new SplittableRandom(100 + step), 0.3, 10 + step);
                write(gpu, false, new SplittableRandom(100 + step), 0.3, 10 + step);
                compactPartial(cpu, null);
                compactPartial(gpu, factory);
                assertSame(cpu, gpu);
            }

            compact(cpu, null, true);
            compact(gpu, factory, true);
            assertSame(cpu, gpu);
            assertThat(factory.stats())
                    .doesNotContain("device batches=0")
                    .contains("cpu compactions=0");
        }
    }

    private FileStoreTable createTable(java.nio.file.Path dir) throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put("bucket", Integer.toString(BUCKETS));
        options.put("file.format", "parquet");
        options.put("file.compression", "zstd");
        options.put("write-only", "true");
        RowType rowType =
                RowType.of(
                        new DataType[] {
                            DataTypes.BIGINT().notNull(),
                            DataTypes.INT(),
                            DataTypes.DOUBLE(),
                            DataTypes.BIGINT(),
                            DataTypes.INT(),
                            DataTypes.STRING()
                        },
                        new String[] {"k", "qty", "price", "ts", "flag", "comment"});
        Path path = new Path(dir.toUri());
        TableSchema schema =
                SchemaUtils.forceCommit(
                        new FileSystemSchemaManager(LocalFileIO.create(), path),
                        new Schema(
                                rowType.getFields(),
                                Collections.emptyList(),
                                Collections.singletonList("k"),
                                options,
                                ""));
        return FileStoreTableFactory.create(LocalFileIO.create(), path, schema);
    }

    private static FileStoreTable load(java.nio.file.Path dir) {
        return FileStoreTableFactory.create(LocalFileIO.create(), new Path(dir.toUri()));
    }

    private void write(
            FileStoreTable table, boolean base, SplittableRandom random, double share, int round)
            throws Exception {
        long id = ++commitId;
        try (TableWriteImpl<?> write = table.newWrite("test");
                TableCommitImpl commit = table.newCommit("test")) {
            for (long i = 0; i < KEYS; i++) {
                if (!base && random.nextDouble() >= share) {
                    continue;
                }
                long key = base ? i : random.nextLong(KEYS);
                if (!base && random.nextDouble() < 0.02) {
                    write.write(GenericRow.ofKind(RowKind.DELETE, key, 0, 0.0, 0L, 0, null));
                    continue;
                }
                write.write(
                        GenericRow.of(
                                key,
                                random.nextInt(20) == 0 ? null : random.nextInt(1, 51),
                                Math.round(random.nextDouble(1, 100000) * 100) / 100.0,
                                1_700_000_000_000L + random.nextLong(1_000_000_000L),
                                round,
                                random.nextInt(10) == 0
                                        ? null
                                        : BinaryString.fromString(
                                                "c"
                                                        + round
                                                        + " "
                                                        + Long.toHexString(random.nextLong()))));
            }
            commit.commit(id, write.prepareCommit(true, id));
        }
    }

    private void compact(FileStoreTable table, GpuCompactRewriterFactory factory, boolean full)
            throws Exception {
        Map<String, String> dynamic = new HashMap<>();
        dynamic.put("write-only", "false");
        if (!full) {
            dynamic.put("num-sorted-run.compaction-trigger", "2");
        }
        FileStoreTable compacting = table.copy(dynamic);
        long id = ++commitId;
        try (IOManagerImpl io = new IOManagerImpl(tempDir.toString());
                TableWriteImpl<?> write = compacting.newWrite("test");
                TableCommitImpl commit = compacting.newCommit("test")) {
            write.withIOManager(io);
            if (factory != null) {
                write.withCompactRewriterFactory(factory);
            }
            for (int bucket = 0; bucket < BUCKETS; bucket++) {
                write.compact(BinaryRow.EMPTY_ROW, bucket, full);
            }
            commit.commit(id, write.prepareCommit(true, id));
        }
    }

    private void compactPartial(FileStoreTable table, GpuCompactRewriterFactory factory)
            throws Exception {
        compact(table, factory, false);
    }

    private static void assertSame(FileStoreTable cpu, FileStoreTable gpu) throws Exception {
        assertThat(rows(load(path(gpu)))).isEqualTo(rows(load(path(cpu))));
        assertThat(files(load(path(gpu)))).isEqualTo(files(load(path(cpu))));
    }

    private static java.nio.file.Path path(FileStoreTable table) {
        return java.nio.file.Paths.get(table.location().toUri());
    }

    private static List<String> rows(FileStoreTable table) throws Exception {
        ReadBuilder rb = table.newReadBuilder();
        List<String> rows = new ArrayList<>();
        try (RecordReaderIterator<InternalRow> it =
                new RecordReaderIterator<>(rb.newRead().createReader(rb.newScan().plan()))) {
            while (it.hasNext()) {
                InternalRow r = it.next();
                rows.add(
                        r.getLong(0)
                                + "|"
                                + (r.isNullAt(1) ? null : r.getInt(1))
                                + "|"
                                + r.getDouble(2)
                                + "|"
                                + r.getLong(3)
                                + "|"
                                + r.getInt(4)
                                + "|"
                                + (r.isNullAt(5) ? null : r.getString(5)));
            }
        }
        Collections.sort(rows);
        return rows;
    }

    /** The latest snapshot's data files without names, sizes or times, bucket by bucket. */
    private static List<String> files(FileStoreTable table) {
        List<String> files = new ArrayList<>();
        for (DataSplit split : table.newSnapshotReader().read().dataSplits()) {
            for (DataFileMeta f : split.dataFiles()) {
                files.add(
                        String.format(
                                "bucket=%d level=%d rows=%d key=[%d,%d] seq=[%d,%d] deletes=%s"
                                        + " keyStats=%s valueStats=%s cols=%s",
                                split.bucket(),
                                f.level(),
                                f.rowCount(),
                                f.minKey().getLong(0),
                                f.maxKey().getLong(0),
                                f.minSequenceNumber(),
                                f.maxSequenceNumber(),
                                f.deleteRowCount().orElse(-1L),
                                stats(f.keyStats(), RowType.of(DataTypes.BIGINT())),
                                stats(f.valueStats(), table.rowType()),
                                f.valueStatsCols()));
            }
        }
        Collections.sort(files);
        return files;
    }

    /**
     * Stats as values. A zero bound prints without its sign: libcudf writes a zero minimum as -0.0,
     * as the Parquet format recommends, where parquet-mr writes 0.0, and both bound the same
     * values.
     */
    private static String stats(SimpleStats stats, RowType type) {
        StringBuilder b = new StringBuilder();
        for (int i = 0; i < stats.minValues().getFieldCount(); i++) {
            InternalRow.FieldGetter getter = InternalRow.createFieldGetter(type.getTypeAt(i), i);
            b.append(zero(getter.getFieldOrNull(stats.minValues())))
                    .append("..")
                    .append(zero(getter.getFieldOrNull(stats.maxValues())))
                    .append(" nulls=")
                    .append(stats.nullCounts().getLong(i))
                    .append("; ");
        }
        return b.toString();
    }

    private static Object zero(Object value) {
        if (value instanceof Double && (Double) value == 0.0) {
            return 0.0;
        }
        return value;
    }

    private static void copy(java.nio.file.Path from, java.nio.file.Path to) throws IOException {
        try (Stream<java.nio.file.Path> paths = Files.walk(from)) {
            for (java.nio.file.Path source : (Iterable<java.nio.file.Path>) paths::iterator) {
                Files.copy(
                        source,
                        to.resolve(from.relativize(source).toString()),
                        StandardCopyOption.COPY_ATTRIBUTES);
            }
        }
    }
}
