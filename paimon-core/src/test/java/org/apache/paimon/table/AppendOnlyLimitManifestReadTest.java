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

package org.apache.paimon.table;

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.FileSystemSchemaManager;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.CloseableIterator;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/** Verifies actual manifest I/O, as well as result rows, for append-only LIMIT scans. */
class AppendOnlyLimitManifestReadTest {

    @TempDir java.nio.file.Path tempDir;

    private final CountingFileIO fileIO = new CountingFileIO();

    @ParameterizedTest
    @ValueSource(ints = {1, 10, 100})
    void testLimitStopsManifestReads(int limit) throws Exception {
        FileStoreTable table = createTable();
        writeManifests(table, 20);

        fileIO.manifestReads.set(0);
        ReadBuilder read = table.newReadBuilder().withLimit(limit);
        TableScan.Plan plan = read.newScan().plan();
        assertThat(fileIO.manifestReads.get()).isEqualTo(Math.min(20, (limit + 3) / 4));
        List<Integer> rows = readIds(read, plan, limit);
        assertThat(rows).hasSize(Math.min(limit, 80)).doesNotHaveDuplicates();
        assertThat(rows).allMatch(id -> id >= 0 && id < 80);
    }

    @Test
    void testUnpartitionedLimit() throws Exception {
        FileStoreTable table = createTable(false);
        writeManifests(table, 20);

        fileIO.manifestReads.set(0);
        ReadBuilder read = table.newReadBuilder().withLimit(10);
        TableScan.Plan plan = read.newScan().plan();
        assertThat(fileIO.manifestReads.get()).isEqualTo(3);
        assertThat(readIds(read, plan, 10)).hasSize(10).doesNotHaveDuplicates();
    }

    @Test
    void testNoLimitReadsAllManifests() throws Exception {
        FileStoreTable table = createTable();
        writeManifests(table, 20);

        fileIO.manifestReads.set(0);
        ReadBuilder read = table.newReadBuilder();
        TableScan.Plan plan = read.newScan().plan();
        assertThat(fileIO.manifestReads.get()).isEqualTo(20);
        assertThat(readIds(read, plan)).hasSize(80).doesNotHaveDuplicates();
    }

    @Test
    void testDataFilterFindsRowsInLaterManifests() throws Exception {
        FileStoreTable table = createTable();
        writeManifests(table, 20);

        fileIO.manifestReads.set(0);
        ReadBuilder read =
                table.newReadBuilder()
                        .withFilter(new PredicateBuilder(table.rowType()).greaterOrEqual(0, 76))
                        .withLimit(10);
        TableScan.Plan plan = read.newScan().plan();
        assertThat(fileIO.manifestReads.get()).isEqualTo(20);
        assertThat(readIds(read, plan)).containsExactlyInAnyOrder(76, 77, 78, 79);
    }

    @Test
    void testPartitionFilterCanStopEarly() throws Exception {
        FileStoreTable table = createTable();
        writeManifests(table, 20);

        fileIO.manifestReads.set(0);
        ReadBuilder read =
                table.newReadBuilder()
                        .withFilter(new PredicateBuilder(table.rowType()).equal(1, 1))
                        .withLimit(10);
        TableScan.Plan plan = read.newScan().plan();
        assertThat(fileIO.manifestReads.get()).isEqualTo(3);
        assertThat(readIds(read, plan, 10))
                .hasSize(10)
                .doesNotHaveDuplicates()
                .allMatch(id -> (id / 4) % 2 == 1);
    }

    @Test
    void testLaterDeletesAreAppliedBeforeLimit() throws Exception {
        FileStoreTable table = createTable();
        writeManifests(table, 4);
        // The oldest ADD entries belong to partition 0. Their DELETE entries occur later.
        try (BatchTableCommit commit = table.newBatchWriteBuilder().newCommit()) {
            commit.truncatePartitions(
                    Collections.singletonList(Collections.singletonMap("p", "0")));
        }

        ReadBuilder read = table.newReadBuilder().withLimit(10);
        TableScan.Plan plan = read.newScan().plan();
        assertThat(readIds(read, plan)).containsExactlyInAnyOrder(4, 5, 6, 7, 12, 13, 14, 15);
    }

    @Test
    void testEmptyTable() throws Exception {
        FileStoreTable table = createTable();
        ReadBuilder read = table.newReadBuilder().withLimit(1);
        assertThat(read.newScan().plan().splits()).isEmpty();
        assertThat(fileIO.manifestReads.get()).isZero();
    }

    private FileStoreTable createTable() throws Exception {
        return createTable(true);
    }

    private FileStoreTable createTable(boolean partitioned) throws Exception {
        Path path = new Path(tempDir.toUri());
        Schema.Builder schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("p", DataTypes.INT())
                        .option("bucket", "-1")
                        .option("file.format", "avro")
                        .option("write-only", "true")
                        .option("manifest.merge-min-count", "1000")
                        .option("scan.manifest.parallelism", "1");
        if (partitioned) {
            schema.partitionKeys("p");
        }
        new FileSystemSchemaManager(fileIO, path).createTable(schema.build());
        return FileStoreTableFactory.create(fileIO, path);
    }

    private void writeManifests(FileStoreTable table, int count) throws Exception {
        for (int i = 0; i < count; i++) {
            BatchWriteBuilder builder = table.newBatchWriteBuilder();
            try (BatchTableWrite write = builder.newWrite();
                    BatchTableCommit commit = builder.newCommit()) {
                for (int j = 0; j < 4; j++) {
                    write.write(GenericRow.of(i * 4 + j, i % 2));
                }
                commit.commit(write.prepareCommit());
            }
        }
        assertThat(
                        table.store()
                                .manifestListFactory()
                                .create()
                                .readDataManifests(table.latestSnapshot().get()))
                .hasSize(count);
    }

    private List<Integer> readIds(ReadBuilder read, TableScan.Plan plan) throws Exception {
        return readIds(read, plan, Integer.MAX_VALUE);
    }

    private List<Integer> readIds(ReadBuilder read, TableScan.Plan plan, int limit)
            throws Exception {
        List<Integer> result = new ArrayList<>();
        RecordReader<InternalRow> reader = read.newRead().executeFilter().createReader(plan);
        // Scan pruning retains whole files; the consuming engine enforces the global row limit.
        try (CloseableIterator<InternalRow> rows = reader.toCloseableIterator()) {
            while (result.size() < limit && rows.hasNext()) {
                result.add(rows.next().getInt(0));
            }
        }
        return result;
    }

    private static class CountingFileIO extends LocalFileIO {
        private static final long serialVersionUID = 1L;

        private final AtomicInteger manifestReads = new AtomicInteger();

        @Override
        public SeekableInputStream newInputStream(Path path) throws IOException {
            String name = path.getName();
            if (name.startsWith("manifest-") && !name.startsWith("manifest-list-")) {
                manifestReads.incrementAndGet();
            }
            return super.newInputStream(path);
        }
    }
}
