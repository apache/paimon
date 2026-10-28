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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.StreamTableScan;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for generating changelog during reads. */
public class ChangelogOnReadTest extends TableTestBase {

    @Test
    public void testAggregationProjectionMultipleLevel0AndCacheLifecycle() throws Exception {
        FileStoreTable table = createTable(CoreOptions.MergeEngine.AGGREGATE);
        write(table, GenericRow.of(1, 1, 1, 1));

        StreamTableScan scan = table.newStreamScan();
        ReadBuilder readBuilder = table.newReadBuilder().withProjection(new int[] {3});
        Path lookupDir = tempPath.resolve("lookup");
        IOManager lookupIO = IOManager.create(lookupDir.toString());
        TableRead read = readBuilder.newRead().withIOManager(lookupIO);
        Path firstLookupFile = null;
        try {
            TableScan.Plan initial = scan.plan();
            String firstDataFile =
                    ((DataSplit) initial.splits().get(0)).dataFiles().get(0).fileName();
            assertThat(read(read, initial)).containsExactly("+I 1");

            write(table, GenericRow.of(1, 1, 2, 2));
            assertThat(readAfterLookupPrepared(read, scan.plan(), lookupDir, firstDataFile))
                    .containsExactly("-U 1", "+U 3");
            firstLookupFile = findLookupFile(lookupDir, firstDataFile);
            long modified = Files.getLastModifiedTime(firstLookupFile).toMillis();

            write(table, GenericRow.of(1, 1, 0, 3));
            assertThat(read(read, scan.plan())).containsExactly("-U 3", "+U 6");
            assertThat(firstLookupFile).exists();
            assertThat(Files.getLastModifiedTime(firstLookupFile).toMillis()).isEqualTo(modified);

            // The temporary history filter for partition 1 must not affect the next delta scan.
            write(table, GenericRow.of(2, 1, 1, 4));
            assertThat(read(read, scan.plan())).containsExactly("+I 4");
        } finally {
            read.close();
            if (firstLookupFile != null) {
                assertThat(firstLookupFile).doesNotExist();
            }
            lookupIO.close();
        }
    }

    @Test
    public void testDeduplicateMultipleLevel0() throws Exception {
        FileStoreTable table = createTable(CoreOptions.MergeEngine.DEDUPLICATE);
        write(table, GenericRow.of(1, 1, 1, 10));

        StreamTableScan scan = table.newStreamScan();
        ReadBuilder readBuilder = table.newReadBuilder().withProjection(new int[] {3});
        try (IOManager lookupIO = IOManager.create(tempPath.resolve("lookup").toString());
                TableRead read = readBuilder.newRead().withIOManager(lookupIO)) {
            assertThat(read(read, scan.plan())).containsExactly("+I 10");

            write(table, GenericRow.of(1, 1, 3, 30));
            assertThat(read(read, scan.plan())).containsExactly("-U 10", "+U 30");

            write(table, GenericRow.of(1, 1, 4, 40));
            assertThat(read(read, scan.plan())).containsExactly("-U 30", "+U 40");
        }
    }

    private FileStoreTable createTable(CoreOptions.MergeEngine mergeEngine) throws Exception {
        Schema.Builder builder =
                Schema.newBuilder()
                        .column("p", DataTypes.INT())
                        .column("k", DataTypes.INT())
                        .column("seq", DataTypes.INT())
                        .column("v", DataTypes.INT())
                        .primaryKey("p", "k")
                        .partitionKeys("p")
                        .option(CoreOptions.BUCKET.key(), "1")
                        .option(CoreOptions.WRITE_ONLY.key(), "true")
                        .option(CoreOptions.CHANGELOG_PRODUCER.key(), "on-read")
                        .option(CoreOptions.MERGE_ENGINE.key(), mergeEngine.toString())
                        .option(CoreOptions.SEQUENCE_FIELD.key(), "seq");
        if (mergeEngine == CoreOptions.MergeEngine.AGGREGATE) {
            builder.option("fields.v.aggregate-function", "sum");
        }
        catalog.createTable(identifier(), builder.build(), false);
        return getTableDefault();
    }

    private List<String> read(TableRead read, TableScan.Plan plan) throws IOException {
        List<String> result = new ArrayList<>();
        try (RecordReader<InternalRow> reader = read.createReader(plan)) {
            reader.forEachRemaining(
                    row -> result.add(row.getRowKind().shortString() + " " + row.getInt(0)));
        }
        return result;
    }

    private List<String> readAfterLookupPrepared(
            TableRead read, TableScan.Plan plan, Path lookupDir, String dataFile)
            throws IOException {
        List<String> result = new ArrayList<>();
        try (RecordReader<InternalRow> reader = read.createReader(plan)) {
            RecordReader.RecordIterator<InternalRow> batch = reader.readBatch();
            assertThat(batch).isNotNull();
            assertThat(findLookupFile(lookupDir, dataFile)).exists();
            consume(batch, result);
            while ((batch = reader.readBatch()) != null) {
                consume(batch, result);
            }
        }
        return result;
    }

    private void consume(RecordReader.RecordIterator<InternalRow> batch, List<String> result)
            throws IOException {
        InternalRow row;
        while ((row = batch.next()) != null) {
            result.add(row.getRowKind().shortString() + " " + row.getInt(0));
        }
        batch.releaseBatch();
    }

    private Path findLookupFile(Path lookupDir, String dataFile) throws IOException {
        try (Stream<Path> paths = Files.walk(lookupDir)) {
            return paths.filter(Files::isRegularFile)
                    .filter(path -> path.getFileName().toString().contains(dataFile))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("Lookup file was not cached."));
        }
    }
}
