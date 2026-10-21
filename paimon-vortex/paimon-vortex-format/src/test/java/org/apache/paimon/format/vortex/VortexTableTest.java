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

package org.apache.paimon.format.vortex;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests Vortex files through Paimon tables. */
class VortexTableTest {

    @TempDir java.nio.file.Path tempDir;

    private Catalog catalog;

    @BeforeEach
    void before() throws Exception {
        catalog =
                CatalogFactory.createCatalog(
                        CatalogContext.create(new Path(tempDir.toUri().toString())));
        catalog.createDatabase("db", false);
    }

    @Test
    void testAppendTableHasFileStatsAndFilters() throws Exception {
        FileStoreTable table =
                createTable(
                        "append",
                        Schema.newBuilder()
                                .column("id", DataTypes.INT())
                                .column("name", DataTypes.STRING())
                                .option("file.format", "vortex")
                                .option("bucket", "-1")
                                .build());

        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = writeBuilder.newWrite();
                BatchTableCommit commit = writeBuilder.newCommit()) {
            for (int i = 0; i < 10_000; i++) {
                write.write(GenericRow.of(i, BinaryString.fromString("name-" + i)));
            }
            commit.commit(write.prepareCommit());
        }

        // Without a stats extractor, Paimon silently stored no stats for Vortex files, so scans
        // could never skip one.
        List<DataSplit> splits = table.newSnapshotReader().read().dataSplits();
        DataFileMeta file = splits.get(0).dataFiles().get(0);
        assertThat(file.valueStats().minValues().getInt(0)).isEqualTo(0);
        assertThat(file.valueStats().maxValues().getInt(0)).isEqualTo(9_999);
        assertThat(file.rowCount()).isEqualTo(10_000);

        PredicateBuilder builder = new PredicateBuilder(table.rowType());
        assertThat(
                        table.newReadBuilder()
                                .withFilter(builder.greaterThan(0, 10_000))
                                .newScan()
                                .plan()
                                .splits())
                .isEmpty();

        List<String> rows = read(table, builder.between(0, 5_000, 5_004));
        assertThat(rows)
                .containsExactly(
                        "5000:name-5000",
                        "5001:name-5001",
                        "5002:name-5002",
                        "5003:name-5003",
                        "5004:name-5004");
    }

    @Test
    void testPrimaryKeyTableWithDeletionVectorsAndFilter() throws Exception {
        FileStoreTable table =
                createTable(
                        "pk",
                        Schema.newBuilder()
                                .column("id", DataTypes.INT())
                                .column("name", DataTypes.STRING())
                                .primaryKey("id")
                                .option("file.format", "vortex")
                                .option("bucket", "1")
                                .option("deletion-vectors.enabled", "true")
                                .build());

        StreamWriteBuilder writeBuilder = table.newStreamWriteBuilder();
        try (IOManager ioManager = IOManager.create(tempDir.resolve("io").toString());
                StreamTableWrite write = writeBuilder.newWrite();
                StreamTableCommit commit = writeBuilder.newCommit()) {
            write.withIOManager(ioManager);
            for (int i = 0; i < 10_000; i++) {
                write.write(GenericRow.of(i, BinaryString.fromString("v1-" + i)));
            }
            write.compact(BinaryRow.EMPTY_ROW, 0, true);
            commit.commit(0, write.prepareCommit(true, 0));

            // Updating keys of the compacted file marks their old rows in a deletion vector.
            for (int i = 9_990; i < 10_000; i += 3) {
                write.write(GenericRow.of(i, BinaryString.fromString("v2-" + i)));
            }
            write.compact(BinaryRow.EMPTY_ROW, 0, false);
            commit.commit(1, write.prepareCommit(true, 1));
        }

        List<DataSplit> splits = table.newSnapshotReader().read().dataSplits();
        assertThat(splits.get(0).deletionFiles()).isPresent();

        // The filter drops rows natively, so deletion vectors only hit the right rows when the
        // reader reports file positions.
        PredicateBuilder builder = new PredicateBuilder(table.rowType());
        List<String> rows = read(table, builder.greaterOrEqual(0, 9_988));
        List<String> expected = new ArrayList<>();
        for (int i = 9_988; i < 10_000; i++) {
            expected.add(i + ":" + ((i - 9_990) % 3 == 0 && i >= 9_990 ? "v2-" : "v1-") + i);
        }
        assertThat(rows).containsExactlyInAnyOrderElementsOf(expected);
    }

    private FileStoreTable createTable(String name, Schema schema) throws Exception {
        Identifier identifier = Identifier.create("db", name);
        catalog.createTable(identifier, schema, false);
        return (FileStoreTable) catalog.getTable(identifier);
    }

    private static List<String> read(FileStoreTable table, Predicate filter) throws Exception {
        ReadBuilder readBuilder = table.newReadBuilder().withFilter(filter);
        List<String> result = new ArrayList<>();
        try (RecordReader<InternalRow> reader =
                        readBuilder
                                .newRead()
                                .executeFilter()
                                .createReader(readBuilder.newScan().plan());
                RecordReaderIterator<InternalRow> iterator = new RecordReaderIterator<>(reader)) {
            while (iterator.hasNext()) {
                InternalRow row = iterator.next();
                result.add(row.getInt(0) + ":" + row.getString(1));
            }
        }
        return result;
    }
}
