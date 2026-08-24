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

package org.apache.paimon.table.system;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.paimon.catalog.Identifier.SYSTEM_TABLE_SPLITTER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link FileIndexesTable}. */
public class FileIndexesTableTest extends TableTestBase {

    @Test
    public void testEmbeddedFileIndexes() throws Exception {
        FileIndexesTable fileIndexesTable = createTable("EmbeddedIndexes", "1 MB");

        List<InternalRow> rows = read(fileIndexesTable);
        assertIndexRows(rows, "EMBEDDED");
        assertThat(rows).allMatch(row -> row.isNullAt(9));
        assertThat(rows)
                .allMatch(
                        row ->
                                row.getLong(11) > row.getLong(10)
                                        && row.getLong(11) > 0
                                        && !row.getBoolean(12));
    }

    @Test
    public void testExternalFileIndexes() throws Exception {
        FileIndexesTable fileIndexesTable = createTable("ExternalIndexes", "1 B");

        List<InternalRow> rows = read(fileIndexesTable);
        assertIndexRows(rows, "FILE");
        assertThat(rows)
                .allMatch(
                        row ->
                                row.getString(9).toString().endsWith(".index")
                                        && row.getLong(11) > row.getLong(10)
                                        && row.getLong(11) > 0
                                        && !row.getBoolean(12));
        assertThat(
                        rows.stream()
                                .map(row -> row.getString(9).toString())
                                .distinct()
                                .collect(Collectors.toList()))
                .hasSize(1);
    }

    @Test
    public void testLazyReadExternalFileIndexes() throws Exception {
        String tableName = "LazyExternalIndexes";
        FileIndexesTable fileIndexesTable = createTable(tableName, "1 B", true);
        FileStoreTable dataTable = (FileStoreTable) catalog.getTable(identifier(tableName));

        ReadBuilder readBuilder = fileIndexesTable.newReadBuilder();
        List<Split> systemSplits = readBuilder.newScan().plan().splits();
        assertThat(systemSplits).hasSize(1);

        List<DataSplit> dataSplits =
                ((FilesTable.FilesSplit) systemSplits.get(0))
                        .splits(dataTable).stream()
                                .map(DataSplit.class::cast)
                                .collect(Collectors.toList());
        List<DataFileMeta> dataFiles =
                dataSplits.stream()
                        .flatMap(split -> split.dataFiles().stream())
                        .collect(Collectors.toList());
        assertThat(dataFiles).hasSize(2);

        DataSplit firstSplit = dataSplits.get(0);
        DataFileMeta secondFile = dataFiles.get(1);
        String secondIndexFile =
                secondFile.extraFiles().stream()
                        .filter(name -> name.endsWith(DataFilePathFactory.INDEX_PATH_SUFFIX))
                        .findFirst()
                        .orElseThrow(AssertionError::new);
        Path secondIndexPath =
                dataTable
                        .store()
                        .pathFactory()
                        .createDataFilePathFactory(firstSplit.partition(), firstSplit.bucket())
                        .toAlignedPath(secondIndexFile, secondFile);
        try (PositionOutputStream output =
                dataTable.fileIO().newOutputStream(secondIndexPath, true)) {
            output.write(0);
        }

        try (RecordReader<InternalRow> reader =
                readBuilder.newRead().createReader(systemSplits.get(0))) {
            RecordReader.RecordIterator<InternalRow> batch = reader.readBatch();
            InternalRow first = batch.next();
            InternalRow second = batch.next();
            assertThat(first).isNotNull();
            assertThat(second).isNotNull();
            assertThat(first.getString(2)).isEqualTo(second.getString(2));
            assertThatThrownBy(batch::next)
                    .isInstanceOf(UncheckedIOException.class)
                    .hasMessageContaining(secondIndexPath.toString());
            batch.releaseBatch();
        }
    }

    @Test
    public void testFilters() throws Exception {
        FileIndexesTable fileIndexesTable = createTable("FilteredIndexes", "1 MB");
        PredicateBuilder builder = new PredicateBuilder(FileIndexesTable.TABLE_TYPE);

        List<InternalRow> bitmapRows =
                readWithFilter(
                        fileIndexesTable, builder.equal(7, BinaryString.fromString("bitmap")));
        assertThat(bitmapRows).hasSize(1);
        assertThat(bitmapRows.get(0).getString(7).toString()).isEqualTo("bitmap");

        List<InternalRow> partitionRows =
                readWithFilter(fileIndexesTable, builder.equal(0, BinaryString.fromString("{1}")));
        assertThat(partitionRows).hasSize(2);

        assertThat(
                        readWithFilter(
                                fileIndexesTable,
                                PredicateBuilder.and(
                                        builder.greaterThan(1, 0), builder.lessThan(1, 1))))
                .isEmpty();

        String filePath = partitionRows.get(0).getString(2).toString();
        List<InternalRow> fileRows =
                readWithFilter(
                        fileIndexesTable, builder.equal(2, BinaryString.fromString(filePath)));
        assertThat(fileRows).hasSize(2);

        assertThat(
                        readWithFilter(
                                fileIndexesTable,
                                builder.equal(2, BinaryString.fromString(filePath + ".missing"))))
                .isEmpty();

        Predicate mixedOr =
                PredicateBuilder.or(
                        builder.equal(0, BinaryString.fromString("{2}")),
                        builder.equal(7, BinaryString.fromString("bitmap")));
        List<InternalRow> mixedOrRows = readWithFilter(fileIndexesTable, mixedOr);
        assertThat(mixedOrRows).hasSize(1);
        assertThat(mixedOrRows.get(0).getString(7).toString()).isEqualTo("bitmap");
    }

    @Test
    public void testTableWithoutFileIndexes() throws Exception {
        Identifier identifier = identifier("NoIndexes");
        catalog.createTable(
                identifier,
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .option(CoreOptions.BUCKET.key(), "1")
                        .option(CoreOptions.BUCKET_KEY.key(), "id")
                        .build(),
                false);
        write((FileStoreTable) catalog.getTable(identifier), GenericRow.of(1));

        FileIndexesTable fileIndexesTable =
                (FileIndexesTable)
                        catalog.getTable(
                                identifier(
                                        "NoIndexes"
                                                + SYSTEM_TABLE_SPLITTER
                                                + FileIndexesTable.FILE_INDEXES));
        assertThat(read(fileIndexesTable)).isEmpty();
    }

    private FileIndexesTable createTable(String tableName, String inManifestThreshold)
            throws Exception {
        return createTable(tableName, inManifestThreshold, false);
    }

    private FileIndexesTable createTable(
            String tableName, String inManifestThreshold, boolean writeSeparateFiles)
            throws Exception {
        Identifier identifier = identifier(tableName);
        catalog.createTable(
                identifier,
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("pt", DataTypes.INT())
                        .partitionKeys("pt")
                        .option(CoreOptions.BUCKET.key(), "1")
                        .option(CoreOptions.BUCKET_KEY.key(), "id")
                        .option("file-index.bitmap.columns", "id")
                        .option("file-index.bloom-filter.columns", "id")
                        .option(
                                CoreOptions.FILE_INDEX_IN_MANIFEST_THRESHOLD.key(),
                                inManifestThreshold)
                        .build(),
                false);
        FileStoreTable table = (FileStoreTable) catalog.getTable(identifier);
        if (writeSeparateFiles) {
            write(table, GenericRow.of(1, 1));
            write(table, GenericRow.of(2, 1));
        } else {
            write(table, GenericRow.of(1, 1), GenericRow.of(2, 1));
        }

        return (FileIndexesTable)
                catalog.getTable(
                        identifier(
                                tableName + SYSTEM_TABLE_SPLITTER + FileIndexesTable.FILE_INDEXES));
    }

    private static void assertIndexRows(List<InternalRow> rows, String storageType) {
        assertThat(rows).hasSize(2);
        assertThat(rows).extracting(row -> row.getString(6).toString()).containsOnly("id");
        assertThat(rows)
                .extracting(row -> row.getString(7).toString())
                .containsExactlyInAnyOrder("bitmap", "bloom-filter");
        assertThat(rows).extracting(row -> row.getString(8).toString()).containsOnly(storageType);
        assertThat(rows).allMatch(row -> row.getString(0).toString().equals("{1}"));
        assertThat(rows).allMatch(row -> row.getInt(1) == 0);
        assertThat(rows).allMatch(row -> row.getLong(3) > 0);
        assertThat(rows).allMatch(row -> row.getLong(4) == 2);
        assertThat(rows).allMatch(row -> row.getLong(5) == 0);
        assertThat(rows).allMatch(row -> row.getLong(10) > 0);
        assertThat(
                        rows.stream()
                                .map(row -> row.getString(2).toString())
                                .distinct()
                                .collect(Collectors.toList()))
                .hasSize(1);
    }

    private static List<InternalRow> readWithFilter(FileIndexesTable table, Predicate predicate)
            throws Exception {
        ReadBuilder readBuilder = table.newReadBuilder().withFilter(predicate);
        List<InternalRow> rows = new ArrayList<>();
        try (RecordReader<InternalRow> reader =
                readBuilder.newRead().createReader(readBuilder.newScan().plan())) {
            reader.forEachRemaining(rows::add);
        }
        return rows;
    }
}
