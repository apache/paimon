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
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.predicate.SubstringTransform;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.WriterAppender;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nullable;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.paimon.data.DataFormatTestUtil.toStringNoRowKind;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link RowTrackingTable}. */
public class RowTrackingTableTest extends TableTestBase {

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testGeneratedMetadataDoesNotWarn(boolean dataEvolution) throws Exception {
        FileStoreTable table = createRowTrackingTable(dataEvolution);
        write(table, GenericRow.of(10), GenericRow.of(20));
        assertRowsWithoutMissingFieldWarnings(table, "10, 0, 1", "20, 1, 1");
    }

    @Test
    public void testGeneratedSequenceNumberDoesNotWarnForStoredRowId() throws Exception {
        FileStoreTable table = createRowTrackingTable(false);
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite();
                BatchTableCommit commit = builder.newCommit()) {
            write.withWriteType(SpecialFields.rowTypeWithRowId(table.rowType()));
            write.write(GenericRow.of(10, 42L));
            commit.commit(write.prepareCommit());
        }
        assertRowsWithoutMissingFieldWarnings(table, "10, 42, 1");
    }

    private FileStoreTable createRowTrackingTable(boolean dataEvolution) throws Exception {
        Identifier id = identifier("T");
        catalog.createTable(
                id,
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .option(CoreOptions.FILE_FORMAT.key(), "parquet")
                        .option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true")
                        .option(
                                CoreOptions.DATA_EVOLUTION_ENABLED.key(),
                                String.valueOf(dataEvolution))
                        .option(CoreOptions.BUCKET.key(), "-1")
                        .build(),
                false);
        return getTable(id);
    }

    private static void assertRowsWithoutMissingFieldWarnings(
            FileStoreTable source, String... expectedRows) throws Exception {
        Logger logger =
                (Logger)
                        LogManager.getLogger(
                                "org.apache.paimon.format.parquet.ParquetReaderFactory");
        Level previousLevel = logger.getLevel();
        StringWriter messages = new StringWriter();
        WriterAppender appender =
                WriterAppender.newBuilder()
                        .setName("row-tracking-warnings")
                        .setTarget(messages)
                        .build();
        appender.start();
        logger.addAppender(appender);
        logger.setLevel(Level.WARN);
        try {
            RowTrackingTable table = new RowTrackingTable(source);
            ReadBuilder builder = table.newReadBuilder();
            List<String> rows = new ArrayList<>();
            try (RecordReader<InternalRow> reader =
                    builder.newRead().createReader(builder.newScan().plan())) {
                reader.forEachRemaining(row -> rows.add(toStringNoRowKind(row, table.rowType())));
            }
            assertThat(rows).containsExactlyInAnyOrder(expectedRows);
            assertThat(messages.toString()).doesNotContain("does not exist");
        } finally {
            logger.removeAppender(appender);
            logger.setLevel(previousLevel);
            appender.stop();
        }
    }

    @Test
    public void testFilterOnRowTrackingFields() throws Exception {
        Identifier id = identifier("T");
        catalog.createTable(
                id,
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.INT())
                        .option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true")
                        .option(CoreOptions.BUCKET.key(), "-1")
                        .build(),
                false);
        write(catalog.getTable(id), GenericRow.of(1, 10), GenericRow.of(2, 20));
        write(catalog.getTable(id), GenericRow.of(3, 30));

        for (Integer limit : Arrays.asList(null, 1)) {
            assertThat(read(id, "_ROW_ID", 2L, limit)).containsExactly("3, 30, 2, 2");
            assertThat(read(id, "_SEQUENCE_NUMBER", 2L, limit)).containsExactly("3, 30, 2, 2");
        }

        // after a schema change, the files are read with an older schema
        catalog.alterTable(id, SchemaChange.addColumn("c", DataTypes.INT()), false);
        assertThat(read(id, "_ROW_ID", 2L, null)).containsExactly("3, 30, NULL, 2, 2");
    }

    @Test
    public void testFilterMixingRowTrackingAndDataFields() throws Exception {
        Identifier id = identifier("T");
        catalog.createTable(
                id,
                Schema.newBuilder()
                        .column("s", DataTypes.STRING())
                        .option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true")
                        .option(CoreOptions.BUCKET.key(), "-1")
                        .build(),
                false);
        BinaryString abc = BinaryString.fromString("abc");
        write(catalog.getTable(id), GenericRow.of(abc), GenericRow.of(abc), GenericRow.of(abc));
        catalog.alterTable(id, SchemaChange.addColumn("c", DataTypes.INT()), false);

        // SUBSTRING(s, _ROW_ID, 1) = 'b' only matches row id 2
        RowTrackingTable table = new RowTrackingTable((FileStoreTable) catalog.getTable(id));
        RowType type = table.rowType();
        Predicate filter =
                new PredicateBuilder(type)
                        .equal(
                                new SubstringTransform(
                                        Arrays.asList(
                                                new FieldRef(0, "s", DataTypes.STRING()),
                                                new FieldRef(
                                                        type.getFieldIndex("_ROW_ID"),
                                                        "_ROW_ID",
                                                        DataTypes.BIGINT()),
                                                1)),
                                BinaryString.fromString("b"));
        for (Integer limit : Arrays.asList(null, 1)) {
            assertThat(read(table, filter, limit)).containsExactly("abc, NULL, 2, 1");
        }
    }

    private List<String> read(Identifier id, String field, long value, @Nullable Integer limit)
            throws Exception {
        RowTrackingTable table = new RowTrackingTable((FileStoreTable) catalog.getTable(id));
        Predicate filter =
                new PredicateBuilder(table.rowType())
                        .equal(table.rowType().getFieldIndex(field), value);
        return read(table, filter, limit);
    }

    private static List<String> read(
            RowTrackingTable table, Predicate filter, @Nullable Integer limit) throws Exception {
        ReadBuilder readBuilder = table.newReadBuilder().withFilter(filter);
        if (limit != null) {
            readBuilder.withLimit(limit);
        }
        List<String> result = new ArrayList<>();
        try (RecordReader<InternalRow> reader =
                readBuilder.newRead().executeFilter().createReader(readBuilder.newScan().plan())) {
            reader.forEachRemaining(row -> result.add(toStringNoRowKind(row, table.rowType())));
        }
        return result;
    }
}
