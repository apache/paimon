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
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.source.InnerTableRead;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for storing only required fields in primary-key DELETE rows. */
public class DeleteRowFieldsTest extends TableTestBase {

    @ParameterizedTest
    @ValueSource(strings = {"deduplicate", "partial-update", "aggregation"})
    public void testDeleteReadFilterCompactionAndReinsert(String mergeEngine) throws Exception {
        FileStoreTable table = createTable(mergeEngine);
        write(table, row(RowKind.INSERT, 1L, "old-payload"));
        write(table, row(RowKind.DELETE, 2L, "old-payload"));

        assertThat(read(table)).isEmpty();

        List<InternalRow> deletes = readKeepingDeletes(table);
        assertThat(deletes).hasSize(1);
        InternalRow delete = deletes.get(0);
        assertThat(delete.getRowKind()).isEqualTo(RowKind.DELETE);
        assertThat(delete.getInt(0)).isEqualTo(1);
        assertThat(delete.getInt(1)).isEqualTo(1);
        assertThat(delete.getLong(2)).isEqualTo(2L);
        assertThat(delete.getString(3).toString()).isEqualTo("required-value");
        assertThat(delete.isNullAt(4)).isTrue();

        Predicate payloadFilter =
                new PredicateBuilder(table.rowType())
                        .equal(4, BinaryString.fromString("old-payload"));
        assertThat(readWithFilter(table, payloadFilter)).isEmpty();

        FileStoreTable compactionTable =
                (FileStoreTable)
                        table.copy(Collections.singletonMap(CoreOptions.WRITE_ONLY.key(), "false"));
        compact(compactionTable, partition(1), 0);
        assertThat(read(compactionTable)).isEmpty();

        write(compactionTable, row(RowKind.INSERT, 3L, "new-payload"));
        List<InternalRow> rows = read(compactionTable);
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).getLong(2)).isEqualTo(3L);
        assertThat(rows.get(0).getString(4).toString()).isEqualTo("new-payload");
    }

    private FileStoreTable createTable(String mergeEngine) throws Exception {
        Schema.Builder builder =
                Schema.newBuilder()
                        .column("pt", DataTypes.INT())
                        .column("k", DataTypes.INT())
                        .column("seq", DataTypes.BIGINT())
                        .column("required_value", DataTypes.STRING().notNull())
                        .column("payload", DataTypes.STRING())
                        .partitionKeys("pt")
                        .primaryKey("pt", "k")
                        .option(CoreOptions.BUCKET.key(), "1")
                        .option(CoreOptions.WRITE_ONLY.key(), "true")
                        .option(CoreOptions.SEQUENCE_FIELD.key(), "seq")
                        .option(CoreOptions.DATA_FILE_THIN_MODE.key(), "true")
                        .option(CoreOptions.MERGE_ENGINE.key(), mergeEngine);
        if (mergeEngine.equals("partial-update")) {
            builder.option(CoreOptions.PARTIAL_UPDATE_REMOVE_RECORD_ON_DELETE.key(), "true");
        } else if (mergeEngine.equals("aggregation")) {
            builder.option(CoreOptions.AGGREGATION_REMOVE_RECORD_ON_DELETE.key(), "true");
        }
        catalog.createTable(identifier(), builder.build(), false);
        return (FileStoreTable) catalog.getTable(identifier());
    }

    private GenericRow row(RowKind kind, long sequence, String payload) {
        return GenericRow.ofKind(
                kind,
                1,
                1,
                sequence,
                BinaryString.fromString("required-value"),
                BinaryString.fromString(payload));
    }

    private BinaryRow partition(int value) {
        InternalRowSerializer serializer =
                new InternalRowSerializer(RowType.of(DataTypes.INT().notNull()));
        return serializer.toBinaryRow(GenericRow.of(value)).copy();
    }

    private List<InternalRow> readKeepingDeletes(FileStoreTable table) throws Exception {
        InnerTableRead read = table.newRead().forceKeepDelete();
        return read(table, read, null);
    }

    private List<InternalRow> readWithFilter(FileStoreTable table, Predicate predicate)
            throws Exception {
        InnerTableRead read = table.newRead().withFilter(predicate);
        return read(table, read, predicate);
    }

    private List<InternalRow> read(FileStoreTable table, InnerTableRead read, Predicate predicate)
            throws Exception {
        List<InternalRow> rows = new ArrayList<>();
        InternalRowSerializer serializer = new InternalRowSerializer(table.rowType());
        try (RecordReader<InternalRow> reader =
                read.createReader(
                        predicate == null
                                ? table.newScan().plan()
                                : table.newScan().withFilter(predicate).plan())) {
            reader.forEachRemaining(row -> rows.add(serializer.copy(row)));
        }
        return rows;
    }
}
