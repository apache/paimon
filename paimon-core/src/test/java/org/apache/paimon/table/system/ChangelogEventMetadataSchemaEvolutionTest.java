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
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.StreamTableScan;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests reading changelog event metadata from files written under historical schemas. */
class ChangelogEventMetadataSchemaEvolutionTest extends TableTestBase {

    @ParameterizedTest(name = "file.format = {0}")
    @ValueSource(strings = {"parquet", "avro"})
    void testDropColumnDoesNotAliasMetadata(String format) throws Exception {
        Schema schema =
                schemaBuilder(format)
                        .column("id", DataTypes.INT().notNull())
                        .column("data", DataTypes.INT())
                        .column("event_ts", DataTypes.BIGINT())
                        .column("extra", DataTypes.BIGINT())
                        .build();
        catalog.createTable(identifier(), schema, false);

        writeAndCompact(GenericRow.of(1, 10, 50L, 777L), GenericRow.of(1, 20, 100L, 888L));

        // The dropped field keeps its ID in historical schemas. Metadata must not reuse that ID,
        // otherwise historical files would resolve the metadata to the dropped column's values.
        catalog.alterTable(identifier(), SchemaChange.dropColumn("extra"), false);

        ChangelogEventMetadataTable table = new ChangelogEventMetadataTable(getTableDefault());
        assertThat(table.rowType().getFieldNames())
                .containsExactly("id", "data", "event_ts", "__internal__event_ts");
        assertThat(streamingRead(table))
                .containsExactly("+I[1, 10, 50, 50]", "-U[1, 10, 50, 100]", "+U[1, 20, 100, 100]");
        assertThat(batchRead(table)).containsExactly("+I[1, 20, 100, 100]");
    }

    @ParameterizedTest(name = "file.format = {0}")
    @ValueSource(strings = {"parquet", "avro"})
    void testSourceTypeChangeCastsHistoricalMetadata(String format) throws Exception {
        Schema schema =
                schemaBuilder(format)
                        .column("id", DataTypes.INT().notNull())
                        .column("data", DataTypes.INT())
                        .column("event_ts", DataTypes.BIGINT())
                        .build();
        catalog.createTable(identifier(), schema, false);

        writeAndCompact(GenericRow.of(1, 10, 50L), GenericRow.of(1, 20, 100L));

        // Historical metadata values were written as BIGINT and must be cast like the source
        // column itself.
        catalog.alterTable(
                identifier(),
                SchemaChange.updateColumnType("event_ts", DataTypes.DECIMAL(20, 0)),
                false);

        ChangelogEventMetadataTable table = new ChangelogEventMetadataTable(getTableDefault());
        assertThat(table.rowType().getField("__internal__event_ts").type())
                .isEqualTo(DataTypes.DECIMAL(20, 0));
        assertThat(streamingRead(table))
                .containsExactly("+I[1, 10, 50, 50]", "-U[1, 10, 50, 100]", "+U[1, 20, 100, 100]");
        assertThat(batchRead(table)).containsExactly("+I[1, 20, 100, 100]");
    }

    private Schema.Builder schemaBuilder(String format) {
        return Schema.newBuilder()
                .primaryKey("id")
                .option(CoreOptions.BUCKET.key(), "1")
                .option(CoreOptions.FILE_FORMAT.key(), format)
                .option(CoreOptions.CHANGELOG_PRODUCER.key(), "lookup")
                .option(CoreOptions.CHANGELOG_PRODUCER_EVENT_METADATA_FIELDS.key(), "event_ts");
    }

    /** Writes each row in its own commit and waits for the lookup changelog compaction. */
    private void writeAndCompact(InternalRow... rows) throws Exception {
        FileStoreTable table = getTableDefault();
        try (StreamTableWrite write = table.newWrite(commitUser).withIOManager(ioManager);
                StreamTableCommit commit = table.newCommit(commitUser)) {
            for (int i = 0; i < rows.length; i++) {
                write.write(rows[i]);
                commit.commit(i, write.prepareCommit(true, i));
            }
        }
    }

    private List<String> streamingRead(ChangelogEventMetadataTable table) throws Exception {
        // Restore the scan position instead of setting scan.snapshot-id, which would time travel
        // to the historical schema and bypass reading old files with the current schema.
        ReadBuilder readBuilder = table.newReadBuilder();
        StreamTableScan scan = readBuilder.newStreamScan();
        scan.restore(1L);
        List<String> result = new ArrayList<>();
        long latestSnapshotId = table.snapshotManager().latestSnapshotId();
        for (int i = 0;
                i < 2 * latestSnapshotId
                        && (scan.checkpoint() == null || scan.checkpoint() <= latestSnapshotId);
                i++) {
            result.addAll(read(readBuilder, scan.plan(), table.rowType()));
        }
        assertThat(scan.checkpoint()).isGreaterThan(latestSnapshotId);
        return result;
    }

    private List<String> batchRead(ChangelogEventMetadataTable table) throws Exception {
        ReadBuilder readBuilder = table.newReadBuilder();
        return read(readBuilder, readBuilder.newScan().plan(), table.rowType());
    }

    private static List<String> read(ReadBuilder readBuilder, TableScan.Plan plan, RowType rowType)
            throws Exception {
        List<InternalRow.FieldGetter> getters = new ArrayList<>();
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            getters.add(InternalRow.createFieldGetter(rowType.getTypeAt(i), i));
        }
        List<String> result = new ArrayList<>();
        readBuilder
                .newRead()
                .createReader(plan)
                .forEachRemaining(
                        row -> {
                            List<String> values = new ArrayList<>();
                            for (InternalRow.FieldGetter getter : getters) {
                                values.add(String.valueOf(getter.getFieldOrNull(row)));
                            }
                            result.add(
                                    row.getRowKind().shortString()
                                            + "["
                                            + String.join(", ", values)
                                            + "]");
                        });
        return result;
    }
}
