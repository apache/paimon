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

package org.apache.paimon.table.sink;

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.schema.FileSystemSchemaManager;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.SchemaValidation;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Storage and schema contracts for generated partition columns. */
class GeneratedPartitionColumnTest {

    @TempDir java.nio.file.Path tempDir;

    @ParameterizedTest
    @ValueSource(strings = {"append", "primary-key", "data-evolution"})
    void testGenerateRouteCommitAndRead(String mode) throws Exception {
        FileStoreTable table = createTable(mode);
        RowType input = RowType.of(table.rowType().getField("ts"), table.rowType().getField("id"));
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite();
                BatchTableCommit commit = builder.newCommit()) {
            write.withInputType(input);
            GenericRow first = GenericRow.of(Timestamp.fromEpochMillis(-1), 1);
            assertThat(write.getPartition(first).getInt(0)).isEqualTo(-1);
            write.write(first);
            write.write(GenericRow.of(Timestamp.fromEpochMillis(0), 2));
            commit.commit(write.prepareCommit());
        }
        List<Split> splits = table.newScan().plan().splits();
        List<Integer> partitions = new ArrayList<>();
        for (Split split : splits) {
            partitions.add(((DataSplit) split).partition().getInt(0));
        }
        assertThat(partitions).containsExactlyInAnyOrder(-1, 0);
        List<String> rows = new ArrayList<>();
        try (RecordReaderIterator<InternalRow> reader =
                new RecordReaderIterator<>(table.newRead().createReader(table.newScan().plan()))) {
            while (reader.hasNext()) {
                InternalRow row = reader.next();
                rows.add(row.getInt(0) + ":" + row.getInt(1));
            }
        }
        assertThat(rows).containsExactlyInAnyOrder("1:-1", "2:0");
        // A reader and writer reopened from persistent schema retain the contract.
        FileStoreTable reopened =
                FileStoreTableFactory.create(LocalFileIO.create(), table.location());
        try (BatchTableWrite write = reopened.newBatchWriteBuilder().newWrite()) {
            assertThatThrownBy(() -> write.write(GenericRow.of(3, 1, Timestamp.fromEpochMillis(0))))
                    .hasMessageContaining("does not match generation expression");
            assertThatThrownBy(
                            () -> write.write(GenericRow.of(3, null, Timestamp.fromEpochMillis(0))))
                    .hasMessageContaining("does not match generation expression");
            assertThatThrownBy(() -> write.withWriteType(input))
                    .hasMessageContaining("Partial-column writes");
        }
    }

    @Test
    void testSchemaEvolutionProtectsDependencies() throws Exception {
        FileStoreTable table = createTable(false);
        FileSystemSchemaManager manager =
                new FileSystemSchemaManager(LocalFileIO.create(), table.location());
        TableSchema changed =
                manager.commitChanges(
                        SchemaChange.updateColumnComment(new String[] {"dt"}, "generated date"));
        assertThat(changed.logicalRowType().getField("dt").generationExpression())
                .isEqualTo("CAST(ts AS DATE)");
        assertThatThrownBy(() -> manager.commitChanges(SchemaChange.renameColumn("ts", "time")))
                .hasMessageContaining("referenced by generated partition column");
        assertThatThrownBy(() -> manager.commitChanges(SchemaChange.dropColumn("ts")))
                .hasMessageContaining("referenced by generated partition column");
        assertThatThrownBy(
                        () ->
                                manager.commitChanges(
                                        SchemaChange.updateColumnType(
                                                "ts", DataTypes.TIMESTAMP(6))))
                .hasMessageContaining("referenced by generated partition column");
        assertThatThrownBy(
                        () ->
                                manager.commitChanges(
                                        SchemaChange.updateColumnDefaultValue(
                                                new String[] {"dt"}, "'2026-10-30'")))
                .hasMessageContaining("cannot have a default value");
    }

    @Test
    void testRejectInvalidDefinitions() {
        Schema plain =
                Schema.newBuilder()
                        .column("ts", DataTypes.TIMESTAMP(3))
                        .generatedColumn("dt", DataTypes.DATE(), "CAST(ts AS DATE)")
                        .build();
        assertThatThrownBy(() -> validate(plain))
                .hasMessageContaining("must be a partition column");
        Schema defaults =
                Schema.newBuilder()
                        .column("ts", DataTypes.TIMESTAMP(3))
                        .column("dt", DataTypes.DATE(), null, "'2026-10-30'", "CAST(ts AS DATE)")
                        .partitionKeys("dt")
                        .build();
        assertThatThrownBy(() -> validate(defaults))
                .hasMessageContaining("cannot have a default value");
        for (String engine : Arrays.asList("partial-update", "aggregation", "first-row")) {
            Schema merge = schema(true).option("merge-engine", engine).build();
            assertThatThrownBy(() -> validate(merge))
                    .hasMessageContaining("deduplicate merge engine");
        }
        Schema nested =
                Schema.newBuilder()
                        .column(
                                "nested",
                                DataTypes.ROW(
                                        new DataField(0, "dt", DataTypes.DATE())
                                                .newGenerationExpression("CAST(ts AS DATE)")))
                        .build();
        assertThatThrownBy(() -> validate(nested))
                .hasMessageContaining("top-level partition columns");
    }

    private FileStoreTable createTable(boolean primaryKey) throws Exception {
        return createTable(primaryKey ? "primary-key" : "append");
    }

    private FileStoreTable createTable(String mode) throws Exception {
        Path path = new Path(tempDir.resolve(UUID.randomUUID().toString()).toUri());
        Schema.Builder builder = schema(mode.equals("primary-key"));
        if (mode.equals("data-evolution")) {
            builder.option("row-tracking.enabled", "true").option("data-evolution.enabled", "true");
        }
        new FileSystemSchemaManager(LocalFileIO.create(), path).createTable(builder.build());
        return FileStoreTableFactory.create(LocalFileIO.create(), path);
    }

    private Schema.Builder schema(boolean primaryKey) {
        Schema.Builder builder =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .generatedColumn("dt", DataTypes.DATE().notNull(), "CAST(ts AS DATE)")
                        .column("ts", DataTypes.TIMESTAMP(3))
                        .partitionKeys("dt");
        if (primaryKey) {
            builder.primaryKey("id", "dt").option("bucket", "1");
        }
        return builder;
    }

    private void validate(Schema schema) {
        SchemaValidation.validateTableSchema(
                new TableSchema(
                        0,
                        schema.fields(),
                        10,
                        schema.partitionKeys(),
                        schema.primaryKeys(),
                        schema.options(),
                        null));
    }
}
