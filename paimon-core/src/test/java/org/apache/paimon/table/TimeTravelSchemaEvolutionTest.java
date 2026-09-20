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
import org.apache.paimon.data.Blob;
import org.apache.paimon.data.BlobDescriptor;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.file.Files;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for time travel across changes to directive-managed columns. */
public class TimeTravelSchemaEvolutionTest extends TableTestBase {

    @ParameterizedTest
    @MethodSource("columnDirectives")
    public void testTimeTravelAfterAddingColumn(String directive, String optionKey)
            throws Exception {
        catalog.createTable(identifier(), schemaBuilder(directive, true).build(), false);
        FileStoreTable table = getTableDefault();
        write(table, GenericRow.of(1, null));
        table.createTag("before_add", 1);
        TableSchema historicalSchema = table.schemaManager().schema(table.schema().id());

        // A second snapshot shares the old schema, so checking only schema ID is insufficient.
        write(table, GenericRow.of(2, null));
        catalog.alterTable(
                identifier(),
                SchemaChange.addColumn("payload_v2", sourceType(directive), directive, null),
                false);
        table = getTableDefault();
        write(table, GenericRow.of(3, null, null));
        TableSchema latestSchema = table.schema();
        table = table.copy(Collections.singletonMap(CoreOptions.READ_BATCH_SIZE.key(), "32"));
        FileStoreTable historicalTable =
                table.copy(Collections.singletonMap(CoreOptions.SCAN_VERSION.key(), "before_add"));
        assertHistoricalTable(historicalTable, historicalSchema);
        assertThat(historicalTable.options())
                .containsEntry(optionKey, "payload")
                .containsEntry(CoreOptions.READ_BATCH_SIZE.key(), "32")
                .containsEntry(CoreOptions.SCAN_TAG_NAME.key(), "before_add");

        assertThat(read(table, new int[] {0}))
                .extracting(row -> row.getInt(0))
                .containsExactlyInAnyOrder(1, 2, 3);
        assertThat(getTableDefault().schema()).isEqualTo(latestSchema);
        assertThat(table.schemaManager().schema(historicalSchema.id())).isEqualTo(historicalSchema);
    }

    @Test
    public void testTimeTravelBeforeFirstVectorColumn() throws Exception {
        String directive = "__VECTOR_FIELD;3";
        catalog.createTable(identifier(), schemaBuilder(directive, false).build(), false);
        FileStoreTable table = getTableDefault();
        write(table, GenericRow.of(1));
        TableSchema historicalSchema = table.schema();
        catalog.alterTable(
                identifier(),
                SchemaChange.addColumn("payload", sourceType(directive), directive, null),
                false);
        table = getTableDefault();
        write(table, GenericRow.of(2, null));

        FileStoreTable historicalTable =
                table.copy(Collections.singletonMap(CoreOptions.SCAN_SNAPSHOT_ID.key(), "1"));
        assertHistoricalTable(historicalTable, historicalSchema);
        assertThat(historicalTable.options()).doesNotContainKey(CoreOptions.VECTOR_FIELD.key());
    }

    @Test
    public void testTimeTravelAfterDroppingDescriptorColumn() throws Exception {
        String directive = "__BLOB_DESCRIPTOR_FIELD";
        catalog.createTable(identifier(), schemaBuilder(directive, true).build(), false);
        FileStoreTable table = getTableDefault();
        byte[] bytes = new byte[] {1, 2, 3};
        java.nio.file.Path externalFile = tempPath.resolve("payload.bin");
        Files.write(externalFile, bytes);
        BlobDescriptor descriptor = new BlobDescriptor(externalFile.toString(), 0, bytes.length);
        write(
                table,
                GenericRow.of(1, Blob.fromFile(table.fileIO(), descriptor.uri(), 0, bytes.length)));
        TableSchema historicalSchema = table.schema();
        catalog.alterTable(identifier(), SchemaChange.dropColumn("payload"), false);
        table = getTableDefault();
        write(table, GenericRow.of(2));
        assertThat(table.options()).doesNotContainKey(CoreOptions.BLOB_DESCRIPTOR_FIELD.key());

        FileStoreTable historicalTable =
                table.copy(Collections.singletonMap(CoreOptions.SCAN_SNAPSHOT_ID.key(), "1"));
        assertHistoricalTable(historicalTable, historicalSchema);
        assertThat(historicalTable.options())
                .containsEntry(CoreOptions.BLOB_DESCRIPTOR_FIELD.key(), "payload");
        // The old column must still be read as an inline descriptor, not a managed .blob column.
        List<InternalRow> rows = read(historicalTable);
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).getBlob(1).toDescriptor()).isEqualTo(descriptor);
        assertThat(rows.get(0).getBlob(1).toData()).isEqualTo(bytes);
    }

    @Test
    public void testTimeTravelWithLegacyDescriptorOption() throws Exception {
        String legacyKey = "blob.stored-descriptor-fields";
        Schema schema =
                schemaBuilder("__BLOB_DESCRIPTOR_FIELD", false)
                        .column("payload", DataTypes.BLOB())
                        .option(legacyKey, "payload")
                        .build();
        catalog.createTable(identifier(), schema, false);
        FileStoreTable table = getTableDefault();
        write(table, GenericRow.of(1, null));
        TableSchema historicalSchema = table.schema();
        catalog.alterTable(
                identifier(),
                SchemaChange.addColumn(
                        "payload_v2", DataTypes.BYTES(), "__BLOB_DESCRIPTOR_FIELD", null),
                false);
        table = getTableDefault();
        assertThat(table.options())
                .containsEntry(CoreOptions.BLOB_DESCRIPTOR_FIELD.key(), "payload,payload_v2")
                .doesNotContainKey(legacyKey);

        FileStoreTable historicalTable =
                table.copy(Collections.singletonMap(CoreOptions.SCAN_SNAPSHOT_ID.key(), "1"));
        assertHistoricalTable(historicalTable, historicalSchema);
        assertThat(historicalTable.options())
                .containsEntry(legacyKey, "payload")
                .doesNotContainKey(CoreOptions.BLOB_DESCRIPTOR_FIELD.key());
        assertThat(historicalTable.coreOptions().blobDescriptorField()).containsExactly("payload");
    }

    @Test
    public void testTimeTravelBeforeLegacyDescriptorOption() throws Exception {
        catalog.createTable(
                identifier(), schemaBuilder("__BLOB_DESCRIPTOR_FIELD", false).build(), false);
        FileStoreTable table = getTableDefault();
        write(table, GenericRow.of(1));
        TableSchema historicalSchema = table.schema();
        catalog.alterTable(
                identifier(), SchemaChange.addColumn("payload", DataTypes.BLOB()), false);
        catalog.alterTable(
                identifier(),
                SchemaChange.setOption("blob.stored-descriptor-fields", "payload"),
                false);
        table = getTableDefault();
        FileStoreTable historicalTable =
                table.copy(Collections.singletonMap(CoreOptions.SCAN_SNAPSHOT_ID.key(), "1"));
        assertHistoricalTable(historicalTable, historicalSchema);
        assertThat(historicalTable.options()).doesNotContainKey("blob.stored-descriptor-fields");
        assertThat(historicalTable.coreOptions().blobDescriptorField()).isEmpty();
    }

    @Test
    public void testExplicitVectorOptionsArePreserved() throws Exception {
        String directive = "__VECTOR_FIELD;3";
        catalog.createTable(identifier(), schemaBuilder(directive, true).build(), false);
        FileStoreTable table = getTableDefault();
        write(table, GenericRow.of(1, null));

        Map<String, String> queryOptions = new HashMap<>();
        queryOptions.put(CoreOptions.SCAN_SNAPSHOT_ID.key(), "1");
        queryOptions.put(CoreOptions.VECTOR_FIELD.key(), "missing");
        assertThatThrownBy(() -> table.copy(queryOptions))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Some of the columns specified as vector-field are unknown.");

        queryOptions.put(CoreOptions.VECTOR_FIELD.key(), null);
        FileStoreTable historicalTable = table.copy(queryOptions);
        assertThat(historicalTable.options()).doesNotContainKey(CoreOptions.VECTOR_FIELD.key());

        // A later copy must not restore a field option explicitly removed by an earlier copy.
        Map<String, String> readOptions =
                Collections.singletonMap(CoreOptions.READ_BATCH_SIZE.key(), "32");
        assertThat(historicalTable.copy(readOptions).options())
                .doesNotContainKey(CoreOptions.VECTOR_FIELD.key());
        assertThat(
                        table.copyWithoutTimeTravel(
                                        Collections.singletonMap(
                                                CoreOptions.VECTOR_FIELD.key(), null))
                                .copy(
                                        Collections.singletonMap(
                                                CoreOptions.SCAN_SNAPSHOT_ID.key(), "1"))
                                .options())
                .doesNotContainKey(CoreOptions.VECTOR_FIELD.key());
    }

    private void assertHistoricalTable(FileStoreTable historicalTable, TableSchema historicalSchema)
            throws Exception {
        assertThat(historicalTable.schema().id()).isEqualTo(historicalSchema.id());
        assertThat(historicalTable.schema().fields()).isEqualTo(historicalSchema.fields());
        assertThat(read(historicalTable, new int[] {0}))
                .extracting(row -> row.getInt(0))
                .containsExactly(1);
    }

    private static Schema.Builder schemaBuilder(String directive, boolean withPayload) {
        Schema.Builder builder =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .option(CoreOptions.FILE_FORMAT.key(), "parquet")
                        .option(CoreOptions.FILE_COMPRESSION.key(), "none")
                        .option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true")
                        .option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true");
        if (withPayload) {
            builder.column("payload", sourceType(directive), directive);
        }
        if (directive.startsWith("__VECTOR")) {
            builder.option(CoreOptions.VECTOR_FILE_FORMAT.key(), "json");
        }
        return builder;
    }

    private static Stream<Arguments> columnDirectives() {
        return Stream.of(
                Arguments.of("__VECTOR_FIELD;3", CoreOptions.VECTOR_FIELD.key()),
                Arguments.of("__BLOB_FIELD", CoreOptions.BLOB_FIELD.key()),
                Arguments.of("__BLOB_DESCRIPTOR_FIELD", CoreOptions.BLOB_DESCRIPTOR_FIELD.key()),
                Arguments.of("__BLOB_VIEW_FIELD", CoreOptions.BLOB_VIEW_FIELD.key()));
    }

    private static DataType sourceType(String directive) {
        return directive.startsWith("__VECTOR")
                ? DataTypes.ARRAY(DataTypes.FLOAT())
                : DataTypes.BYTES();
    }
}
