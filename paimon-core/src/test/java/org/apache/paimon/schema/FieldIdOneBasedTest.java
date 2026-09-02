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

package org.apache.paimon.schema;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link CoreOptions#FIELD_ID_ONE_BASED} at table creation and evolution. */
public class FieldIdOneBasedTest {

    @TempDir java.nio.file.Path tempDir;

    private Schema.Builder schemaBuilder() {
        return Schema.newBuilder()
                .column("a", DataTypes.INT())
                .column(
                        "s",
                        DataTypes.ROW(
                                DataTypes.FIELD(0, "x", DataTypes.INT()),
                                DataTypes.FIELD(0, "y", DataTypes.STRING())))
                .column("m", DataTypes.MAP(DataTypes.STRING(), DataTypes.ARRAY(DataTypes.INT())));
    }

    private SchemaManager newSchemaManager(String name) {
        return new SchemaManager(
                LocalFileIO.create(),
                new Path(tempDir.toString() + "/" + name + UUID.randomUUID()));
    }

    @Test
    public void testDefaultRemainsZeroBased() throws Exception {
        TableSchema schema = newSchemaManager("t").createTable(schemaBuilder().build());
        assertThat(topLevelIds(schema)).containsExactly(0, 1, 4);
        RowType nested = (RowType) schema.fields().get(1).type();
        assertThat(nested.getFields().get(0).id()).isEqualTo(2);
        assertThat(nested.getFields().get(1).id()).isEqualTo(3);
        assertThat(schema.highestFieldId()).isEqualTo(4);
    }

    @Test
    public void testOneBasedShiftsAllIds() throws Exception {
        TableSchema schema =
                newSchemaManager("t")
                        .createTable(
                                schemaBuilder()
                                        .option(CoreOptions.FIELD_ID_ONE_BASED.key(), "true")
                                        .build());
        assertThat(topLevelIds(schema)).containsExactly(1, 2, 5);
        RowType nested = (RowType) schema.fields().get(1).type();
        assertThat(nested.getFields().get(0).id()).isEqualTo(3);
        assertThat(nested.getFields().get(1).id()).isEqualTo(4);
        // map/array types carry no ids of their own; ensure the structure survived the shift
        MapType map = (MapType) schema.fields().get(2).type();
        assertThat(map.getValueType()).isInstanceOf(ArrayType.class);
        assertThat(schema.highestFieldId()).isEqualTo(5);
    }

    @Test
    public void testEvolutionContinuesFromShiftedIds() throws Exception {
        SchemaManager manager = newSchemaManager("t");
        manager.createTable(
                schemaBuilder().option(CoreOptions.FIELD_ID_ONE_BASED.key(), "true").build());
        TableSchema evolved = manager.commitChanges(SchemaChange.addColumn("z", DataTypes.INT()));
        DataField added =
                evolved.fields().stream()
                        .filter(f -> f.name().equals("z"))
                        .findFirst()
                        .orElseThrow(IllegalStateException::new);
        assertThat(added.id()).isEqualTo(6);
        assertThat(evolved.highestFieldId()).isEqualTo(6);
    }

    @Test
    public void testCreateFromPersistedOneBasedSchemaPreservesIds() throws Exception {
        // the copy_files procedure rebuilds a Schema from a persisted TableSchema, options
        // included; already-shifted ids must survive creation unchanged, since the copied
        // data files embed them
        SchemaManager source = newSchemaManager("src");
        TableSchema sourceSchema =
                source.createTable(
                        schemaBuilder()
                                .option(CoreOptions.FIELD_ID_ONE_BASED.key(), "true")
                                .build());
        assertThat(topLevelIds(sourceSchema)).containsExactly(1, 2, 5);

        Schema copied =
                new Schema(
                        sourceSchema.fields(),
                        sourceSchema.partitionKeys(),
                        sourceSchema.primaryKeys(),
                        sourceSchema.options(),
                        sourceSchema.comment());
        TableSchema copiedSchema = newSchemaManager("dst").createTable(copied);
        assertThat(topLevelIds(copiedSchema)).isEqualTo(topLevelIds(sourceSchema));
        RowType sourceNested = (RowType) sourceSchema.fields().get(1).type();
        RowType copiedNested = (RowType) copiedSchema.fields().get(1).type();
        assertThat(copiedNested.getFields().stream().map(DataField::id))
                .containsExactlyElementsOf(
                        sourceNested.getFields().stream()
                                .map(DataField::id)
                                .collect(java.util.stream.Collectors.toList()));
        assertThat(copiedSchema.highestFieldId()).isEqualTo(sourceSchema.highestFieldId());
    }

    @Test
    public void testInvalidOptionValueRejected() throws Exception {
        SchemaManager manager = newSchemaManager("t");
        manager.createTable(schemaBuilder().build());
        // a lax parse would persist this as an accidental 'false' that the immutability
        // check then freezes forever
        assertThatThrownBy(
                        () ->
                                manager.commitChanges(
                                        SchemaChange.setOption(
                                                CoreOptions.FIELD_ID_ONE_BASED.key(), "yes")))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testOneBasedImmutableAndCreateTimeOnly() throws Exception {
        // registered as immutable, so ALTER is rejected once the table has snapshots
        assertThat(CoreOptions.IMMUTABLE_OPTIONS).contains(CoreOptions.FIELD_ID_ONE_BASED.key());
        // ids are assigned once at creation, so changing the value is rejected even before the
        // first snapshot: the ids would keep their base while the option claims another one
        SchemaManager manager = newSchemaManager("t");
        manager.createTable(schemaBuilder().build());
        assertThatThrownBy(
                        () ->
                                manager.commitChanges(
                                        SchemaChange.setOption(
                                                CoreOptions.FIELD_ID_ONE_BASED.key(), "true")))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining(CoreOptions.FIELD_ID_ONE_BASED.key());
        // removing the option from a one-based table would change the effective value back
        SchemaManager oneBased = newSchemaManager("t2");
        oneBased.createTable(
                schemaBuilder().option(CoreOptions.FIELD_ID_ONE_BASED.key(), "true").build());
        assertThatThrownBy(
                        () ->
                                oneBased.commitChanges(
                                        SchemaChange.removeOption(
                                                CoreOptions.FIELD_ID_ONE_BASED.key())))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining(CoreOptions.FIELD_ID_ONE_BASED.key());
        // re-stating the current value is a no-op, not a change, and stays allowed
        TableSchema unchanged =
                manager.commitChanges(
                        SchemaChange.setOption(CoreOptions.FIELD_ID_ONE_BASED.key(), "false"));
        assertThat(topLevelIds(unchanged)).containsExactly(0, 1, 4);
    }

    private static List<Integer> topLevelIds(TableSchema schema) {
        return schema.fields().stream().map(DataField::id).collect(Collectors.toList());
    }
}
