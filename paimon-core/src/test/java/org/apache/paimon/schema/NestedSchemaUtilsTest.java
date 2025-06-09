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

import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.MultisetType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VarCharType;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link NestedSchemaUtils}. */
public class NestedSchemaUtilsTest {

    @Test
    public void testPrimitiveTypeUpdate() {
        List<String> fieldNames = Arrays.asList("column1");
        List<SchemaChange> schemaChanges = new ArrayList<>();

        // Test type conversion (INT -> BIGINT)
        NestedSchemaUtils.generateNestedColumnUpdates(
                fieldNames, DataTypes.INT(), DataTypes.BIGINT(), schemaChanges);

        assertThat(schemaChanges).hasSize(1);
        assertThat(schemaChanges.get(0)).isInstanceOf(SchemaChange.UpdateColumnType.class);
        SchemaChange.UpdateColumnType typeChange =
                (SchemaChange.UpdateColumnType) schemaChanges.get(0);
        assertThat(typeChange.fieldNames()).containsExactly("column1");
        assertThat(typeChange.newDataType()).isEqualTo(DataTypes.BIGINT());
    }

    @Test
    public void testPrimitiveTypeUpdateWithSameType() {
        List<String> fieldNames = Arrays.asList("column1");
        List<SchemaChange> schemaChanges = new ArrayList<>();

        // Same type should not generate any changes
        NestedSchemaUtils.generateNestedColumnUpdates(
                fieldNames, DataTypes.INT(), DataTypes.INT(), schemaChanges);

        assertThat(schemaChanges).isEmpty();
    }

    @Test
    public void testNullabilityChangeOnly() {
        List<String> fieldNames = Arrays.asList("column1");
        List<SchemaChange> schemaChanges = new ArrayList<>();

        // Test nullable to non-nullable
        NestedSchemaUtils.generateNestedColumnUpdates(
                fieldNames, DataTypes.INT().nullable(), DataTypes.INT().notNull(), schemaChanges);

        assertThat(schemaChanges).hasSize(1);
        assertThat(schemaChanges.get(0)).isInstanceOf(SchemaChange.UpdateColumnNullability.class);
        SchemaChange.UpdateColumnNullability nullabilityChange =
                (SchemaChange.UpdateColumnNullability) schemaChanges.get(0);
        assertThat(nullabilityChange.fieldNames()).containsExactly("column1");
        assertThat(nullabilityChange.newNullability()).isFalse();
    }

    @Test
    public void testTypeAndNullabilityChange() {
        List<String> fieldNames = Arrays.asList("column1");
        List<SchemaChange> schemaChanges = new ArrayList<>();

        // Test type change and nullability change together
        NestedSchemaUtils.generateNestedColumnUpdates(
                fieldNames,
                DataTypes.INT().nullable(),
                DataTypes.BIGINT().notNull(),
                schemaChanges);

        assertThat(schemaChanges).hasSize(2);

        // Should have both type and nullability changes
        assertThat(schemaChanges)
                .anyMatch(change -> change instanceof SchemaChange.UpdateColumnType);
        assertThat(schemaChanges)
                .anyMatch(change -> change instanceof SchemaChange.UpdateColumnNullability);
    }

    @Test
    public void testArrayTypeUpdateElement() {
        List<String> fieldNames = Arrays.asList("arr_column");
        List<SchemaChange> schemaChanges = new ArrayList<>();

        ArrayType oldType = new ArrayType(true, DataTypes.INT());
        ArrayType newType = new ArrayType(true, DataTypes.BIGINT());

        NestedSchemaUtils.generateNestedColumnUpdates(fieldNames, oldType, newType, schemaChanges);

        assertThat(schemaChanges).hasSize(1);
        assertThat(schemaChanges.get(0)).isInstanceOf(SchemaChange.UpdateColumnType.class);
        SchemaChange.UpdateColumnType typeChange =
                (SchemaChange.UpdateColumnType) schemaChanges.get(0);
        assertThat(typeChange.fieldNames()).containsExactly("arr_column", "element");
        assertThat(typeChange.newDataType()).isEqualTo(DataTypes.BIGINT());
    }

    @Test
    public void testArrayTypeUpdateNullability() {
        List<String> fieldNames = Arrays.asList("arr_column");
        List<SchemaChange> schemaChanges = new ArrayList<>();

        ArrayType oldType = new ArrayType(true, DataTypes.INT());
        ArrayType newType = new ArrayType(false, DataTypes.INT());

        NestedSchemaUtils.generateNestedColumnUpdates(fieldNames, oldType, newType, schemaChanges);

        assertThat(schemaChanges).hasSize(1);
        assertThat(schemaChanges.get(0)).isInstanceOf(SchemaChange.UpdateColumnNullability.class);
        SchemaChange.UpdateColumnNullability nullabilityChange =
                (SchemaChange.UpdateColumnNullability) schemaChanges.get(0);
        assertThat(nullabilityChange.fieldNames()).containsExactly("arr_column");
        assertThat(nullabilityChange.newNullability()).isFalse();
    }

    @Test
    public void testArrayWithIncompatibleTypeThrowsException() {
        List<String> fieldNames = Arrays.asList("arr_column");
        List<SchemaChange> schemaChanges = new ArrayList<>();

        ArrayType oldType = new ArrayType(true, DataTypes.INT());
        RowType newElementType = RowType.of(DataTypes.STRING()); // Incompatible type

        assertThatThrownBy(
                        () -> {
                            NestedSchemaUtils.generateNestedColumnUpdates(
                                    fieldNames, oldType, newElementType, schemaChanges);
                        })
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("can only be updated to array type");
    }

    @Test
    public void testMapTypeUpdateValue() {
        List<String> fieldNames = Arrays.asList("map_column");
        List<SchemaChange> schemaChanges = new ArrayList<>();

        MapType oldType = new MapType(true, DataTypes.STRING(), DataTypes.INT());
        MapType newType = new MapType(true, DataTypes.STRING(), DataTypes.BIGINT());

        NestedSchemaUtils.generateNestedColumnUpdates(fieldNames, oldType, newType, schemaChanges);

        assertThat(schemaChanges).hasSize(1);
        assertThat(schemaChanges.get(0)).isInstanceOf(SchemaChange.UpdateColumnType.class);
        SchemaChange.UpdateColumnType typeChange =
                (SchemaChange.UpdateColumnType) schemaChanges.get(0);
        assertThat(typeChange.fieldNames()).containsExactly("map_column", "value");
        assertThat(typeChange.newDataType()).isEqualTo(DataTypes.BIGINT());
    }

    @Test
    public void testMapTypeUpdateNullability() {
        List<String> fieldNames = Arrays.asList("map_column");
        List<SchemaChange> schemaChanges = new ArrayList<>();

        MapType oldType = new MapType(true, DataTypes.STRING(), DataTypes.INT());
        MapType newType = new MapType(false, DataTypes.STRING(), DataTypes.INT());

        NestedSchemaUtils.generateNestedColumnUpdates(fieldNames, oldType, newType, schemaChanges);

        assertThat(schemaChanges).hasSize(1);
        assertThat(schemaChanges.get(0)).isInstanceOf(SchemaChange.UpdateColumnNullability.class);
        SchemaChange.UpdateColumnNullability nullabilityChange =
                (SchemaChange.UpdateColumnNullability) schemaChanges.get(0);
        assertThat(nullabilityChange.fieldNames()).containsExactly("map_column");
        assertThat(nullabilityChange.newNullability()).isFalse();
    }

    @Test
    public void testMapWithIncompatibleKeyTypeThrowsException() {
        List<String> fieldNames = Arrays.asList("map_column");
        List<SchemaChange> schemaChanges = new ArrayList<>();

        MapType oldType = new MapType(true, DataTypes.STRING(), DataTypes.INT());
        MapType newType = new MapType(true, DataTypes.INT(), DataTypes.INT()); // Different key type

        assertThatThrownBy(
                        () -> {
                            NestedSchemaUtils.generateNestedColumnUpdates(
                                    fieldNames, oldType, newType, schemaChanges);
                        })
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Cannot update key type");
    }

    @Test
    public void testMapWithIncompatibleTypeThrowsException() {
        List<String> fieldNames = Arrays.asList("map_column");
        List<SchemaChange> schemaChanges = new ArrayList<>();

        MapType oldType = new MapType(true, DataTypes.STRING(), DataTypes.INT());
        ArrayType newType = new ArrayType(true, DataTypes.INT()); // Incompatible type

        assertThatThrownBy(
                        () -> {
                            NestedSchemaUtils.generateNestedColumnUpdates(
                                    fieldNames, oldType, newType, schemaChanges);
                        })
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("can only be updated to map type");
    }

    @Test
    public void testMultisetTypeUpdateElement() {
        List<String> fieldNames = Arrays.asList("multiset_column");
        List<SchemaChange> schemaChanges = new ArrayList<>();

        MultisetType oldType = new MultisetType(true, DataTypes.INT());
        MultisetType newType = new MultisetType(true, DataTypes.BIGINT());

        NestedSchemaUtils.generateNestedColumnUpdates(fieldNames, oldType, newType, schemaChanges);

        assertThat(schemaChanges).hasSize(1);
        assertThat(schemaChanges.get(0)).isInstanceOf(SchemaChange.UpdateColumnType.class);
        SchemaChange.UpdateColumnType typeChange =
                (SchemaChange.UpdateColumnType) schemaChanges.get(0);
        assertThat(typeChange.fieldNames()).containsExactly("multiset_column", "element");
        assertThat(typeChange.newDataType()).isEqualTo(DataTypes.BIGINT());
    }

    @Test
    public void testMultisetWithIncompatibleTypeThrowsException() {
        List<String> fieldNames = Arrays.asList("multiset_column");
        List<SchemaChange> schemaChanges = new ArrayList<>();

        MultisetType oldType = new MultisetType(true, DataTypes.INT());
        MapType newType =
                new MapType(true, DataTypes.STRING(), DataTypes.INT()); // Incompatible type

        assertThatThrownBy(
                        () -> {
                            NestedSchemaUtils.generateNestedColumnUpdates(
                                    fieldNames, oldType, newType, schemaChanges);
                        })
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("can only be updated to multiset type");
    }

    @Test
    public void testRowTypeAddField() {
        List<String> fieldNames = Arrays.asList("row_column");
        List<SchemaChange> schemaChanges = new ArrayList<>();

        RowType oldType =
                RowType.of(
                        new DataField(0, "f1", DataTypes.INT()),
                        new DataField(1, "f2", DataTypes.STRING()));
        RowType newType =
                RowType.of(
                        new DataField(0, "f1", DataTypes.INT()),
                        new DataField(1, "f2", DataTypes.STRING()),
                        new DataField(2, "f3", DataTypes.DOUBLE()));

        NestedSchemaUtils.generateNestedColumnUpdates(fieldNames, oldType, newType, schemaChanges);

        assertThat(schemaChanges).hasSize(1);
        assertThat(schemaChanges.get(0)).isInstanceOf(SchemaChange.AddColumn.class);
        SchemaChange.AddColumn addColumn = (SchemaChange.AddColumn) schemaChanges.get(0);
        assertThat(addColumn.fieldNames()).containsExactly("row_column", "f3");
        assertThat(addColumn.dataType()).isEqualTo(DataTypes.DOUBLE());
    }

    @Test
    public void testRowTypeDropField() {
        List<String> fieldNames = Arrays.asList("row_column");
        List<SchemaChange> schemaChanges = new ArrayList<>();

        RowType oldType =
                RowType.of(
                        new DataField(0, "f1", DataTypes.INT()),
                        new DataField(1, "f2", DataTypes.STRING()),
                        new DataField(2, "f3", DataTypes.DOUBLE()));
        RowType newType =
                RowType.of(
                        new DataField(0, "f1", DataTypes.INT()),
                        new DataField(1, "f2", DataTypes.STRING()));

        NestedSchemaUtils.generateNestedColumnUpdates(fieldNames, oldType, newType, schemaChanges);

        assertThat(schemaChanges).hasSize(1);
        assertThat(schemaChanges.get(0)).isInstanceOf(SchemaChange.DropColumn.class);
        SchemaChange.DropColumn dropColumn = (SchemaChange.DropColumn) schemaChanges.get(0);
        assertThat(dropColumn.fieldNames()).containsExactly("row_column", "f3");
    }

    @Test
    public void testRowTypeUpdateExistingField() {
        List<String> fieldNames = Arrays.asList("row_column");
        List<SchemaChange> schemaChanges = new ArrayList<>();

        RowType oldType =
                RowType.of(
                        new DataField(0, "f1", DataTypes.INT()),
                        new DataField(1, "f2", DataTypes.STRING()));
        RowType newType =
                RowType.of(
                        new DataField(0, "f1", DataTypes.BIGINT()), // Type changed
                        new DataField(1, "f2", DataTypes.STRING()));

        NestedSchemaUtils.generateNestedColumnUpdates(fieldNames, oldType, newType, schemaChanges);

        assertThat(schemaChanges).hasSize(1);
        assertThat(schemaChanges.get(0)).isInstanceOf(SchemaChange.UpdateColumnType.class);
        SchemaChange.UpdateColumnType typeChange =
                (SchemaChange.UpdateColumnType) schemaChanges.get(0);
        assertThat(typeChange.fieldNames()).containsExactly("row_column", "f1");
        assertThat(typeChange.newDataType()).isEqualTo(DataTypes.BIGINT());
    }

    @Test
    public void testRowTypeUpdateFieldComment() {
        List<String> fieldNames = Arrays.asList("row_column");
        List<SchemaChange> schemaChanges = new ArrayList<>();

        RowType oldType =
                RowType.of(
                        new DataField(0, "f1", DataTypes.INT(), "old comment"),
                        new DataField(1, "f2", DataTypes.STRING()));
        RowType newType =
                RowType.of(
                        new DataField(0, "f1", DataTypes.INT(), "new comment"),
                        new DataField(1, "f2", DataTypes.STRING()));

        NestedSchemaUtils.generateNestedColumnUpdates(fieldNames, oldType, newType, schemaChanges);

        assertThat(schemaChanges).hasSize(1);
        assertThat(schemaChanges.get(0)).isInstanceOf(SchemaChange.UpdateColumnComment.class);
        SchemaChange.UpdateColumnComment commentChange =
                (SchemaChange.UpdateColumnComment) schemaChanges.get(0);
        assertThat(commentChange.fieldNames()).containsExactly("row_column", "f1");
        assertThat(commentChange.newDescription()).isEqualTo("new comment");
    }

    @Test
    public void testRowTypeReorderFields() {
        List<String> fieldNames = Arrays.asList("row_column");
        List<SchemaChange> schemaChanges = new ArrayList<>();

        RowType oldType =
                RowType.of(
                        new DataField(0, "f1", DataTypes.INT()),
                        new DataField(1, "f2", DataTypes.STRING()),
                        new DataField(2, "f3", DataTypes.DOUBLE()));
        RowType newType =
                RowType.of(
                        new DataField(0, "f1", DataTypes.INT()),
                        new DataField(2, "f3", DataTypes.DOUBLE()),
                        new DataField(1, "f2", DataTypes.STRING()) // f2 and f3 swapped
                        );

        assertThatThrownBy(
                        () -> {
                            NestedSchemaUtils.generateNestedColumnUpdates(
                                    fieldNames, oldType, newType, schemaChanges);
                        })
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Order of existing fields");
    }

    @Test
    public void testRowWithIncompatibleTypeThrowsException() {
        List<String> fieldNames = Arrays.asList("row_column");
        List<SchemaChange> schemaChanges = new ArrayList<>();

        RowType oldType = RowType.of(new DataField(0, "f1", DataTypes.INT()));
        ArrayType newType = new ArrayType(true, DataTypes.INT()); // Incompatible type

        assertThatThrownBy(
                        () -> {
                            NestedSchemaUtils.generateNestedColumnUpdates(
                                    fieldNames, oldType, newType, schemaChanges);
                        })
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("can only be updated to row type");
    }

    @Test
    public void testComplexNestedStructure() {
        List<String> fieldNames = Arrays.asList("complex_column");
        List<SchemaChange> schemaChanges = new ArrayList<>();

        // Old: ARRAY<MAP<STRING, ROW<f1 INT, f2 STRING>>>
        RowType oldInnerRowType =
                RowType.of(
                        new DataField(0, "f1", DataTypes.INT()),
                        new DataField(1, "f2", DataTypes.STRING()));
        MapType oldMapType = new MapType(true, DataTypes.STRING(), oldInnerRowType);
        ArrayType oldType = new ArrayType(true, oldMapType);

        // New: ARRAY<MAP<STRING, ROW<f1 BIGINT, f2 STRING, f3 DOUBLE>>>
        RowType newInnerRowType =
                RowType.of(
                        new DataField(0, "f1", DataTypes.BIGINT()), // Type changed
                        new DataField(1, "f2", DataTypes.STRING()),
                        new DataField(2, "f3", DataTypes.DOUBLE()) // New field added
                        );
        MapType newMapType = new MapType(true, DataTypes.STRING(), newInnerRowType);
        ArrayType newType = new ArrayType(true, newMapType);

        NestedSchemaUtils.generateNestedColumnUpdates(fieldNames, oldType, newType, schemaChanges);

        assertThat(schemaChanges).hasSize(2);

        // Should have one type update and one add column
        SchemaChange.UpdateColumnType typeChange =
                schemaChanges.stream()
                        .filter(change -> change instanceof SchemaChange.UpdateColumnType)
                        .map(change -> (SchemaChange.UpdateColumnType) change)
                        .findFirst()
                        .orElse(null);
        assertThat(typeChange).isNotNull();
        assertThat(typeChange.fieldNames())
                .containsExactly("complex_column", "element", "value", "f1");
        assertThat(typeChange.newDataType()).isEqualTo(DataTypes.BIGINT());

        SchemaChange.AddColumn addColumn =
                schemaChanges.stream()
                        .filter(change -> change instanceof SchemaChange.AddColumn)
                        .map(change -> (SchemaChange.AddColumn) change)
                        .findFirst()
                        .orElse(null);
        assertThat(addColumn).isNotNull();
        assertThat(addColumn.fieldNames())
                .containsExactly("complex_column", "element", "value", "f3");
        assertThat(addColumn.dataType()).isEqualTo(DataTypes.DOUBLE());
    }

    @Test
    public void testNestedFieldPaths() {
        List<String> fieldNames = Arrays.asList("outer", "inner");
        List<SchemaChange> schemaChanges = new ArrayList<>();

        // Test that nested field names are properly combined
        NestedSchemaUtils.generateNestedColumnUpdates(
                fieldNames, DataTypes.INT(), DataTypes.BIGINT(), schemaChanges);

        assertThat(schemaChanges).hasSize(1);
        assertThat(schemaChanges.get(0)).isInstanceOf(SchemaChange.UpdateColumnType.class);
        SchemaChange.UpdateColumnType typeChange =
                (SchemaChange.UpdateColumnType) schemaChanges.get(0);
        assertThat(typeChange.fieldNames()).containsExactly("outer", "inner");
    }

    @Test
    public void testEmptyFieldNames() {
        List<String> fieldNames = Collections.emptyList();
        List<SchemaChange> schemaChanges = new ArrayList<>();

        NestedSchemaUtils.generateNestedColumnUpdates(
                fieldNames, DataTypes.INT(), DataTypes.BIGINT(), schemaChanges);

        assertThat(schemaChanges).hasSize(1);
        assertThat(schemaChanges.get(0)).isInstanceOf(SchemaChange.UpdateColumnType.class);
        SchemaChange.UpdateColumnType typeChange =
                (SchemaChange.UpdateColumnType) schemaChanges.get(0);
        assertThat(typeChange.fieldNames()).isEmpty();
    }

    @Test
    public void testVarCharTypeExtension() {
        List<String> fieldNames = Arrays.asList("varchar_column");
        List<SchemaChange> schemaChanges = new ArrayList<>();

        VarCharType oldType = new VarCharType(true, 10);
        VarCharType newType = new VarCharType(true, 20);

        NestedSchemaUtils.generateNestedColumnUpdates(fieldNames, oldType, newType, schemaChanges);

        assertThat(schemaChanges).hasSize(1);
        assertThat(schemaChanges.get(0)).isInstanceOf(SchemaChange.UpdateColumnType.class);
        SchemaChange.UpdateColumnType typeChange =
                (SchemaChange.UpdateColumnType) schemaChanges.get(0);
        assertThat(typeChange.fieldNames()).containsExactly("varchar_column");
        assertThat(typeChange.newDataType()).isEqualTo(newType);
    }

    @Test
    public void testMultipleNestedChanges() {
        List<String> fieldNames = Arrays.asList("root");
        List<SchemaChange> schemaChanges = new ArrayList<>();

        // Old: ROW<f1 INT NULL, f2 STRING>
        RowType oldType =
                RowType.of(
                        new DataField(0, "f1", DataTypes.INT().nullable()),
                        new DataField(1, "f2", DataTypes.STRING()));

        // New: ROW<f1 BIGINT NOT NULL, f2 STRING, f3 DOUBLE>
        RowType newType =
                RowType.of(
                        new DataField(
                                0,
                                "f1",
                                DataTypes.BIGINT().notNull()), // Type and nullability changed
                        new DataField(1, "f2", DataTypes.STRING()),
                        new DataField(2, "f3", DataTypes.DOUBLE()) // New field
                        );

        NestedSchemaUtils.generateNestedColumnUpdates(fieldNames, oldType, newType, schemaChanges);

        assertThat(schemaChanges).hasSize(3);

        // Should have: add column, update type, update nullability
        assertThat(schemaChanges).anyMatch(change -> change instanceof SchemaChange.AddColumn);
        assertThat(schemaChanges)
                .anyMatch(change -> change instanceof SchemaChange.UpdateColumnType);
        assertThat(schemaChanges)
                .anyMatch(change -> change instanceof SchemaChange.UpdateColumnNullability);
    }

    @Test
    public void testArrayOfRowsWithFieldChanges() {
        List<String> fieldNames = Arrays.asList("array_of_rows");
        List<SchemaChange> schemaChanges = new ArrayList<>();

        // Old: ARRAY<ROW<id INT, name STRING>>
        RowType oldRowType =
                RowType.of(
                        new DataField(0, "id", DataTypes.INT()),
                        new DataField(1, "name", DataTypes.STRING()));
        ArrayType oldType = new ArrayType(true, oldRowType);

        // New: ARRAY<ROW<id BIGINT, name STRING, active BOOLEAN>>
        RowType newRowType =
                RowType.of(
                        new DataField(0, "id", DataTypes.BIGINT()), // Type changed
                        new DataField(1, "name", DataTypes.STRING()),
                        new DataField(2, "active", DataTypes.BOOLEAN()) // New field
                        );
        ArrayType newType = new ArrayType(true, newRowType);

        NestedSchemaUtils.generateNestedColumnUpdates(fieldNames, oldType, newType, schemaChanges);

        assertThat(schemaChanges).hasSize(2);

        // Verify field paths include "element" for array access
        SchemaChange.UpdateColumnType typeChange =
                schemaChanges.stream()
                        .filter(change -> change instanceof SchemaChange.UpdateColumnType)
                        .map(change -> (SchemaChange.UpdateColumnType) change)
                        .findFirst()
                        .orElse(null);
        assertThat(typeChange).isNotNull();
        assertThat(typeChange.fieldNames()).containsExactly("array_of_rows", "element", "id");

        SchemaChange.AddColumn addColumn =
                schemaChanges.stream()
                        .filter(change -> change instanceof SchemaChange.AddColumn)
                        .map(change -> (SchemaChange.AddColumn) change)
                        .findFirst()
                        .orElse(null);
        assertThat(addColumn).isNotNull();
        assertThat(addColumn.fieldNames()).containsExactly("array_of_rows", "element", "active");
    }

    @Test
    public void testMapOfRowsWithFieldChanges() {
        List<String> fieldNames = Arrays.asList("map_of_rows");
        List<SchemaChange> schemaChanges = new ArrayList<>();

        // Old: MAP<STRING, ROW<id INT, name STRING>>
        RowType oldRowType =
                RowType.of(
                        new DataField(0, "id", DataTypes.INT()),
                        new DataField(1, "name", DataTypes.STRING()));
        MapType oldType = new MapType(true, DataTypes.STRING(), oldRowType);

        // New: MAP<STRING, ROW<id BIGINT, name STRING, active BOOLEAN>>
        RowType newRowType =
                RowType.of(
                        new DataField(0, "id", DataTypes.BIGINT()), // Type changed
                        new DataField(1, "name", DataTypes.STRING()),
                        new DataField(2, "active", DataTypes.BOOLEAN()) // New field
                        );
        MapType newType = new MapType(true, DataTypes.STRING(), newRowType);

        NestedSchemaUtils.generateNestedColumnUpdates(fieldNames, oldType, newType, schemaChanges);

        assertThat(schemaChanges).hasSize(2);

        // Verify field paths include "value" for map value access
        SchemaChange.UpdateColumnType typeChange =
                schemaChanges.stream()
                        .filter(change -> change instanceof SchemaChange.UpdateColumnType)
                        .map(change -> (SchemaChange.UpdateColumnType) change)
                        .findFirst()
                        .orElse(null);
        assertThat(typeChange).isNotNull();
        assertThat(typeChange.fieldNames()).containsExactly("map_of_rows", "value", "id");

        SchemaChange.AddColumn addColumn =
                schemaChanges.stream()
                        .filter(change -> change instanceof SchemaChange.AddColumn)
                        .map(change -> (SchemaChange.AddColumn) change)
                        .findFirst()
                        .orElse(null);
        assertThat(addColumn).isNotNull();
        assertThat(addColumn.fieldNames()).containsExactly("map_of_rows", "value", "active");
    }

    @Test
    public void testMultisetOfComplexType() {
        List<String> fieldNames = Arrays.asList("multiset_column");
        List<SchemaChange> schemaChanges = new ArrayList<>();

        // Old: MULTISET<ROW<id INT>>
        RowType oldRowType = RowType.of(new DataField(0, "id", DataTypes.INT()));
        MultisetType oldType = new MultisetType(true, oldRowType);

        // New: MULTISET<ROW<id BIGINT, name STRING>>
        RowType newRowType =
                RowType.of(
                        new DataField(0, "id", DataTypes.BIGINT()),
                        new DataField(1, "name", DataTypes.STRING()));
        MultisetType newType = new MultisetType(true, newRowType);

        NestedSchemaUtils.generateNestedColumnUpdates(fieldNames, oldType, newType, schemaChanges);

        assertThat(schemaChanges).hasSize(2);

        // Verify field paths include "element" for multiset element access
        SchemaChange.UpdateColumnType typeChange =
                schemaChanges.stream()
                        .filter(change -> change instanceof SchemaChange.UpdateColumnType)
                        .map(change -> (SchemaChange.UpdateColumnType) change)
                        .findFirst()
                        .orElse(null);
        assertThat(typeChange).isNotNull();
        assertThat(typeChange.fieldNames()).containsExactly("multiset_column", "element", "id");

        SchemaChange.AddColumn addColumn =
                schemaChanges.stream()
                        .filter(change -> change instanceof SchemaChange.AddColumn)
                        .map(change -> (SchemaChange.AddColumn) change)
                        .findFirst()
                        .orElse(null);
        assertThat(addColumn).isNotNull();
        assertThat(addColumn.fieldNames()).containsExactly("multiset_column", "element", "name");
    }
}
