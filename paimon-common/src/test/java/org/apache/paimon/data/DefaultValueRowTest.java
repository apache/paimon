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

package org.apache.paimon.data;

import org.apache.paimon.casting.DefaultValueRow;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link DefaultValueRow} with complex data types support. */
public class DefaultValueRowTest {

    @Test
    public void testDefaultValueRowWithArrayType() {
        // Test with Array default value
        RowType rowType =
                RowType.of(
                        new DataField(0, "id", DataTypes.INT()),
                        new DataField(
                                1,
                                "tags",
                                DataTypes.ARRAY(DataTypes.STRING()),
                                "Default tags",
                                "[tag1, tag2, tag3]"),
                        new DataField(
                                2,
                                "numbers",
                                DataTypes.ARRAY(DataTypes.INT()),
                                "Default numbers",
                                "[1, 2, 3]"));

        DefaultValueRow defaultValueRow = DefaultValueRow.create(rowType);

        GenericRow originalRow = new GenericRow(3);
        originalRow.setField(0, 100);
        originalRow.setField(1, null); // Use default tags
        originalRow.setField(2, null); // Use default numbers

        DefaultValueRow wrappedRow = defaultValueRow.replaceRow(originalRow);

        assertThat(wrappedRow.getInt(0)).isEqualTo(100);

        // Check default array values
        InternalArray tagsValue = wrappedRow.getArray(1);
        assertThat(tagsValue).isNotNull();

        InternalArray numbersValue = wrappedRow.getArray(2);
        assertThat(numbersValue).isNotNull();
    }

    @Test
    public void testDefaultValueRowWithMapType() {
        // Test with Map default value
        RowType rowType =
                RowType.of(
                        new DataField(0, "id", DataTypes.INT()),
                        new DataField(
                                1,
                                "properties",
                                DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING()),
                                "Default properties",
                                "{key1 -> value1, key2 -> value2}"));

        DefaultValueRow defaultValueRow = DefaultValueRow.create(rowType);

        GenericRow originalRow = new GenericRow(2);
        originalRow.setField(0, 200);
        originalRow.setField(1, null); // Use default properties

        DefaultValueRow wrappedRow = defaultValueRow.replaceRow(originalRow);

        assertThat(wrappedRow.getInt(0)).isEqualTo(200);

        InternalMap propertiesValue = wrappedRow.getMap(1);
        assertThat(propertiesValue).isNotNull();
        assertThat(propertiesValue.size()).isEqualTo(2);
    }

    @Test
    public void testDefaultValueRowWithNullMapType() {
        // Test with Map default value
        RowType rowType =
                RowType.of(
                        new DataField(0, "id", DataTypes.INT()),
                        new DataField(
                                1,
                                "properties",
                                DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING()),
                                "Default properties",
                                "null"));

        DefaultValueRow defaultValueRow = DefaultValueRow.create(rowType);

        GenericRow originalRow = new GenericRow(2);
        originalRow.setField(0, 200);
        originalRow.setField(1, null); // Use default properties

        DefaultValueRow wrappedRow = defaultValueRow.replaceRow(originalRow);
        InternalMap propertiesValue = wrappedRow.getMap(1);

        assertThat(wrappedRow.getInt(0)).isEqualTo(200);
        assertThat(propertiesValue).isNull();
    }

    @Test
    public void testDefaultValueRowWithRowType() {
        // Test with Row default value
        RowType innerRowType = RowType.of(DataTypes.INT(), DataTypes.STRING());
        RowType rowType =
                RowType.of(
                        new DataField(0, "id", DataTypes.INT()),
                        new DataField(
                                1,
                                "nested",
                                innerRowType,
                                "Default nested",
                                "{42, default_value}"));

        DefaultValueRow defaultValueRow = DefaultValueRow.create(rowType);

        GenericRow originalRow = new GenericRow(2);
        originalRow.setField(0, 300);
        originalRow.setField(1, null); // Use default nested row

        DefaultValueRow wrappedRow = defaultValueRow.replaceRow(originalRow);

        assertThat(wrappedRow.getInt(0)).isEqualTo(300);

        InternalRow nestedValue = wrappedRow.getRow(1, 2);
        assertThat(nestedValue).isNotNull();
    }

    @Test
    public void testDefaultValueRowErrorHandlingForComplexTypes() {
        // Test error handling for invalid array default
        RowType rowType =
                RowType.of(
                        new DataField(0, "id", DataTypes.INT()),
                        new DataField(
                                1,
                                "invalid_array",
                                DataTypes.ARRAY(DataTypes.INT()),
                                "Invalid array",
                                "[1, 2, invalid]"));

        assertThatThrownBy(() -> DefaultValueRow.create(rowType))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Cannot parse");

        // Test error handling for invalid map default
        RowType mapRowType =
                RowType.of(
                        new DataField(0, "id", DataTypes.INT()),
                        new DataField(
                                1,
                                "invalid_map",
                                DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()),
                                "Invalid map",
                                "{key1 -> invalid_value}"));

        assertThatThrownBy(() -> DefaultValueRow.create(mapRowType))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Cannot parse");
    }
}
