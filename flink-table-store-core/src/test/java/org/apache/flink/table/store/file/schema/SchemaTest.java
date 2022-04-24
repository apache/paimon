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

package org.apache.flink.table.store.file.schema;

import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.RowType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link Schema}. */
public class SchemaTest {

    @Test
    public void testInvalidPrimaryKeys() {
        List<DataField> fields =
                Arrays.asList(
                        new DataField(0, "f0", new AtomicDataType(new IntType())),
                        new DataField(1, "f1", new AtomicDataType(new IntType())),
                        new DataField(2, "f2", new AtomicDataType(new IntType())));
        List<String> partitionKeys = Collections.singletonList("f0");
        List<String> primaryKeys = Collections.singletonList("f1");
        Map<String, String> options = new HashMap<>();

        Assertions.assertThrows(
                IllegalStateException.class,
                () -> new Schema(1, fields, 10, partitionKeys, primaryKeys, options));
    }

    @Test
    public void testInvalidFieldIds() {
        List<DataField> fields =
                Arrays.asList(
                        new DataField(0, "f0", new AtomicDataType(new IntType())),
                        new DataField(0, "f1", new AtomicDataType(new IntType())));
        Assertions.assertThrows(RuntimeException.class, () -> Schema.currentHighestFieldId(fields));
    }

    @Test
    public void testHighestFieldId() {
        List<DataField> fields =
                Arrays.asList(
                        new DataField(0, "f0", new AtomicDataType(new IntType())),
                        new DataField(20, "f1", new AtomicDataType(new IntType())));
        assertThat(Schema.currentHighestFieldId(fields)).isEqualTo(20);
    }

    @Test
    public void testTypeToSchema() {
        RowType type =
                RowType.of(
                        new LogicalType[] {
                            new IntType(),
                            newLogicalRowType(),
                            new ArrayType(newLogicalRowType()),
                            new MultisetType(newLogicalRowType()),
                            new MapType(newLogicalRowType(), newLogicalRowType())
                        },
                        new String[] {"f0", "f1", "f2", "f3", "f4"});

        List<DataField> fields =
                Arrays.asList(
                        new DataField(0, "f0", new AtomicDataType(new IntType())),
                        new DataField(1, "f1", newRowType(2)),
                        new DataField(3, "f2", new ArrayDataType(newRowType(4))),
                        new DataField(5, "f3", new MultisetDataType(newRowType(6))),
                        new DataField(7, "f4", new MapDataType(newRowType(8), newRowType(9))));

        assertThat(Schema.newFields(type)).isEqualTo(fields);
    }

    static RowType newLogicalRowType() {
        return new RowType(
                Collections.singletonList(new RowType.RowField("nestedField", new IntType())));
    }

    static RowDataType newRowType(int fieldId) {
        return new RowDataType(
                Collections.singletonList(
                        new DataField(fieldId, "nestedField", new AtomicDataType(new IntType()))));
    }
}
