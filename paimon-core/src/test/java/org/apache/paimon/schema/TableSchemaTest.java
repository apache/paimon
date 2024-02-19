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

import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.CoreOptions.BUCKET;
import static org.apache.paimon.CoreOptions.SEQUENCE_FIELD;
import static org.apache.paimon.schema.SchemaValidation.validateTableSchema;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link TableSchema}. */
public class TableSchemaTest {

    @Test
    public void testCrossPartition() {
        List<DataField> fields =
                Arrays.asList(
                        new DataField(0, "f0", DataTypes.INT()),
                        new DataField(1, "f1", DataTypes.INT()),
                        new DataField(2, "f2", DataTypes.INT()));
        List<String> partitionKeys = Collections.singletonList("f0");
        List<String> primaryKeys = Collections.singletonList("f1");
        Map<String, String> options = new HashMap<>();

        TableSchema schema =
                new TableSchema(1, fields, 10, partitionKeys, primaryKeys, options, "");
        assertThat(schema.crossPartitionUpdate()).isTrue();

        options.put(BUCKET.key(), "1");
        assertThatThrownBy(() -> validateTableSchema(schema))
                .hasMessageContaining("You should use dynamic bucket");

        options.put(BUCKET.key(), "-1");
        validateTableSchema(schema);

        options.put(SEQUENCE_FIELD.key(), "f2");
        assertThatThrownBy(() -> validateTableSchema(schema))
                .hasMessageContaining("You can not use sequence.field");
    }

    @Test
    public void testInvalidFieldIds() {
        List<DataField> fields =
                Arrays.asList(
                        new DataField(0, "f0", DataTypes.INT()),
                        new DataField(0, "f1", DataTypes.INT()));

        assertThatThrownBy(() -> RowType.currentHighestFieldId(fields))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("Broken schema, field id 0 is duplicated.");
    }

    @Test
    public void testHighestFieldId() {
        List<DataField> fields =
                Arrays.asList(
                        new DataField(0, "f0", DataTypes.INT()),
                        new DataField(20, "f1", DataTypes.INT()));
        assertThat(RowType.currentHighestFieldId(fields)).isEqualTo(20);

        List<DataField> fields1 =
                Arrays.asList(
                        new DataField(0, "f0", DataTypes.INT()),
                        new DataField(
                                1,
                                "f1",
                                DataTypes.ROW(
                                        new DataField(2, "f0", DataTypes.STRING()),
                                        new DataField(3, "f1", DataTypes.ARRAY(DataTypes.INT())))),
                        new DataField(4, "f2", DataTypes.STRING()),
                        new DataField(
                                5,
                                "f3",
                                DataTypes.ARRAY(
                                        DataTypes.ROW(
                                                new DataField(6, "f0", DataTypes.BIGINT())))));
        assertThat(RowType.currentHighestFieldId(fields1)).isEqualTo(6);

        List<DataField> fields2 =
                Arrays.asList(
                        new DataField(0, "f0", DataTypes.INT()),
                        new DataField(
                                1,
                                "f1",
                                DataTypes.ROW(
                                        DataTypes.STRING(), DataTypes.ARRAY(DataTypes.INT()))),
                        new DataField(2, "f2", DataTypes.STRING()));
        assertThatThrownBy(() -> RowType.currentHighestFieldId(fields2))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("Broken schema, field id 0 is duplicated.");
    }

    static RowType newRowType(boolean isNullable, int fieldId) {
        return new RowType(
                isNullable,
                Collections.singletonList(new DataField(fieldId, "nestedField", DataTypes.INT())));
    }
}
