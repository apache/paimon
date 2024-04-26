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
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.CoreOptions.AGG_FUNCTION;
import static org.apache.paimon.CoreOptions.BUCKET;
import static org.apache.paimon.CoreOptions.FIELDS_PREFIX;
import static org.apache.paimon.CoreOptions.MERGE_ENGINE;
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

    @Test
    public void testSequenceField() {
        List<DataField> fields =
                Arrays.asList(
                        new DataField(0, "f0", DataTypes.INT()),
                        new DataField(1, "f1", DataTypes.INT()),
                        new DataField(2, "f2", DataTypes.INT()),
                        new DataField(3, "f3", DataTypes.INT()));
        List<String> partitionKeys = Collections.singletonList("f0");
        List<String> primaryKeys = Collections.singletonList("f1");
        Map<String, String> options = new HashMap<>();

        TableSchema schema =
                new TableSchema(1, fields, 10, partitionKeys, primaryKeys, options, "");

        options.put(SEQUENCE_FIELD.key(), "f3");
        assertThatThrownBy(() -> validateTableSchema(schema))
                .hasMessageContaining(
                        "You can not use sequence.field in cross partition update case (Primary key constraint '[f1]' not include all partition fields '[f0]').");

        options.put(SEQUENCE_FIELD.key(), "f4");
        assertThatThrownBy(() -> validateTableSchema(schema))
                .hasMessageContaining("Sequence field: 'f4' can not be found in table schema.");

        options.put(SEQUENCE_FIELD.key(), "f2,f3,f3");
        assertThatThrownBy(() -> validateTableSchema(schema))
                .hasMessageContaining("Sequence field 'f3' is defined repeatedly.");

        options.put(SEQUENCE_FIELD.key(), "f3");
        options.put(MERGE_ENGINE.key(), CoreOptions.MergeEngine.FIRST_ROW.toString());
        assertThatThrownBy(() -> validateTableSchema(schema))
                .hasMessageContaining(
                        "Do not support use sequence field on FIRST_MERGE merge engine.");

        options.put(FIELDS_PREFIX + ".f3." + AGG_FUNCTION, "max");
        assertThatThrownBy(() -> validateTableSchema(schema))
                .hasMessageContaining("Should not define aggregation on sequence field: 'f3'.");
    }

    @Test
    public void testFieldsPrefix() {
        List<DataField> fields =
                Arrays.asList(
                        new DataField(0, "f0", DataTypes.INT()),
                        new DataField(1, "f1", DataTypes.INT()),
                        new DataField(2, "f2", DataTypes.INT()));
        List<String> primaryKeys = Collections.singletonList("f0");
        Map<String, String> options = new HashMap<>();
        options.put(MERGE_ENGINE.key(), CoreOptions.MergeEngine.AGGREGATE.toString());
        options.put(FIELDS_PREFIX + ".f1." + AGG_FUNCTION, "max");
        options.put(FIELDS_PREFIX + ".fake_col." + AGG_FUNCTION, "max");
        TableSchema schema =
                new TableSchema(1, fields, 10, Collections.emptyList(), primaryKeys, options, "");
        assertThatThrownBy(() -> validateTableSchema(schema))
                .hasMessageContaining("Field fake_col can not be found in table schema.");
    }

    @Test
    public void testBucket() {
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

        options.put(BUCKET.key(), "-2");
        assertThatThrownBy(() -> validateTableSchema(schema))
                .hasMessageContaining("The number of buckets needs to be greater than 0.");
    }

    static RowType newRowType(boolean isNullable, int fieldId) {
        return new RowType(
                isNullable,
                Collections.singletonList(new DataField(fieldId, "nestedField", DataTypes.INT())));
    }
}
