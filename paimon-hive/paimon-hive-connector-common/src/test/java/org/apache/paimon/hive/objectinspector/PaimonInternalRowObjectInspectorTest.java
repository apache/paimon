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

package org.apache.paimon.hive.objectinspector;

import org.apache.paimon.data.GenericRow;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.apache.paimon.hive.RandomGenericRowDataGenerator.FIELD_COMMENTS;
import static org.apache.paimon.hive.RandomGenericRowDataGenerator.FIELD_NAMES;
import static org.apache.paimon.hive.RandomGenericRowDataGenerator.LOGICAL_TYPES;
import static org.apache.paimon.hive.RandomGenericRowDataGenerator.TYPE_NAMES;
import static org.apache.paimon.hive.RandomGenericRowDataGenerator.generate;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link PaimonInternalRowObjectInspector}. */
public class PaimonInternalRowObjectInspectorTest {

    @Test
    public void testGetStructFieldRef() {
        PaimonInternalRowObjectInspector oi =
                new PaimonInternalRowObjectInspector(FIELD_NAMES, LOGICAL_TYPES, FIELD_COMMENTS);
        List<? extends StructField> structFields = oi.getAllStructFieldRefs();
        List<ObjectInspector.Category> expectedOiCategories =
                Arrays.asList(
                        ObjectInspector.Category.PRIMITIVE,
                        ObjectInspector.Category.PRIMITIVE,
                        ObjectInspector.Category.PRIMITIVE,
                        ObjectInspector.Category.PRIMITIVE,
                        ObjectInspector.Category.PRIMITIVE,
                        ObjectInspector.Category.PRIMITIVE,
                        ObjectInspector.Category.PRIMITIVE,
                        ObjectInspector.Category.PRIMITIVE,
                        ObjectInspector.Category.PRIMITIVE,
                        ObjectInspector.Category.PRIMITIVE,
                        ObjectInspector.Category.PRIMITIVE,
                        ObjectInspector.Category.PRIMITIVE,
                        ObjectInspector.Category.PRIMITIVE,
                        ObjectInspector.Category.PRIMITIVE,
                        ObjectInspector.Category.PRIMITIVE,
                        ObjectInspector.Category.PRIMITIVE,
                        ObjectInspector.Category.PRIMITIVE,
                        ObjectInspector.Category.LIST,
                        ObjectInspector.Category.MAP);

        for (int i = 0; i < FIELD_NAMES.size(); i++) {
            StructField structField = oi.getStructFieldRef(FIELD_NAMES.get(i));
            assertThat(structField).isSameAs(structFields.get(i));
            assertThat(structField.getFieldName()).isEqualTo(FIELD_NAMES.get(i));
            assertThat(structField.getFieldID()).isEqualTo(i);
            assertThat(structField.getFieldComment()).isEqualTo(FIELD_COMMENTS.get(i));
            ObjectInspector fieldOi = structField.getFieldObjectInspector();
            assertThat(fieldOi.getTypeName()).isEqualTo(TYPE_NAMES.get(i));
            assertThat(fieldOi.getCategory()).isEqualTo(expectedOiCategories.get(i));
        }
    }

    @Test
    public void testGetTypeName() {
        PaimonInternalRowObjectInspector oi =
                new PaimonInternalRowObjectInspector(FIELD_NAMES, LOGICAL_TYPES, FIELD_COMMENTS);
        String expected =
                "struct<"
                        + String.join(
                                ",",
                                Arrays.asList(
                                        "f_boolean:boolean",
                                        "f_byte:tinyint",
                                        "f_short:smallint",
                                        "f_int:int",
                                        "f_long:bigint",
                                        "f_float:float",
                                        "f_double:double",
                                        "f_decimal_5_3:decimal(5,3)",
                                        "f_decimal_28_6:decimal(28,6)",
                                        "f_char_10:char(10)",
                                        "f_varchar_10:varchar(10)",
                                        "f_string:string",
                                        "f_binary:binary",
                                        "f_date:date",
                                        "f_time:string",
                                        "f_timestamp:timestamp",
                                        "f_timestamp_ltz:timestamp",
                                        "f_list_long:array<bigint>",
                                        "f_map_string_int:map<string,int>"))
                        + ">";
        assertThat(oi.getTypeName()).isEqualTo(expected);
    }

    @Test
    public void testGetStructFieldData() {
        PaimonInternalRowObjectInspector oi =
                new PaimonInternalRowObjectInspector(FIELD_NAMES, LOGICAL_TYPES, FIELD_COMMENTS);
        GenericRow rowData = generate();
        List<Object> structFieldsData = oi.getStructFieldsDataAsList(rowData);
        for (int i = 0; i < structFieldsData.size(); i++) {
            assertThat(structFieldsData.get(i)).isEqualTo(rowData.getField(i));
        }
        List<? extends StructField> structFields = oi.getAllStructFieldRefs();
        for (int i = 0; i < structFields.size(); i++) {
            assertThat(oi.getStructFieldData(rowData, structFields.get(i)))
                    .isEqualTo(rowData.getField(i));
        }
    }
}
