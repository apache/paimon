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
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.MultisetType;
import org.apache.paimon.utils.JsonSerdeUtil;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.schema.TableSchema.PAIMON_07_VERSION;
import static org.apache.paimon.schema.TableSchema.PAIMON_08_VERSION;
import static org.apache.paimon.schema.TableSchemaTest.newRowType;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for serialize {@link TableSchema}. */
public class TableSchemaSerializationTest {

    @Test
    public void testSchema() {
        List<DataField> fields =
                Arrays.asList(
                        new DataField(0, "f0", new IntType()),
                        new DataField(1, "f1", newRowType(false, 2)),
                        new DataField(3, "f2", new ArrayType(false, newRowType(true, 4))),
                        new DataField(5, "f3", new MultisetType(true, newRowType(false, 6))),
                        new DataField(
                                7,
                                "f4",
                                new MapType(true, newRowType(true, 8), newRowType(false, 9))));
        List<String> partitionKeys = Collections.singletonList("f0");
        List<String> primaryKeys = Arrays.asList("f0", "f1");
        Map<String, String> options = new HashMap<>();
        options.put("option-1", "value-1");
        options.put("option-2", "value-2");

        assertSerDeser(
                new TableSchema(1, fields, 10, partitionKeys, primaryKeys, options, "my_comment"),
                Collections.emptyMap());
        Map<String, String> additionalOptions = new HashMap<>();
        additionalOptions.put("file.format", "orc");
        assertSerDeser(
                new TableSchema(
                        PAIMON_08_VERSION,
                        1,
                        fields,
                        10,
                        partitionKeys,
                        primaryKeys,
                        options,
                        "my_comment",
                        System.currentTimeMillis()),
                additionalOptions);

        additionalOptions.put("bucket", "1");
        assertSerDeser(
                new TableSchema(
                        PAIMON_07_VERSION,
                        1,
                        fields,
                        10,
                        partitionKeys,
                        primaryKeys,
                        options,
                        "my_comment",
                        System.currentTimeMillis()),
                additionalOptions);
    }

    private void assertSerDeser(TableSchema tableSchema, Map<String, String> additionalOptions) {
        String serialized = JsonSerdeUtil.toJson(tableSchema);
        TableSchema deserialized = JsonSerdeUtil.fromJson(serialized, TableSchema.class);
        Map<String, String> newOptions = new HashMap<>(tableSchema.options());
        newOptions.putAll(additionalOptions);
        assertThat(deserialized.version()).isEqualTo(tableSchema.version());
        assertThat(deserialized).isEqualTo(tableSchema.copy(newOptions));
    }
}
