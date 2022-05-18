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

import org.apache.flink.table.store.file.utils.JsonSerdeUtil;
import org.apache.flink.table.types.logical.IntType;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.store.file.schema.SchemaTest.newRowType;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for serialize {@link Schema}. */
public class SchemaSerializationTest {

    @Test
    public void testSchema() {
        List<DataField> fields =
                Arrays.asList(
                        new DataField(0, "f0", new AtomicDataType(new IntType())),
                        new DataField(1, "f1", newRowType(2)),
                        new DataField(3, "f2", new ArrayDataType(newRowType(4))),
                        new DataField(5, "f3", new MultisetDataType(newRowType(6))),
                        new DataField(7, "f4", new MapDataType(newRowType(8), newRowType(9))));
        List<String> partitionKeys = Collections.singletonList("f0");
        List<String> primaryKeys = Arrays.asList("f0", "f1");
        Map<String, String> options = new HashMap<>();
        options.put("option-1", "value-1");
        options.put("option-2", "value-2");

        Schema schema = new Schema(1, fields, 10, partitionKeys, primaryKeys, options);
        String serialized = JsonSerdeUtil.toJson(schema);

        Schema deserialized = JsonSerdeUtil.fromJson(serialized, Schema.class);
        assertThat(deserialized).isEqualTo(schema);
    }
}
