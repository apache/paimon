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
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link TableSchema}. */
public class TableSchemaTest {

    @Test
    public void testInvalidPrimaryKeys() {
        List<DataField> fields =
                Arrays.asList(
                        new DataField(0, "f0", new IntType()),
                        new DataField(1, "f1", new IntType()),
                        new DataField(2, "f2", new IntType()));
        List<String> partitionKeys = Collections.singletonList("f0");
        List<String> primaryKeys = Collections.singletonList("f1");
        Map<String, String> options = new HashMap<>();

        Assertions.assertThrows(
                IllegalStateException.class,
                () -> new TableSchema(1, fields, 10, partitionKeys, primaryKeys, options, ""));
    }

    @Test
    public void testInvalidFieldIds() {
        List<DataField> fields =
                Arrays.asList(
                        new DataField(0, "f0", new IntType()),
                        new DataField(0, "f1", new IntType()));
        Assertions.assertThrows(
                RuntimeException.class, () -> RowType.currentHighestFieldId(fields));
    }

    @Test
    public void testHighestFieldId() {
        List<DataField> fields =
                Arrays.asList(
                        new DataField(0, "f0", new IntType()),
                        new DataField(20, "f1", new IntType()));
        assertThat(RowType.currentHighestFieldId(fields)).isEqualTo(20);
    }

    static RowType newRowType(boolean isNullable, int fieldId) {
        return new RowType(
                isNullable,
                Collections.singletonList(new DataField(fieldId, "nestedField", new IntType())));
    }
}
