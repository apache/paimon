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
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link TableSchema}. */
class TableSchemaTest {

    @Test
    void testNestedProjectionRequiresEnabledOption() {
        TableSchema disabled = nestedSchema(Collections.emptyMap());
        assertThatThrownBy(() -> disabled.project(Collections.singletonList("nest.a")))
                .isInstanceOf(IndexOutOfBoundsException.class);

        Map<String, String> enabledOptions = new HashMap<>();
        enabledOptions.put(CoreOptions.DATA_EVOLUTION_NESTED_FIELD_ENABLED.key(), "true");
        TableSchema enabled = nestedSchema(enabledOptions);
        RowType projected = enabled.project(Collections.singletonList("nest.a")).logicalRowType();

        assertThat(projected.getFieldNames()).containsExactly("nest");
        assertThat(((RowType) projected.getTypeAt(0)).getFieldNames()).containsExactly("a");
    }

    @Test
    void testDisabledProjectionTreatsDotAsPartOfTopLevelName() {
        TableSchema schema =
                new TableSchema(
                        1L,
                        Arrays.asList(
                                new DataField(1, "nest.a", DataTypes.INT()),
                                new DataField(
                                        2,
                                        "nest",
                                        DataTypes.ROW(new DataField(3, "a", DataTypes.STRING())))),
                        3,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyMap(),
                        "");

        assertThat(schema.project(Collections.singletonList("nest.a")).fields())
                .extracting(DataField::id)
                .containsExactly(1);
    }

    private static TableSchema nestedSchema(Map<String, String> options) {
        return new TableSchema(
                1L,
                Collections.singletonList(
                        new DataField(
                                1,
                                "nest",
                                DataTypes.ROW(
                                        new DataField(2, "a", DataTypes.INT()),
                                        new DataField(3, "b", DataTypes.STRING())))),
                3,
                Collections.emptyList(),
                Collections.emptyList(),
                options,
                "");
    }
}
