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

package org.apache.paimon.flink.clone.schema;

import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.apache.paimon.flink.clone.schema.CloneHiveSchemaFunction.checkReadableFields;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link CloneHiveSchemaFunction}. */
public class CloneHiveSchemaFunctionTest {

    @Test
    public void testCheckReadableFieldsForCopiedFiles() {
        List<DataField> source =
                Arrays.asList(
                        new DataField(0, "id", DataTypes.INT()),
                        new DataField(
                                1,
                                "s",
                                DataTypes.ROW(DataTypes.FIELD(2, "a", DataTypes.STRING()))));

        // the existing table assigned its own ids to nested fields
        assertThatCode(
                        () ->
                                checkReadableFields(
                                        singletonList(new DataField(0, "tags", arrayOfRow(5))),
                                        singletonList(new DataField(0, "tags", arrayOfRow(9))),
                                        "t"))
                .doesNotThrowAnyException();

        assertCanNotRead(
                source,
                Arrays.asList(
                        new DataField(0, "id", DataTypes.INT()),
                        new DataField(
                                1, "s", DataTypes.ROW(DataTypes.FIELD(4, "b", DataTypes.INT())))));
        assertCanNotRead(
                source,
                Arrays.asList(
                        new DataField(0, "id", DataTypes.BIGINT()),
                        new DataField(
                                1,
                                "s",
                                DataTypes.ROW(DataTypes.FIELD(2, "a", DataTypes.STRING())))));
        // Hive columns are nullable, and an extra field is read as null
        assertCanNotRead(
                source,
                Arrays.asList(
                        new DataField(0, "id", DataTypes.INT()),
                        new DataField(
                                1,
                                "s",
                                DataTypes.ROW(
                                        DataTypes.FIELD(2, "a", DataTypes.STRING()),
                                        DataTypes.FIELD(4, "b", DataTypes.INT().notNull())))));
    }

    private static DataType arrayOfRow(int nestedFieldId) {
        return DataTypes.ARRAY(DataTypes.ROW(DataTypes.FIELD(nestedFieldId, "x", DataTypes.INT())));
    }

    private static void assertCanNotRead(List<DataField> source, List<DataField> existed) {
        assertThatThrownBy(() -> checkReadableFields(source, existed, "t"))
                .isInstanceOf(IllegalStateException.class);
    }
}
