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
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.apache.paimon.flink.clone.schema.CloneHiveSchemaFunction.checkSameFields;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link CloneHiveSchemaFunction}. */
public class CloneHiveSchemaFunctionTest {

    @Test
    public void testCheckSameFieldsForCopiedFiles() {
        List<DataField> source =
                Arrays.asList(
                        new DataField(0, "id", DataTypes.INT()),
                        new DataField(1, "name", DataTypes.STRING()));

        assertThatCode(() -> checkSameFields(source, source, "t")).doesNotThrowAnyException();

        // a wider target would read the copied files with columns they do not have
        List<DataField> wider =
                Arrays.asList(
                        new DataField(0, "id", DataTypes.INT()),
                        new DataField(1, "name", DataTypes.STRING()),
                        new DataField(2, "age", DataTypes.INT()));
        assertThatThrownBy(() -> checkSameFields(source, wider, "t"))
                .isInstanceOf(IllegalStateException.class);

        // Hive columns are nullable, so the copied files may hold nulls
        List<DataField> notNull =
                Arrays.asList(
                        new DataField(0, "id", DataTypes.INT().notNull()),
                        new DataField(1, "name", DataTypes.STRING()));
        assertThatThrownBy(() -> checkSameFields(source, notNull, "t"))
                .isInstanceOf(IllegalStateException.class);
    }
}
