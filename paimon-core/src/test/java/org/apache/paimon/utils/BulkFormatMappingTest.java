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

package org.apache.paimon.utils;

import org.apache.paimon.schema.IndexCastMapping;
import org.apache.paimon.schema.SchemaEvolutionUtil;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

/** Test for {@link BulkFormatMapping.BulkFormatMappingBuilder}. */
public class BulkFormatMappingTest {

    @Test
    public void testTrimKeyFields() {
        List<DataField> keyFields = new ArrayList<>();
        List<DataField> allFields = new ArrayList<>();
        List<DataField> testFields = new ArrayList<>();

        for (int i = 0; i < 10; i++) {
            keyFields.add(
                    new DataField(
                            SpecialFields.KEY_FIELD_ID_START + i,
                            SpecialFields.KEY_FIELD_PREFIX + i,
                            DataTypes.STRING()));
        }

        allFields.addAll(keyFields);
        for (int i = 0; i < 20; i++) {
            allFields.add(new DataField(i, String.valueOf(i), DataTypes.STRING()));
        }

        testFields.add(
                new DataField(
                        SpecialFields.KEY_FIELD_ID_START + 1,
                        SpecialFields.KEY_FIELD_PREFIX + 1,
                        DataTypes.STRING()));
        testFields.add(
                new DataField(
                        SpecialFields.KEY_FIELD_ID_START + 3,
                        SpecialFields.KEY_FIELD_PREFIX + 3,
                        DataTypes.STRING()));
        testFields.add(
                new DataField(
                        SpecialFields.KEY_FIELD_ID_START + 5,
                        SpecialFields.KEY_FIELD_PREFIX + 5,
                        DataTypes.STRING()));
        testFields.add(
                new DataField(
                        SpecialFields.KEY_FIELD_ID_START + 7,
                        SpecialFields.KEY_FIELD_PREFIX + 7,
                        DataTypes.STRING()));
        testFields.add(new DataField(3, String.valueOf(3), DataTypes.STRING()));
        testFields.add(new DataField(4, String.valueOf(4), DataTypes.STRING()));
        testFields.add(new DataField(5, String.valueOf(5), DataTypes.STRING()));
        testFields.add(new DataField(1, String.valueOf(1), DataTypes.STRING()));
        testFields.add(new DataField(6, String.valueOf(6), DataTypes.STRING()));

        Pair<int[], RowType> res =
                BulkFormatMapping.BulkFormatMappingBuilder.trimKeyFields(testFields, allFields);

        Assertions.assertThat(res.getKey()).containsExactly(0, 1, 2, 3, 1, 4, 2, 0, 5);

        List<DataField> fields = res.getRight().getFields();
        Assertions.assertThat(fields.size()).isEqualTo(6);
        Assertions.assertThat(fields.get(0).id()).isEqualTo(1);
        Assertions.assertThat(fields.get(1).id()).isEqualTo(3);
        Assertions.assertThat(fields.get(2).id()).isEqualTo(5);
        Assertions.assertThat(fields.get(3).id()).isEqualTo(7);
        Assertions.assertThat(fields.get(4).id()).isEqualTo(4);
        Assertions.assertThat(fields.get(5).id()).isEqualTo(6);
    }

    @Test
    public void testTrimKeyWithIndexMapping() {
        List<DataField> readTableFields = new ArrayList<>();
        List<DataField> readDataFields = new ArrayList<>();

        readTableFields.add(
                new DataField(
                        SpecialFields.KEY_FIELD_ID_START + 1,
                        SpecialFields.KEY_FIELD_PREFIX + "a",
                        DataTypes.STRING()));
        readTableFields.add(new DataField(0, "0", DataTypes.STRING()));
        readTableFields.add(new DataField(1, "a", DataTypes.STRING()));
        readTableFields.add(new DataField(2, "2", DataTypes.STRING()));
        readTableFields.add(new DataField(3, "3", DataTypes.STRING()));

        readDataFields.add(
                new DataField(
                        SpecialFields.KEY_FIELD_ID_START + 1,
                        SpecialFields.KEY_FIELD_PREFIX + "a",
                        DataTypes.STRING()));
        readDataFields.add(new DataField(0, "0", DataTypes.STRING()));
        readDataFields.add(new DataField(1, "a", DataTypes.STRING()));
        readDataFields.add(new DataField(3, "3", DataTypes.STRING()));

        // build index cast mapping
        IndexCastMapping indexCastMapping =
                SchemaEvolutionUtil.createIndexCastMapping(readTableFields, readDataFields);

        // map from key fields reading to value fields reading
        Pair<int[], RowType> trimmedKeyPair =
                BulkFormatMapping.BulkFormatMappingBuilder.trimKeyFields(
                        readDataFields, readDataFields);

        BulkFormatMapping bulkFormatMapping =
                new BulkFormatMapping(
                        indexCastMapping.getIndexMapping(),
                        indexCastMapping.getCastMapping(),
                        trimmedKeyPair.getLeft(),
                        null,
                        null,
                        null,
                        null);

        Assertions.assertThat(bulkFormatMapping.getIndexMapping()).containsExactly(0, 1, 0, -1, 2);
        List<DataField> trimmed = trimmedKeyPair.getRight().getFields();
        Assertions.assertThat(trimmed.get(0).id()).isEqualTo(1);
        Assertions.assertThat(trimmed.get(1).id()).isEqualTo(0);
        Assertions.assertThat(trimmed.get(2).id()).isEqualTo(3);
        Assertions.assertThat(trimmed.size()).isEqualTo(3);
    }
}
