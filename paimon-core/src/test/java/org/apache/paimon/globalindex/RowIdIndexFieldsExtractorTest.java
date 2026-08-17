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

package org.apache.paimon.globalindex;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.InstantiationUtil;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link RowIdIndexFieldsExtractor}. */
class RowIdIndexFieldsExtractorTest {

    private static final RowType ADDRESS_TYPE =
            DataTypes.ROW(
                    DataTypes.FIELD(3, "city", DataTypes.STRING()),
                    DataTypes.FIELD(4, "zip", DataTypes.INT()));

    private static final RowType PROFILE_TYPE =
            DataTypes.ROW(
                    DataTypes.FIELD(1, "name", DataTypes.STRING()),
                    DataTypes.FIELD(2, "address", ADDRESS_TYPE));

    private static final RowType READ_TYPE =
            SpecialFields.rowTypeWithRowId(
                    DataTypes.ROW(
                            DataTypes.FIELD(0, "partition", DataTypes.STRING()),
                            DataTypes.FIELD(5, "profile", PROFILE_TYPE),
                            DataTypes.FIELD(6, "profile.address.city", DataTypes.STRING())));

    @Test
    void testExtractNestedField() {
        RowIdIndexFieldsExtractor extractor = extractor("profile.address.zip");
        GenericRow record = record(GenericRow.of(string("Hangzhou"), 310000), 42L);

        assertThat(extractor.extractIndexField(record)).isEqualTo(310000);
        assertThat(extractor.extractRowId(record)).isEqualTo(42L);
    }

    @Test
    void testExactTopLevelNameTakesPrecedence() {
        RowIdIndexFieldsExtractor extractor = extractor("profile.address.city");
        GenericRow record = record(GenericRow.of(string("nested"), 310000), 42L);

        assertThat(extractor.extractIndexField(record)).isEqualTo(string("top-level"));
    }

    @Test
    void testNestedNullIsPropagated() {
        RowIdIndexFieldsExtractor extractor = extractor("profile.address.zip");

        assertThat(extractor.extractIndexField(record(null, 42L))).isNull();
        assertThat(
                        extractor.extractIndexField(
                                GenericRow.of(string("p0"), null, string("top-level"), 42L)))
                .isNull();
        assertThat(
                        extractor.extractIndexField(
                                record(GenericRow.of(string("Hangzhou"), null), 42L)))
                .isNull();
    }

    @Test
    void testNestedGetterIsRestoredAfterSerialization() throws Exception {
        RowIdIndexFieldsExtractor extractor = extractor("profile.address.zip");
        GenericRow record = record(GenericRow.of(string("Hangzhou"), 310000), 42L);
        assertThat(extractor.extractIndexField(record)).isEqualTo(310000);

        RowIdIndexFieldsExtractor restored = InstantiationUtil.clone(extractor);

        assertThat(restored.extractIndexField(record)).isEqualTo(310000);
    }

    @Test
    void testExtractMultipleNestedFieldsInDeclaredOrder() {
        RowIdIndexFieldsExtractor extractor =
                new RowIdIndexFieldsExtractor(
                        READ_TYPE,
                        Collections.singletonList("partition"),
                        Arrays.asList("profile.address.zip", "profile.name"));
        GenericRow record = record(GenericRow.of(string("Hangzhou"), 310000), 42L);

        InternalRow indexFields = extractor.extractIndexFields(record);

        assertThat(indexFields.getInt(0)).isEqualTo(310000);
        assertThat(indexFields.getString(1)).isEqualTo(string("Alice"));
    }

    @Test
    void testRejectInvalidPath() {
        assertThatThrownBy(() -> extractor("profile.missing"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("profile.missing");
        assertThatThrownBy(() -> extractor("partition.value"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("partition.value");
    }

    private static RowIdIndexFieldsExtractor extractor(String indexField) {
        return new RowIdIndexFieldsExtractor(
                READ_TYPE, Collections.singletonList("partition"), indexField);
    }

    private static GenericRow record(GenericRow address, long rowId) {
        return GenericRow.of(
                string("p0"), GenericRow.of(string("Alice"), address), string("top-level"), rowId);
    }

    private static BinaryString string(String value) {
        return BinaryString.fromString(value);
    }
}
