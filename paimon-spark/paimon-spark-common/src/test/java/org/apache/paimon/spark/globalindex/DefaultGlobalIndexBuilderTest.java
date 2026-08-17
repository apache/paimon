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

package org.apache.paimon.spark.globalindex;

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.globalindex.RowIdIndexFieldsExtractor;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.ResolvedFieldPath;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link DefaultGlobalIndexBuilder}. */
class DefaultGlobalIndexBuilderTest {

    @Test
    void testCreateExtractorFromNestedLeafMetadata() {
        RowType profileType =
                DataTypes.ROW(
                        DataTypes.FIELD(1, "name", DataTypes.STRING()),
                        DataTypes.FIELD(2, "zip", DataTypes.INT()));
        RowType tableType = DataTypes.ROW(DataTypes.FIELD(0, "profile", profileType));
        RowType readType = SpecialFields.rowTypeWithRowId(tableType);
        List<DataField> indexFields =
                Arrays.asList(
                        ResolvedFieldPath.resolve(tableType, "profile.zip").get().leafField(),
                        ResolvedFieldPath.resolve(tableType, "profile.name").get().leafField());

        RowIdIndexFieldsExtractor extractor =
                DefaultGlobalIndexBuilder.createIndexFieldsExtractor(readType, indexFields);
        InternalRow fields =
                extractor.extractIndexFields(GenericRow.of(GenericRow.of(null, 310000), 42L));

        assertThat(fields.getInt(0)).isEqualTo(310000);
        assertThat(fields.isNullAt(1)).isTrue();
        assertThat(extractor.extractRowId(GenericRow.of(null, 43L))).isEqualTo(43L);
    }
}
