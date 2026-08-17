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

package org.apache.paimon.index;

import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link GlobalIndexMeta}. */
class GlobalIndexMetaTest {

    @Test
    void testResolveNestedIndexedFields() {
        DataField zip = new DataField(7, "zip", DataTypes.INT());
        DataField country = new DataField(8, "country", DataTypes.STRING());
        RowType rowType =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "id", DataTypes.INT()),
                                new DataField(
                                        1, "profile", new RowType(Arrays.asList(zip, country)))));
        GlobalIndexMeta meta = new GlobalIndexMeta(0, 9, 7, new int[] {8}, null);

        assertThat(meta.getIndexField(rowType)).isEqualTo(zip);
        assertThat(meta.getExtraFields(rowType)).containsExactly(country);
        assertThat(meta.getIndexedFields(rowType)).containsExactly(zip, country);
        assertThat(meta.getIndexedFieldNames(rowType))
                .containsExactly("profile.zip", "profile.country");
        assertThat(meta.getIndexedTopLevelFieldNames(rowType))
                .containsExactly("profile", "profile");
    }

    @Test
    void testResolveTopLevelIndexedFieldNamesUnchanged() {
        DataField id = new DataField(0, "id", DataTypes.INT());
        RowType rowType = new RowType(Collections.singletonList(id));
        GlobalIndexMeta meta = new GlobalIndexMeta(0, 9, 0, null, null);

        assertThat(meta.getIndexedFields(rowType)).containsExactly(id);
        assertThat(meta.getIndexedFieldNames(rowType)).containsExactly("id");
        assertThat(meta.getIndexedTopLevelFieldNames(rowType)).containsExactly("id");
    }
}
