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

package org.apache.paimon.types;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link MultisetType}. */
class MultisetTypeTest {

    @Test
    void testEqualsIgnoreFieldIdRecursesIntoElementType() {
        MultisetType left =
                DataTypes.MULTISET(DataTypes.ROW(DataTypes.FIELD(1, "f", DataTypes.INT())));
        MultisetType rightDifferentFieldId =
                DataTypes.MULTISET(DataTypes.ROW(DataTypes.FIELD(2, "f", DataTypes.INT())));

        // field ids differ, but names and element types match
        assertThat(left.equals(rightDifferentFieldId)).isFalse();
        assertThat(left.equalsIgnoreFieldId(rightDifferentFieldId)).isTrue();
    }

    @Test
    void testEqualsIgnoreFieldIdRejectsDifferentElement() {
        MultisetType left =
                DataTypes.MULTISET(DataTypes.ROW(DataTypes.FIELD(1, "f", DataTypes.INT())));
        MultisetType differentName =
                DataTypes.MULTISET(DataTypes.ROW(DataTypes.FIELD(1, "g", DataTypes.INT())));
        MultisetType differentType =
                DataTypes.MULTISET(DataTypes.ROW(DataTypes.FIELD(1, "f", DataTypes.BIGINT())));

        assertThat(left.equalsIgnoreFieldId(differentName)).isFalse();
        assertThat(left.equalsIgnoreFieldId(differentType)).isFalse();
    }

    @Test
    void testIsPrunedFromRecursesIntoElementType() {
        MultisetType pruned =
                DataTypes.MULTISET(DataTypes.ROW(DataTypes.FIELD(1, "f", DataTypes.INT())));
        MultisetType full =
                DataTypes.MULTISET(
                        DataTypes.ROW(
                                DataTypes.FIELD(1, "f", DataTypes.INT()),
                                DataTypes.FIELD(2, "g", DataTypes.INT())));

        assertThat(pruned.equals(full)).isFalse();
        assertThat(pruned.isPrunedFrom(full)).isTrue();
    }

    @Test
    void testIsPrunedFromRejectsDifferentElement() {
        MultisetType left =
                DataTypes.MULTISET(DataTypes.ROW(DataTypes.FIELD(1, "f", DataTypes.INT())));
        MultisetType differentType =
                DataTypes.MULTISET(DataTypes.ROW(DataTypes.FIELD(1, "f", DataTypes.BIGINT())));

        assertThat(left.isPrunedFrom(differentType)).isFalse();
    }
}
