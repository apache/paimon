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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link ResolvedFieldPath}. */
class ResolvedFieldPathTest {

    private static final int ID = 0;
    private static final int PROFILE = 1;
    private static final int NAME = 2;
    private static final int ADDRESS = 3;
    private static final int CITY = 4;
    private static final int ZIP = 5;
    private static final int DOTTED_TOP_LEVEL = 6;
    private static final int ITEMS = 7;
    private static final int ITEM_VALUE = 8;

    private static RowType rowType() {
        RowType addressType =
                DataTypes.ROW(
                        DataTypes.FIELD(CITY, "city", DataTypes.STRING()),
                        DataTypes.FIELD(ZIP, "zip", DataTypes.INT()));
        RowType profileType =
                DataTypes.ROW(
                        DataTypes.FIELD(NAME, "name", DataTypes.STRING()),
                        DataTypes.FIELD(ADDRESS, "address", addressType));
        RowType itemType = DataTypes.ROW(DataTypes.FIELD(ITEM_VALUE, "value", DataTypes.INT()));
        return DataTypes.ROW(
                DataTypes.FIELD(ID, "id", DataTypes.BIGINT()),
                DataTypes.FIELD(PROFILE, "profile", profileType),
                DataTypes.FIELD(DOTTED_TOP_LEVEL, "profile.address.zip", DataTypes.STRING()),
                DataTypes.FIELD(ITEMS, "items", DataTypes.ARRAY(itemType)));
    }

    @Test
    void testResolveNestedRowFieldByDottedName() {
        ResolvedFieldPath path = ResolvedFieldPath.resolve(rowType(), "profile.address.city").get();

        assertThat(path.fieldNames()).containsExactly("profile", "address", "city");
        assertThat(path.indexes()).containsExactly(1, 1, 0);
        assertThat(path.nestedIndexes()).containsExactly(1, 0);
        assertThat(path.nestedArities()).containsExactly(2, 2);
        assertThat(path.topLevelIndex()).isEqualTo(1);
        assertThat(path.topLevelField().id()).isEqualTo(PROFILE);
        assertThat(path.leafField().id()).isEqualTo(CITY);
        assertThat(path.fullName()).isEqualTo("profile.address.city");
        assertThat(path.isNested()).isTrue();
    }

    @Test
    void testExactTopLevelNameTakesPrecedenceOverDottedPath() {
        ResolvedFieldPath path = ResolvedFieldPath.resolve(rowType(), "profile.address.zip").get();

        assertThat(path.fieldNames()).containsExactly("profile.address.zip");
        assertThat(path.indexes()).containsExactly(2);
        assertThat(path.nestedIndexes()).isEmpty();
        assertThat(path.nestedArities()).isEmpty();
        assertThat(path.leafField().id()).isEqualTo(DOTTED_TOP_LEVEL);
        assertThat(path.isNested()).isFalse();
    }

    @Test
    void testStructuredPathCanAddressFieldWhenDottedNameIsAmbiguous() {
        ResolvedFieldPath path =
                ResolvedFieldPath.resolve(rowType(), Arrays.asList("profile", "address", "zip"))
                        .get();

        assertThat(path.fieldNames()).containsExactly("profile", "address", "zip");
        assertThat(path.indexes()).containsExactly(1, 1, 1);
        assertThat(path.leafField().id()).isEqualTo(ZIP);
    }

    @Test
    void testResolveNestedRowFieldByLeafId() {
        ResolvedFieldPath path = ResolvedFieldPath.resolve(rowType(), ZIP).get();

        assertThat(path.fieldNames()).containsExactly("profile", "address", "zip");
        assertThat(path.indexes()).containsExactly(1, 1, 1);
        assertThat(path.leafField().id()).isEqualTo(ZIP);

        ResolvedFieldPath topLevelPath = ResolvedFieldPath.resolve(rowType(), ID).get();
        assertThat(topLevelPath.fieldNames()).containsExactly("id");
        assertThat(topLevelPath.indexes()).containsExactly(0);
    }

    @Test
    void testUnsupportedOrMissingPathsReturnEmpty() {
        RowType rowType = rowType();

        assertThat(ResolvedFieldPath.resolve(rowType, "missing")).isEmpty();
        assertThat(ResolvedFieldPath.resolve(rowType, "profile.missing")).isEmpty();
        assertThat(ResolvedFieldPath.resolve(rowType, "profile..city")).isEmpty();
        assertThat(ResolvedFieldPath.resolve(rowType, "id.value")).isEmpty();
        assertThat(ResolvedFieldPath.resolve(rowType, "")).isEmpty();
        assertThat(ResolvedFieldPath.resolve(rowType, ITEM_VALUE)).isEmpty();
        assertThat(ResolvedFieldPath.resolve(rowType, 999)).isEmpty();
        assertThat(ResolvedFieldPath.resolve(rowType, Collections.emptyList())).isEmpty();
        assertThat(ResolvedFieldPath.resolve(rowType, Arrays.asList("profile", ""))).isEmpty();
    }

    @Test
    void testReturnedIndexesAreDefensiveCopies() {
        Optional<ResolvedFieldPath> resolved =
                ResolvedFieldPath.resolve(rowType(), "profile.address.city");
        assertThat(resolved).isPresent();
        assertThatThrownBy(() -> resolved.get().fields().clear())
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> resolved.get().fieldNames().clear())
                .isInstanceOf(UnsupportedOperationException.class);

        int[] indexes = resolved.get().indexes();
        int[] nestedIndexes = resolved.get().nestedIndexes();
        int[] nestedArities = resolved.get().nestedArities();
        indexes[0] = 99;
        nestedIndexes[0] = 99;
        nestedArities[0] = 99;

        assertThat(resolved.get().indexes()).containsExactly(1, 1, 0);
        assertThat(resolved.get().nestedIndexes()).containsExactly(1, 0);
        assertThat(resolved.get().nestedArities()).containsExactly(2, 2);
    }

    @Test
    void testResolveAllAndProjectDistinctTopLevelFields() {
        RowType rowType = rowType();
        List<ResolvedFieldPath> paths =
                ResolvedFieldPath.resolveAll(
                                rowType,
                                Arrays.asList("profile.address.city", "profile.name", "id"))
                        .get();

        assertThat(paths)
                .extracting(ResolvedFieldPath::fullName)
                .containsExactly("profile.address.city", "profile.name", "id");
        assertThat(ResolvedFieldPath.projectTopLevel(rowType, paths).getFieldNames())
                .containsExactly("profile", "id");
        assertThat(ResolvedFieldPath.resolveAll(rowType, Arrays.asList("profile.name", "missing")))
                .isEmpty();
    }
}
