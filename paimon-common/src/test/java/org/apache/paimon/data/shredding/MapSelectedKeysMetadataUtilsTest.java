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

package org.apache.paimon.data.shredding;

import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link MapSelectedKeysMetadataUtils}. */
class MapSelectedKeysMetadataUtilsTest {

    @Test
    void testBuildAndParseMetadata() {
        String metadata =
                MapSelectedKeysMetadataUtils.buildMapSelectedKeysMetadata(
                        Arrays.asList("key1", "key2"));

        assertThat(metadata).isEqualTo("__PAIMON_MAP_SELECTED_KEYS:key1;key2");
        assertThat(MapSelectedKeysMetadataUtils.isMapSelectedKeysMetadata(metadata)).isTrue();
        assertThat(MapSelectedKeysMetadataUtils.selectedKeys(metadata))
                .containsExactly("key1", "key2");
    }

    @Test
    void testBuildAndParseMetadataWithEmptyKey() {
        String metadata =
                MapSelectedKeysMetadataUtils.buildMapSelectedKeysMetadata(
                        Arrays.asList("key1", "", "key2"));

        String expectedMetadata = String.join(";", "__PAIMON_MAP_SELECTED_KEYS:key1", "", "key2");
        assertThat(metadata).isEqualTo(expectedMetadata);
        assertThat(MapSelectedKeysMetadataUtils.selectedKeys(metadata))
                .containsExactly("key1", "", "key2");
        assertThat(MapSelectedKeysMetadataUtils.selectedKeys("__PAIMON_MAP_SELECTED_KEYS:"))
                .containsExactly("");
    }

    @Test
    void testBuildMetadataRejectsDuplicateKeys() {
        assertThatThrownBy(
                        () ->
                                MapSelectedKeysMetadataUtils.buildMapSelectedKeysMetadata(
                                        Arrays.asList("key1", "key2", "key1")))
                .hasMessageContaining("Selected key must not be duplicated: key1");
        assertThatThrownBy(
                        () ->
                                MapSelectedKeysMetadataUtils.buildMapSelectedKeysMetadata(
                                        Arrays.asList("key1", "", "")))
                .hasMessageContaining("Selected key must not be duplicated");
    }

    @Test
    void testWithSelectedKeys() {
        DataField field =
                DataTypes.FIELD(
                        1,
                        "attrs",
                        DataTypes.MAP(DataTypes.STRING(), DataTypes.BIGINT()),
                        "user comment");
        RowType selectedType =
                DataTypes.ROW(
                        DataTypes.FIELD(0, "0", DataTypes.BIGINT()),
                        DataTypes.FIELD(1, "1", DataTypes.BIGINT()));

        DataField rewritten =
                MapSelectedKeysMetadataUtils.withSelectedKeys(
                        field, selectedType, Arrays.asList("key1", "key2"));

        assertThat(rewritten.id()).isEqualTo(1);
        assertThat(rewritten.name()).isEqualTo("attrs");
        assertThat(rewritten.type()).isEqualTo(selectedType);
        assertThat(rewritten.description()).isEqualTo("__PAIMON_MAP_SELECTED_KEYS:key1;key2");
        assertThat(MapSelectedKeysMetadataUtils.isMapSelectedKeysField(rewritten)).isTrue();
    }

    @Test
    void testErrors() {
        assertThatThrownBy(
                        () ->
                                MapSelectedKeysMetadataUtils.buildMapSelectedKeysMetadata(
                                        Collections.emptyList()))
                .hasMessageContaining("Selected keys must not be empty.");
        assertThatThrownBy(
                        () ->
                                MapSelectedKeysMetadataUtils.buildMapSelectedKeysMetadata(
                                        Arrays.asList("key1", null)))
                .hasMessageContaining("Selected key must not be null.");
        assertThatThrownBy(
                        () ->
                                MapSelectedKeysMetadataUtils.buildMapSelectedKeysMetadata(
                                        Arrays.asList("key;1")))
                .hasMessageContaining("Selected key must not contain ';'");
        assertThatThrownBy(() -> MapSelectedKeysMetadataUtils.selectedKeys("invalid"))
                .hasMessageContaining("Invalid selected-key MAP metadata");
        assertThatThrownBy(
                        () ->
                                MapSelectedKeysMetadataUtils.withSelectedKeys(
                                        DataTypes.FIELD(
                                                1,
                                                "attrs",
                                                DataTypes.MAP(
                                                        DataTypes.STRING(), DataTypes.BIGINT())),
                                        DataTypes.BIGINT(),
                                        Arrays.asList("key1")))
                .hasMessageContaining("Selected-key MAP read type must be ROW.");
    }
}
