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

package org.apache.paimon.iceberg.metadata;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@code next-row-id} row-lineage field of {@link IcebergMetadata}. */
class IcebergMetadataNextRowIdTest {

    @Test
    void testV3MetadataRoundTripsNextRowId() {
        String v3Json =
                "{\"format-version\":3,\"table-uuid\":\"uuid\",\"location\":\"loc\",\"next-row-id\":42}";
        IcebergMetadata metadata = IcebergMetadata.fromJson(v3Json);
        assertThat(metadata.nextRowId()).isEqualTo(42L);

        String serialized = metadata.toJson();
        assertThat(serialized).contains("next-row-id");
        assertThat(IcebergMetadata.fromJson(serialized).nextRowId()).isEqualTo(42L);
    }

    @Test
    void testV2MetadataOmitsNextRowId() {
        String v2Json = "{\"format-version\":2,\"table-uuid\":\"uuid\",\"location\":\"loc\"}";
        IcebergMetadata metadata = IcebergMetadata.fromJson(v2Json);
        assertThat(metadata.nextRowId()).isNull();
        assertThat(metadata.toJson()).doesNotContain("next-row-id");
    }
}
