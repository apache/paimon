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

package org.apache.paimon.data.variant;

import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import java.time.ZoneId;

import static org.apache.paimon.data.variant.VariantMetadataUtils.VariantRowTypeBuilder;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link VariantMetadataUtils}. */
public class VariantMetadataUtilsTest {

    @Test
    public void testBuildVariantMetadata() {
        String metadata = VariantMetadataUtils.buildVariantMetadata("$.a.b", true, "UTC");
        assertThat(metadata).isEqualTo("__VARIANT_METADATA$.a.b;true;UTC");
    }

    @Test
    public void testIsVariantRow() {
        // Create a row type with variant metadata
        DataField field1 =
                new DataField(
                        0,
                        "age",
                        DataTypes.INT(),
                        VariantMetadataUtils.buildVariantMetadata("$.age"));
        DataField field2 =
                new DataField(
                        1,
                        "name",
                        DataTypes.STRING(),
                        VariantMetadataUtils.buildVariantMetadata("$.name"));
        RowType variantRowType = RowType.of(field1, field2);

        assertThat(VariantMetadataUtils.isVariantRowType(variantRowType)).isTrue();

        // Create a normal row type
        DataField normalField1 = new DataField(0, "age", DataTypes.INT());
        DataField normalField2 = new DataField(1, "name", DataTypes.STRING());
        RowType normalRowType = RowType.of(normalField1, normalField2);

        assertThat(VariantMetadataUtils.isVariantRowType(normalRowType)).isFalse();

        // Mixed row type (one field with metadata, one without)
        RowType mixedRowType = RowType.of(field1, normalField2);
        assertThat(VariantMetadataUtils.isVariantRowType(mixedRowType)).isFalse();

        // Empty row type
        RowType emptyRowType = RowType.of();
        assertThat(VariantMetadataUtils.isVariantRowType(emptyRowType)).isFalse();
    }

    @Test
    public void testExtractPath() {
        String description = VariantMetadataUtils.buildVariantMetadata("$.a.b", true, "UTC");
        assertThat(VariantMetadataUtils.path(description)).isEqualTo("$.a.b");
    }

    @Test
    public void testExtractFailOnError() {
        String description1 = VariantMetadataUtils.buildVariantMetadata("$.field", true, "UTC");
        assertThat(VariantMetadataUtils.failOnError(description1)).isTrue();

        String description2 = VariantMetadataUtils.buildVariantMetadata("$.field", false, "UTC");
        assertThat(VariantMetadataUtils.failOnError(description2)).isFalse();
    }

    @Test
    public void testExtractTimeZoneId() {
        String description1 =
                VariantMetadataUtils.buildVariantMetadata("$.field", true, "Asia/Shanghai");
        assertThat(VariantMetadataUtils.timeZoneId(description1))
                .isEqualTo(ZoneId.of("Asia/Shanghai"));

        String description2 = VariantMetadataUtils.buildVariantMetadata("$.field", false, "UTC");
        assertThat(VariantMetadataUtils.timeZoneId(description2)).isEqualTo(ZoneId.of("UTC"));
    }

    @Test
    public void testCreateVariantRowType() {
        RowType rowType =
                VariantRowTypeBuilder.builder(false)
                        .field(DataTypes.INT(), "$.age", false, "UTC")
                        .field(DataTypes.STRING(), "$.name", false, "UTC")
                        .build();

        assertThat(VariantMetadataUtils.isVariantRowType(rowType)).isTrue();
        assertThat(rowType.getFieldCount()).isEqualTo(2);

        DataField field0 = rowType.getField(0);
        assertThat(VariantMetadataUtils.path(field0.description())).isEqualTo("$.age");

        DataField field1 = rowType.getField(1);
        assertThat(VariantMetadataUtils.path(field1.description())).isEqualTo("$.name");
    }

    @Test
    public void testExtractVariantFields() {
        // Create a variant row type using VariantRowTypeBuilder
        RowType variantRowType =
                VariantRowTypeBuilder.builder(false)
                        .field(DataTypes.INT(), "$.age", true, "Asia/Shanghai")
                        .field(DataTypes.STRING(), "$.name", false, "UTC")
                        .build();

        // Extract back - now directly check fields from RowType
        assertThat(VariantMetadataUtils.isVariantRowType(variantRowType)).isTrue();
        assertThat(variantRowType.getFieldCount()).isEqualTo(2);

        DataField field0 = variantRowType.getField(0);
        assertThat(VariantMetadataUtils.path(field0.description())).isEqualTo("$.age");
        assertThat(VariantMetadataUtils.failOnError(field0.description())).isTrue();
        assertThat(VariantMetadataUtils.timeZoneId(field0.description()))
                .isEqualTo(ZoneId.of("Asia/Shanghai"));

        DataField field1 = variantRowType.getField(1);
        assertThat(VariantMetadataUtils.path(field1.description())).isEqualTo("$.name");
        assertThat(VariantMetadataUtils.failOnError(field1.description())).isFalse();
        assertThat(VariantMetadataUtils.timeZoneId(field1.description()))
                .isEqualTo(ZoneId.of("UTC"));
    }

    @Test
    public void testDifferentTimeZones() {
        String[] timeZones = {"UTC", "Asia/Shanghai", "America/New_York", "Europe/London"};

        for (String tz : timeZones) {
            String metadata = VariantMetadataUtils.buildVariantMetadata("$.field", true, tz);
            assertThat(VariantMetadataUtils.timeZoneId(metadata)).isEqualTo(ZoneId.of(tz));
        }
    }
}
