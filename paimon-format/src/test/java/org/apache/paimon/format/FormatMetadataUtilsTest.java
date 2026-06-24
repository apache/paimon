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

package org.apache.paimon.format;

import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link FormatMetadataUtils}. */
public class FormatMetadataUtilsTest {

    @Test
    public void testEncodeAndDecodeMetadata() {
        Map<String, byte[]> metadata = new LinkedHashMap<>();
        metadata.put("encoded", "paimon-value".getBytes(StandardCharsets.UTF_8));

        Map<String, String> encoded = FormatMetadataUtils.encodeMetadata(metadata);
        encoded.put("plain", "plain-value");
        assertThat(encoded)
                .containsEntry(
                        "encoded",
                        Base64.getEncoder()
                                .encodeToString("paimon-value".getBytes(StandardCharsets.UTF_8)));

        Map<String, byte[]> decoded = FormatMetadataUtils.decodeMetadata(encoded);

        assertThat(new String(decoded.get("encoded"), StandardCharsets.UTF_8))
                .isEqualTo("paimon-value");
        assertThat(new String(decoded.get("plain"), StandardCharsets.UTF_8))
                .isEqualTo("plain-value");
    }

    @Test
    public void testReadFieldMetadataFromArrowSchemaMetadata() {
        RowType rowType = DataTypes.ROW(DataTypes.FIELD(0, "field", DataTypes.STRING()));
        Map<String, String> fieldMetadata = new LinkedHashMap<>();
        fieldMetadata.put("paimon.test.field-key", "field-value");
        Map<String, Map<String, String>> expected = new LinkedHashMap<>();
        expected.put("field", fieldMetadata);
        String encodedSchema =
                Base64.getEncoder()
                        .encodeToString(
                                FormatMetadataUtils.buildArrowSchemaMetadata(rowType, expected));

        assertThat(FormatMetadataUtils.readFieldMetadata(encodedSchema).get("field"))
                .containsAllEntriesOf(fieldMetadata);
        assertThat(FormatMetadataUtils.readFieldMetadata(null)).isEmpty();
        assertThat(FormatMetadataUtils.readFieldMetadata("not-base64")).isEmpty();
        assertThat(
                        FormatMetadataUtils.readFieldMetadata(
                                Base64.getEncoder()
                                        .encodeToString(
                                                "not-arrow-schema"
                                                        .getBytes(StandardCharsets.UTF_8))))
                .isEmpty();
    }

    @Test
    public void testReadFieldMetadata() {
        RowType rowType =
                DataTypes.ROW(
                        DataTypes.FIELD(0, "with_metadata", DataTypes.INT()),
                        DataTypes.FIELD(1, "without_metadata", DataTypes.INT()));
        Map<String, String> fieldMetadata = new LinkedHashMap<>();
        fieldMetadata.put("paimon.test.field-key", "field-value");
        Map<String, Map<String, String>> expected = new LinkedHashMap<>();
        expected.put("with_metadata", fieldMetadata);
        String encodedSchema =
                Base64.getEncoder()
                        .encodeToString(
                                FormatMetadataUtils.buildArrowSchemaMetadata(rowType, expected));

        Map<String, Map<String, String>> metadata =
                FormatMetadataUtils.readFieldMetadata(encodedSchema);
        assertThat(metadata.get("with_metadata")).containsAllEntriesOf(fieldMetadata);
        assertThat(metadata).containsKey("without_metadata");
    }

    @Test
    public void testBuildArrowSchemaWithFieldMetadata() {
        RowType rowType =
                DataTypes.ROW(
                        DataTypes.FIELD(0, "id", DataTypes.INT()),
                        DataTypes.FIELD(
                                1, "tags", DataTypes.MAP(DataTypes.STRING(), DataTypes.INT())),
                        DataTypes.FIELD(
                                2,
                                "nested",
                                DataTypes.ROW(
                                        DataTypes.FIELD(3, "name", DataTypes.STRING()),
                                        DataTypes.FIELD(
                                                4, "scores", DataTypes.ARRAY(DataTypes.INT())))));
        Map<String, String> tagsMetadata = new LinkedHashMap<>();
        tagsMetadata.put("paimon.test.tags", "enabled");

        Map<String, Map<String, String>> fieldMetadata = new LinkedHashMap<>();
        fieldMetadata.put("tags", tagsMetadata);

        String encodedSchema =
                Base64.getEncoder()
                        .encodeToString(
                                FormatMetadataUtils.buildArrowSchemaMetadata(
                                        rowType, fieldMetadata));

        Map<String, Map<String, String>> metadata =
                FormatMetadataUtils.readFieldMetadata(encodedSchema);
        assertThat(metadata).containsOnlyKeys("id", "tags", "nested");
        assertThat(metadata.get("tags")).containsAllEntriesOf(tagsMetadata);
        assertThat(metadata.get("nested")).doesNotContainKey("paimon.test.tags");
    }
}
