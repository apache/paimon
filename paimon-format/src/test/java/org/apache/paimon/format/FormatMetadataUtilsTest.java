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

import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
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
    public void testReadArrowSchema() {
        Map<String, String> fieldMetadata = new LinkedHashMap<>();
        fieldMetadata.put("paimon.test.field-key", "field-value");
        Schema schema =
                new Schema(
                        Collections.singletonList(
                                new Field(
                                        "field",
                                        new FieldType(
                                                true,
                                                Types.MinorType.VARCHAR.getType(),
                                                null,
                                                fieldMetadata),
                                        null)));
        String encodedSchema =
                Base64.getEncoder()
                        .encodeToString(FormatMetadataUtils.serializeArrowSchema(schema));

        assertThat(FormatMetadataUtils.readArrowSchema(encodedSchema)).hasValue(schema);
        assertThat(FormatMetadataUtils.readArrowSchema(null)).isEmpty();
    }

    @Test
    public void testReadFieldMetadata() {
        assertThat(FormatMetadataUtils.readFieldMetadata(new Schema(Collections.emptyList())))
                .isEmpty();

        Map<String, String> fieldMetadata = new LinkedHashMap<>();
        fieldMetadata.put("paimon.test.field-key", "field-value");
        Schema schema =
                new Schema(
                        Arrays.asList(
                                new Field(
                                        "with_metadata",
                                        new FieldType(
                                                true,
                                                Types.MinorType.INT.getType(),
                                                null,
                                                fieldMetadata),
                                        null),
                                new Field(
                                        "without_metadata",
                                        new FieldType(
                                                true, Types.MinorType.INT.getType(), null, null),
                                        null)));

        assertThat(FormatMetadataUtils.readFieldMetadata(schema))
                .containsEntry("with_metadata", fieldMetadata)
                .containsEntry("without_metadata", Collections.emptyMap());
    }

    @Test
    public void testBuildArrowSchemaWithFieldMetadata() {
        RowType rowType =
                DataTypes.ROW(
                        DataTypes.FIELD(0, "id", DataTypes.INT()),
                        DataTypes.FIELD(
                                1,
                                "tags",
                                DataTypes.MAP(DataTypes.STRING(), DataTypes.INT())),
                        DataTypes.FIELD(
                                2,
                                "nested",
                                DataTypes.ROW(
                                        DataTypes.FIELD(3, "name", DataTypes.STRING()),
                                        DataTypes.FIELD(
                                                4,
                                                "scores",
                                                DataTypes.ARRAY(DataTypes.INT())))));
        Map<String, String> tagsMetadata = new LinkedHashMap<>();
        tagsMetadata.put("paimon.test.tags", "enabled");

        Map<String, Map<String, String>> fieldMetadata = new LinkedHashMap<>();
        fieldMetadata.put("tags", tagsMetadata);

        Schema schema = FormatMetadataUtils.buildArrowSchema(rowType, fieldMetadata);

        assertThat(schema.getFields())
                .extracting(Field::getName)
                .containsExactly("id", "tags", "nested");
        assertThat(schema.findField("tags").getMetadata())
                .containsEntry("paimon.test.tags", "enabled");
        assertThat(schema.findField("nested").getMetadata())
                .doesNotContainKey("paimon.test.tags");
    }
}
