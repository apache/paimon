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

package org.apache.paimon.arrow;

import org.apache.paimon.format.FormatMetadataUtils;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Compatibility tests between Arrow Java schema serialization and format metadata utilities. */
public class ArrowSchemaMetadataCompatibilityTest {

    @Test
    public void testFormatMetadataCanBeReadByArrowJava() {
        RowType rowType = rowType();
        Map<String, String> tagsMetadata = tagsMetadata();
        Map<String, Map<String, String>> fieldMetadata = new LinkedHashMap<>();
        fieldMetadata.put("tags", tagsMetadata);

        byte[] schemaBytes =
                FormatMetadataUtils.buildArrowSchemaMetadata(
                        rowType, fieldMetadata, FormatMetadataUtils.PARQUET_FIELD_ID_KEY);
        Schema schema = Schema.deserializeMessage(ByteBuffer.wrap(schemaBytes));

        assertThat(schema.getFields()).extracting(Field::getName).containsExactly("id", "tags");
        assertThat(schema.findField("id").getMetadata())
                .containsEntry(ArrowUtils.PARQUET_FIELD_ID, "0");
        assertThat(schema.findField("tags").getMetadata())
                .containsEntry(ArrowUtils.PARQUET_FIELD_ID, "1")
                .containsAllEntriesOf(tagsMetadata);
    }

    @Test
    public void testArrowJavaSchemaCanBeReadByFormatMetadata() {
        RowType rowType = rowType();
        Map<String, String> tagsMetadata = tagsMetadata();
        List<Field> fields = new ArrayList<>();
        for (DataField field : rowType.getFields()) {
            Field arrowField = ArrowUtils.toArrowField(field.name(), field.id(), field.type(), 0);
            if ("tags".equals(field.name())) {
                arrowField = withMetadata(arrowField, tagsMetadata);
            }
            fields.add(arrowField);
        }
        byte[] schemaBytes = new Schema(fields).serializeAsMessage();

        Map<String, Map<String, String>> metadata =
                FormatMetadataUtils.readFieldMetadata(schemaBytes);

        assertThat(metadata).containsOnlyKeys("id", "tags");
        assertThat(metadata.get("id")).containsEntry(ArrowUtils.PARQUET_FIELD_ID, "0");
        assertThat(metadata.get("tags"))
                .containsEntry(ArrowUtils.PARQUET_FIELD_ID, "1")
                .containsAllEntriesOf(tagsMetadata);
    }

    private static RowType rowType() {
        return DataTypes.ROW(
                DataTypes.FIELD(0, "id", DataTypes.INT()),
                DataTypes.FIELD(1, "tags", DataTypes.MAP(DataTypes.STRING(), DataTypes.INT())));
    }

    private static Map<String, String> tagsMetadata() {
        Map<String, String> metadata = new LinkedHashMap<>();
        metadata.put("paimon.test.tags", "enabled");
        metadata.put("paimon.test.version", "1");
        return metadata;
    }

    private static Field withMetadata(Field field, Map<String, String> metadata) {
        Map<String, String> merged = new LinkedHashMap<>();
        merged.putAll(metadata);
        merged.putAll(field.getMetadata());
        FieldType fieldType = field.getFieldType();
        return new Field(
                field.getName(),
                new FieldType(
                        fieldType.isNullable(),
                        fieldType.getType(),
                        fieldType.getDictionary(),
                        merged),
                field.getChildren());
    }
}
