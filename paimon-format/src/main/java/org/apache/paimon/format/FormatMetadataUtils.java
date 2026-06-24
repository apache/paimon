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

import org.apache.paimon.arrow.ArrowUtils;
import org.apache.paimon.types.RowType;

import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import javax.annotation.Nullable;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/** Utilities for format metadata encoded at file boundaries. */
public class FormatMetadataUtils {

    public static final String ARROW_SCHEMA_METADATA_KEY = "ARROW:schema";

    private FormatMetadataUtils() {}

    public static Map<String, String> encodeMetadata(Map<String, byte[]> metadata) {
        Map<String, String> encoded = new LinkedHashMap<>();
        for (Map.Entry<String, byte[]> entry : metadata.entrySet()) {
            encoded.put(entry.getKey(), Base64.getEncoder().encodeToString(entry.getValue()));
        }
        return encoded;
    }

    /**
     * Decodes base64-encoded metadata values. Values that are not valid base64 are returned as
     * UTF-8 bytes.
     */
    public static Map<String, byte[]> decodeMetadata(Map<String, String> metadata) {
        Map<String, byte[]> decoded = new LinkedHashMap<>();
        for (Map.Entry<String, String> entry : metadata.entrySet()) {
            try {
                decoded.put(entry.getKey(), Base64.getDecoder().decode(entry.getValue()));
            } catch (IllegalArgumentException e) {
                decoded.put(entry.getKey(), entry.getValue().getBytes(StandardCharsets.UTF_8));
            }
        }
        return decoded;
    }

    public static Optional<Schema> readArrowSchema(@Nullable String encodedSchema) {
        if (encodedSchema == null) {
            return Optional.empty();
        }
        try {
            byte[] schemaBytes = Base64.getDecoder().decode(encodedSchema);
            return Optional.of(Schema.deserializeMessage(ByteBuffer.wrap(schemaBytes)));
        } catch (RuntimeException e) {
            return Optional.empty();
        }
    }

    public static byte[] serializeArrowSchema(Schema arrowSchema) {
        return arrowSchema.serializeAsMessage();
    }

    /**
     * Builds an Arrow schema from a Paimon row type and injects metadata into top-level fields.
     *
     * <p>The keys of {@code fieldMetadata} are top-level field names. Nested fields are converted
     * from the {@link RowType} but do not receive metadata from this map. If injected metadata
     * conflicts with metadata produced during Arrow conversion, the Arrow conversion metadata wins
     * to preserve format-specific field information.
     */
    public static Schema buildArrowSchema(
            RowType rowType, Map<String, Map<String, String>> fieldMetadata) {
        List<Field> fields =
                rowType.getFields().stream()
                        .map(
                                field ->
                                        withMetadata(
                                                ArrowUtils.toArrowField(
                                                        field.name(), field.id(), field.type(), 0),
                                                fieldMetadata.get(field.name())))
                        .collect(Collectors.toList());
        return new Schema(fields);
    }

    /** Returns metadata for top-level Arrow fields only. */
    public static Map<String, Map<String, String>> readFieldMetadata(Schema arrowSchema) {
        Map<String, Map<String, String>> result = new LinkedHashMap<>();
        for (Field field : arrowSchema.getFields()) {
            result.put(
                    field.getName(),
                    Collections.unmodifiableMap(new LinkedHashMap<>(field.getMetadata())));
        }
        return Collections.unmodifiableMap(result);
    }

    private static Field withMetadata(Field field, @Nullable Map<String, String> metadata) {
        if (metadata == null || metadata.isEmpty()) {
            return field;
        }
        FieldType fieldType = field.getFieldType();
        Map<String, String> result = new LinkedHashMap<>();
        result.putAll(metadata);
        result.putAll(fieldType.getMetadata());
        return new Field(
                field.getName(),
                new FieldType(
                        fieldType.isNullable(),
                        fieldType.getType(),
                        fieldType.getDictionary(),
                        result),
                field.getChildren());
    }
}
