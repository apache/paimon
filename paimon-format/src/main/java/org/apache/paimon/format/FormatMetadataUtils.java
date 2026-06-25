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

import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/** Utilities for format metadata encoded at file boundaries. */
public class FormatMetadataUtils {

    public static final String ARROW_SCHEMA_METADATA_KEY = "ARROW:schema";
    public static final String PARQUET_FIELD_ID_KEY = "PARQUET:field_id";
    public static final String ORC_FIELD_ID_KEY = "paimon.id";

    private FormatMetadataUtils() {}

    /**
     * Encodes raw metadata values as base64 strings so they can be stored in format key-value
     * metadata.
     */
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

    /**
     * Builds serialized Arrow schema metadata from a Paimon row type and injects metadata into
     * top-level fields.
     *
     * <p>The keys of {@code fieldMetadata} are top-level field names. Nested fields are converted
     * from the {@link RowType} but do not receive metadata from this map. If injected metadata
     * conflicts with metadata produced during Arrow conversion, the Arrow conversion metadata wins
     * to preserve format-specific field information. Set {@code fieldIdKey} to the field id
     * metadata key used by the target format, or {@code null} if field id metadata should not be
     * written.
     */
    public static byte[] buildArrowSchemaMetadata(
            RowType rowType,
            Map<String, Map<String, String>> fieldMetadata,
            @Nullable String fieldIdKey) {
        return ArrowSchemaMetadata.serialize(rowType, fieldMetadata, fieldIdKey);
    }

    /**
     * Reads field metadata from serialized Arrow schema metadata.
     *
     * <p>The returned map contains top-level fields only, keyed by field name. If the input is
     * {@code null} or cannot be parsed as an Arrow schema message, this method returns an empty
     * map. Fields without any metadata are omitted from the returned map.
     */
    public static Map<String, Map<String, String>> readFieldMetadata(@Nullable byte[] schemaBytes) {
        if (schemaBytes == null) {
            return Collections.emptyMap();
        }
        try {
            return ArrowSchemaMetadata.readFieldMetadata(schemaBytes);
        } catch (RuntimeException e) {
            return Collections.emptyMap();
        }
    }
}
