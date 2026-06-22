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

import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

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

    public static Map<String, byte[]> decodeMetadata(Map<String, String> metadata) {
        Map<String, byte[]> decoded = new LinkedHashMap<>();
        for (Map.Entry<String, String> entry : metadata.entrySet()) {
            decoded.put(entry.getKey(), Base64.getDecoder().decode(entry.getValue()));
        }
        return decoded;
    }

    public static Optional<Schema> readArrowSchema(String encodedSchema) {
        if (encodedSchema == null) {
            return Optional.empty();
        }
        byte[] schemaBytes = Base64.getDecoder().decode(encodedSchema);
        return Optional.of(Schema.deserializeMessage(ByteBuffer.wrap(schemaBytes)));
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
}
