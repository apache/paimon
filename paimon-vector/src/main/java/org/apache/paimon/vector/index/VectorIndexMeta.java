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

package org.apache.paimon.vector.index;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Metadata for a vector index file.
 *
 * <p>Serialized as an empty JSON {@code Map<String, String>}. Search-time parameters are passed
 * through {@link org.apache.paimon.predicate.VectorSearch#options()}.
 */
public class VectorIndexMeta implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final TypeReference<LinkedHashMap<String, String>> MAP_TYPE_REF =
            new TypeReference<LinkedHashMap<String, String>>() {};

    VectorIndexMeta() {}

    public byte[] serialize() throws IOException {
        return OBJECT_MAPPER.writeValueAsBytes(Collections.<String, String>emptyMap());
    }

    public static VectorIndexMeta deserialize(byte[] data) throws IOException {
        Map<String, String> ignored = OBJECT_MAPPER.readValue(data, MAP_TYPE_REF);
        return new VectorIndexMeta();
    }
}
