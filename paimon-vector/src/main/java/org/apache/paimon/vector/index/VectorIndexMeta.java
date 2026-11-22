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

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

/** JSON metadata for a range-sharded vector index file. */
public class VectorIndexMeta implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final TypeReference<LinkedHashMap<String, String>> MAP_TYPE_REF =
            new TypeReference<LinkedHashMap<String, String>>() {};

    @Nullable private final Integer localNlist;
    @Nullable private final String metric;

    VectorIndexMeta() {
        this(null, null);
    }

    VectorIndexMeta(@Nullable Integer localNlist, @Nullable String metric) {
        this.localNlist = localNlist;
        this.metric = metric;
    }

    public byte[] serialize() throws IOException {
        Map<String, String> values = new LinkedHashMap<>();
        if (localNlist != null) {
            values.put("localNlist", localNlist.toString());
        }
        if (metric != null) {
            values.put("metric", metric);
        }
        return OBJECT_MAPPER.writeValueAsBytes(values);
    }

    public static VectorIndexMeta deserialize(byte[] data) throws IOException {
        Map<String, String> values = OBJECT_MAPPER.readValue(data, MAP_TYPE_REF);
        String nlist = values.get("localNlist");
        return new VectorIndexMeta(
                nlist == null ? null : Integer.valueOf(nlist), values.get("metric"));
    }
}
