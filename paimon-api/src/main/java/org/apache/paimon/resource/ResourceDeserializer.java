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

package org.apache.paimon.resource;

import org.apache.paimon.catalog.Identifier;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.DeserializationContext;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.deser.std.StdDeserializer;

import java.io.IOException;

/** Jackson deserializer for {@link Resource} interface. */
public class ResourceDeserializer extends StdDeserializer<Resource> {

    private static final long serialVersionUID = 1L;

    public ResourceDeserializer() {
        super(Resource.class);
    }

    @Override
    public Resource deserialize(JsonParser parser, DeserializationContext context)
            throws IOException {
        JsonNode node = parser.getCodec().readTree(parser);
        String name = node.has("name") ? node.get("name").asText() : "";
        String fullName = node.has("fullName") ? node.get("fullName").asText() : name;
        String uri = node.has("uri") ? node.get("uri").asText() : "";
        long size = node.has("size") ? node.get("size").asLong() : 0;
        long lastModifiedTime =
                node.has("lastModifiedTime") ? node.get("lastModifiedTime").asLong() : 0;
        String resourceType = node.has("resourceType") ? node.get("resourceType").asText() : "file";
        String comment =
                node.has("comment") && !node.get("comment").isNull()
                        ? node.get("comment").asText()
                        : null;

        Identifier identifier;
        if (fullName.contains(".")) {
            identifier = Identifier.fromString(fullName);
        } else {
            identifier = Identifier.create("default", name);
        }

        return Resource.toResource(
                ResourceType.fromValue(resourceType),
                identifier,
                comment,
                uri,
                size,
                lastModifiedTime);
    }
}
