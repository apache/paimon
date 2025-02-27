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

package org.apache.paimon.iceberg.metadata;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.ObjectCodec;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.DeserializationContext;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonDeserializer;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;

/** JsonDeserializer for iceberg data types. */
public class IcebergDataTypeDeserializer extends JsonDeserializer<Object> {
    @Override
    public Object deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
        ObjectCodec mapper = p.getCodec();
        JsonNode node = mapper.readTree(p);

        if (node.isTextual()) {
            return node.asText();
        } else if (node.isObject()) {
            if (!node.has("type")) {
                throw new IOException(
                        "Exception occurs when deserialize iceberg data field. Missing 'type' field: "
                                + node);
            }

            String type = node.get("type").asText();
            switch (type) {
                case "map":
                    return mapper.treeToValue(node, IcebergMapType.class);
                case "list":
                    return mapper.treeToValue(node, IcebergListType.class);
                case "struct":
                    return mapper.treeToValue(node, IcebergStructType.class);
                default:
                    throw new IOException(
                            "Exception occurs when deserialize iceberg data field. Unknown iceberg field type: "
                                    + type);
            }
        } else {
            throw new IOException("Unexpected json node. node type: " + node.getNodeType());
        }
    }
}
