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

package org.apache.paimon.rest.serializer;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.utils.JsonDeserializer;
import org.apache.paimon.utils.JsonSerializer;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;

/** Serializer for {@link Identifier}. */
public class IdentifierSerializer
        implements JsonSerializer<Identifier>, JsonDeserializer<Identifier> {

    public static final IdentifierSerializer INSTANCE = new IdentifierSerializer();

    private static final String FIELD_DATABASE_NAME = "database";
    private static final String FIELD_TABLE_NAME = "table";
    private static final String FIELD_BRANCH_NAME = "branch";

    @Override
    public void serialize(Identifier identifier, JsonGenerator generator) throws IOException {
        generator.writeStartObject();
        generator.writeStringField(FIELD_DATABASE_NAME, identifier.getDatabaseName());
        if (identifier.getTableName() != null) {
            generator.writeStringField(FIELD_TABLE_NAME, identifier.getTableName());
        }
        if (identifier.getBranchName() != null) {
            generator.writeStringField(FIELD_BRANCH_NAME, identifier.getBranchName());
        }
        generator.writeEndObject();
    }

    @Override
    public Identifier deserialize(JsonNode node) {
        JsonNode databaseNode = node.get(FIELD_DATABASE_NAME);
        String databaseName = null;
        if (databaseNode != null) {
            databaseName = databaseNode.asText();
        }
        JsonNode tableNode = node.get(FIELD_TABLE_NAME);
        String tableName = null;
        if (tableNode != null) {
            tableName = tableNode.asText();
        }
        JsonNode branchNode = node.get(FIELD_BRANCH_NAME);
        String branchName = null;
        if (branchNode != null) {
            branchName = branchNode.asText();
        }
        return new Identifier(databaseName, tableName, branchName);
    }
}
