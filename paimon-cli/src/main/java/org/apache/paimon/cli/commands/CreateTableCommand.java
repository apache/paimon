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

package org.apache.paimon.cli.commands;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.cli.CliArgs;
import org.apache.paimon.cli.Command;
import org.apache.paimon.cli.CommandContext;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.types.DataTypes;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Creates a new table from a schema JSON file. */
public class CreateTableCommand implements Command {

    private static final Set<String> BOOLEAN_FLAGS;

    static {
        Set<String> flags = new HashSet<>();
        flags.add("ignore-if-exists");
        BOOLEAN_FLAGS = Collections.unmodifiableSet(flags);
    }

    @Override
    public String name() {
        return "create-table";
    }

    @Override
    public String description() {
        return "Create a new table from a schema JSON file";
    }

    @Override
    public void execute(CommandContext ctx, String[] args) throws Exception {
        CliArgs parsed =
                new CliArgs(args, BOOLEAN_FLAGS, "-s", "--schema", "-i", "--ignore-if-exists");
        String tableId = parsed.positional(0, "DATABASE.TABLE");
        String[] parts = SchemaCommand.parseTableIdentifier(tableId);
        String schemaFile = parsed.require("schema", "path to schema JSON file");
        boolean ignoreIfExists = "true".equals(parsed.get("ignore-if-exists"));

        String schemaJson = readFile(schemaFile);
        Schema schema = parseSchema(schemaJson);

        ctx.getCatalog().createTable(Identifier.create(parts[0], parts[1]), schema, ignoreIfExists);
        System.out.println("Table '" + tableId + "' created successfully.");
    }

    @Override
    public String usage() {
        return "Usage: paimon create-table DATABASE.TABLE --schema FILE [options]\n\n"
                + "Create a new table from a schema JSON file.\n\n"
                + "Schema JSON format:\n"
                + "  {\n"
                + "    \"fields\": [{\"name\": \"id\", \"type\": \"BIGINT\"}, ...],\n"
                + "    \"partitionKeys\": [\"dt\"],\n"
                + "    \"primaryKeys\": [\"id\"],\n"
                + "    \"options\": {\"bucket\": \"4\"}\n"
                + "  }\n\n"
                + "Options:\n"
                + "  -s, --schema FILE          Path to schema JSON file (required)\n"
                + "  -i, --ignore-if-exists     Do not raise error if table already exists";
    }

    @SuppressWarnings("unchecked")
    static Schema parseSchema(String json) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode root = mapper.readTree(json);

        // Parse fields
        JsonNode fieldsNode = root.get("fields");
        if (fieldsNode == null || !fieldsNode.isArray()) {
            throw new IllegalArgumentException("Schema must contain 'fields' array.");
        }

        Schema.Builder builder = Schema.newBuilder();
        for (JsonNode fieldNode : fieldsNode) {
            String name = fieldNode.get("name").asText();
            String type = fieldNode.get("type").asText();
            builder.column(name, DataTypes.STRING()); // placeholder, will be resolved below
        }

        // Build schema manually with proper types
        List<String> columnNames = new ArrayList<>();
        List<org.apache.paimon.types.DataType> columnTypes = new ArrayList<>();
        for (JsonNode fieldNode : fieldsNode) {
            String name = fieldNode.get("name").asText();
            String typeStr = fieldNode.get("type").asText();
            columnNames.add(name);
            columnTypes.add(parseDataType(typeStr));
        }

        // Parse partition keys
        List<String> partitionKeys = new ArrayList<>();
        JsonNode partNode = root.get("partitionKeys");
        if (partNode != null && partNode.isArray()) {
            for (JsonNode n : partNode) {
                partitionKeys.add(n.asText());
            }
        }

        // Parse primary keys
        List<String> primaryKeys = new ArrayList<>();
        JsonNode pkNode = root.get("primaryKeys");
        if (pkNode != null && pkNode.isArray()) {
            for (JsonNode n : pkNode) {
                primaryKeys.add(n.asText());
            }
        }

        // Parse options
        Map<String, String> options = new HashMap<>();
        JsonNode optNode = root.get("options");
        if (optNode != null && optNode.isObject()) {
            Iterator<Map.Entry<String, JsonNode>> it = optNode.fields();
            while (it.hasNext()) {
                Map.Entry<String, JsonNode> entry = it.next();
                options.put(entry.getKey(), entry.getValue().asText());
            }
        }

        // Parse comment
        String comment = null;
        JsonNode commentNode = root.get("comment");
        if (commentNode != null && !commentNode.isNull()) {
            comment = commentNode.asText();
        }

        // Build final schema
        Schema.Builder schemaBuilder = Schema.newBuilder();
        for (int i = 0; i < columnNames.size(); i++) {
            schemaBuilder.column(columnNames.get(i), columnTypes.get(i));
        }
        schemaBuilder.partitionKeys(partitionKeys.toArray(new String[0]));
        schemaBuilder.primaryKey(primaryKeys.toArray(new String[0]));
        schemaBuilder.options(options);
        if (comment != null) {
            schemaBuilder.comment(comment);
        }
        return schemaBuilder.build();
    }

    static org.apache.paimon.types.DataType parseDataType(String typeStr) {
        String upper = typeStr.trim().toUpperCase();
        switch (upper) {
            case "BOOLEAN":
                return DataTypes.BOOLEAN();
            case "TINYINT":
                return DataTypes.TINYINT();
            case "SMALLINT":
                return DataTypes.SMALLINT();
            case "INT":
            case "INTEGER":
                return DataTypes.INT();
            case "BIGINT":
                return DataTypes.BIGINT();
            case "FLOAT":
                return DataTypes.FLOAT();
            case "DOUBLE":
                return DataTypes.DOUBLE();
            case "STRING":
                return DataTypes.STRING();
            case "DATE":
                return DataTypes.DATE();
            case "TIMESTAMP":
                return DataTypes.TIMESTAMP(6);
            case "BINARY":
                return DataTypes.BINARY(1);
            case "VARBINARY":
                return DataTypes.VARBINARY(Integer.MAX_VALUE);
            case "BYTES":
                return DataTypes.BYTES();
            default:
                break;
        }
        // DECIMAL(p, s)
        if (upper.startsWith("DECIMAL")) {
            String params = upper.substring("DECIMAL".length()).trim();
            if (params.startsWith("(") && params.endsWith(")")) {
                String[] ps = params.substring(1, params.length() - 1).split(",");
                int precision = Integer.parseInt(ps[0].trim());
                int scale = ps.length > 1 ? Integer.parseInt(ps[1].trim()) : 0;
                return DataTypes.DECIMAL(precision, scale);
            }
            return DataTypes.DECIMAL(38, 18);
        }
        // VARCHAR(n)
        if (upper.startsWith("VARCHAR")) {
            String params = upper.substring("VARCHAR".length()).trim();
            if (params.startsWith("(") && params.endsWith(")")) {
                int length = Integer.parseInt(params.substring(1, params.length() - 1).trim());
                return DataTypes.VARCHAR(length);
            }
            return DataTypes.STRING();
        }
        // CHAR(n)
        if (upper.startsWith("CHAR")) {
            String params = upper.substring("CHAR".length()).trim();
            if (params.startsWith("(") && params.endsWith(")")) {
                int length = Integer.parseInt(params.substring(1, params.length() - 1).trim());
                return DataTypes.CHAR(length);
            }
            return DataTypes.CHAR(1);
        }
        // TIMESTAMP(p)
        if (upper.startsWith("TIMESTAMP")) {
            String params = upper.substring("TIMESTAMP".length()).trim();
            if (params.startsWith("(") && params.endsWith(")")) {
                int precision = Integer.parseInt(params.substring(1, params.length() - 1).trim());
                return DataTypes.TIMESTAMP(precision);
            }
            return DataTypes.TIMESTAMP(6);
        }
        return DataTypes.STRING();
    }

    private static String readFile(String path) throws IOException {
        return new String(Files.readAllBytes(Paths.get(path)), StandardCharsets.UTF_8);
    }
}
