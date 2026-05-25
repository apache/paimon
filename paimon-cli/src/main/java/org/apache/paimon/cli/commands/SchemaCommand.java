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
import org.apache.paimon.table.Table;
import org.apache.paimon.types.DataField;

import java.util.List;
import java.util.Map;

/** Displays table schema information. */
public class SchemaCommand implements Command {

    @Override
    public String name() {
        return "schema";
    }

    @Override
    public String description() {
        return "Show table schema information";
    }

    @Override
    public void execute(CommandContext ctx, String[] args) throws Exception {
        CliArgs parsed = new CliArgs(args, "-f", "--format");
        String tableId = parsed.positional(0, "DATABASE.TABLE");
        String[] parts = parseTableIdentifier(tableId);
        String format = parsed.getOrDefault("format", "table");

        Table table = ctx.getCatalog().getTable(Identifier.create(parts[0], parts[1]));
        List<DataField> fields = table.rowType().getFields();
        List<String> primaryKeys = table.primaryKeys();
        List<String> partitionKeys = table.partitionKeys();

        if ("json".equals(format)) {
            printJson(table, fields, primaryKeys, partitionKeys);
        } else {
            printTable(tableId, table, fields, primaryKeys, partitionKeys);
        }
    }

    private void printJson(
            Table table,
            List<DataField> fields,
            List<String> primaryKeys,
            List<String> partitionKeys) {
        StringBuilder sb = new StringBuilder();
        sb.append("{\"fields\":[");
        for (int i = 0; i < fields.size(); i++) {
            if (i > 0) {
                sb.append(",");
            }
            DataField f = fields.get(i);
            sb.append("{\"id\":").append(f.id());
            sb.append(",\"name\":\"").append(f.name()).append("\"");
            sb.append(",\"type\":\"").append(f.type().toString()).append("\"}");
        }
        sb.append("],\"partitionKeys\":[");
        for (int i = 0; i < partitionKeys.size(); i++) {
            if (i > 0) {
                sb.append(",");
            }
            sb.append("\"").append(partitionKeys.get(i)).append("\"");
        }
        sb.append("],\"primaryKeys\":[");
        for (int i = 0; i < primaryKeys.size(); i++) {
            if (i > 0) {
                sb.append(",");
            }
            sb.append("\"").append(primaryKeys.get(i)).append("\"");
        }
        sb.append("],\"options\":{");
        Map<String, String> options = table.options();
        if (options != null) {
            int idx = 0;
            for (Map.Entry<String, String> entry : options.entrySet()) {
                if (idx > 0) {
                    sb.append(",");
                }
                sb.append("\"").append(escapeJson(entry.getKey())).append("\":");
                sb.append("\"").append(escapeJson(entry.getValue())).append("\"");
                idx++;
            }
        }
        sb.append("}");
        String comment = table.comment().orElse(null);
        if (comment != null) {
            sb.append(",\"comment\":\"").append(escapeJson(comment)).append("\"");
        }
        sb.append("}");
        System.out.println(sb.toString());
    }

    private void printTable(
            String tableId,
            Table table,
            List<DataField> fields,
            List<String> primaryKeys,
            List<String> partitionKeys) {
        System.out.println("Table: " + tableId);
        System.out.println();
        System.out.println(String.format("  %-30s %-35s %-8s", "Column", "Type", "Nullable"));
        System.out.println(
                String.format(
                        "  %-30s %-35s %-8s", repeat("-", 30), repeat("-", 35), repeat("-", 8)));
        for (DataField field : fields) {
            String nullable = field.type().isNullable() ? "YES" : "NO";
            System.out.println(
                    String.format(
                            "  %-30s %-35s %-8s", field.name(), field.type().toString(), nullable));
        }

        if (!primaryKeys.isEmpty()) {
            System.out.println();
            System.out.println("Primary keys: " + String.join(", ", primaryKeys));
        }
        if (!partitionKeys.isEmpty()) {
            System.out.println("Partition keys: " + String.join(", ", partitionKeys));
        }

        Map<String, String> options = table.options();
        if (options != null && !options.isEmpty()) {
            System.out.println();
            System.out.println("Options:");
            for (Map.Entry<String, String> entry : options.entrySet()) {
                System.out.println("  " + entry.getKey() + " = " + entry.getValue());
            }
        }
    }

    @Override
    public String usage() {
        return "Usage: paimon schema DATABASE.TABLE [options]\n\n"
                + "Show table schema, primary keys, partition keys, and options.\n\n"
                + "Options:\n"
                + "  -f, --format   Output format: table (default) / json";
    }

    static String[] parseTableIdentifier(String tableId) {
        String[] parts = tableId.split("\\.", 2);
        if (parts.length != 2 || parts[0].isEmpty() || parts[1].isEmpty()) {
            System.err.println(
                    "Invalid table identifier '" + tableId + "'. Expected format: database.table");
            System.exit(1);
        }
        return parts;
    }

    private static String repeat(String str, int count) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < count; i++) {
            sb.append(str);
        }
        return sb.toString();
    }

    private static String escapeJson(String value) {
        return value.replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\n", "\\n")
                .replace("\r", "\\r")
                .replace("\t", "\\t");
    }
}
