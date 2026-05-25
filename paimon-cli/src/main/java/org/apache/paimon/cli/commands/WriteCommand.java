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
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeChecks;
import org.apache.paimon.types.DataTypeRoot;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Writes data from a local file into a Paimon table. */
public class WriteCommand implements Command {

    private static final Set<String> BOOLEAN_FLAGS;

    static {
        Set<String> flags = new HashSet<>();
        BOOLEAN_FLAGS = Collections.unmodifiableSet(flags);
    }

    @Override
    public String name() {
        return "write";
    }

    @Override
    public String description() {
        return "Write data from a local file into a Paimon table";
    }

    @Override
    public void execute(CommandContext ctx, String[] args) throws Exception {
        CliArgs parsed =
                new CliArgs(
                        args,
                        BOOLEAN_FLAGS,
                        "-f",
                        "--format",
                        "-d",
                        "--delimiter",
                        "-e",
                        "--encoding");

        String filePath = parsed.positional(0, "FILE");
        String tableId = parsed.positional(1, "DATABASE.TABLE");
        String[] parts = SchemaCommand.parseTableIdentifier(tableId);

        String format = parsed.getOrDefault("format", "csv");
        String delimiter = parsed.getOrDefault("delimiter", ",");
        String encoding = parsed.getOrDefault("encoding", "utf-8");

        Table table = ctx.getCatalog().getTable(Identifier.create(parts[0], parts[1]));
        List<DataField> fields = table.rowType().getFields();

        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        BatchTableWrite writer = writeBuilder.newWrite();

        int rowCount;
        if ("json".equals(format)) {
            rowCount = loadJson(filePath, encoding, fields, writer);
        } else {
            rowCount = loadCsv(filePath, encoding, delimiter, fields, writer);
        }

        if (rowCount == 0) {
            System.err.println("No data to write.");
            writer.close();
            return;
        }

        List<CommitMessage> messages = writer.prepareCommit();
        writer.close();

        System.err.println("Committing " + rowCount + " rows...");
        writeBuilder.newCommit().commit(messages);
        System.out.println("Successfully wrote " + rowCount + " rows into '" + tableId + "'.");
    }

    @Override
    public String usage() {
        return "Usage: paimon write FILE DATABASE.TABLE [options]\n\n"
                + "Write data from a local file into a Paimon table.\n\n"
                + "Options:\n"
                + "  -f, --format     Input format: csv (default) / json\n"
                + "  -d, --delimiter  CSV delimiter (default: ,)\n"
                + "  -e, --encoding   File encoding (default: utf-8)";
    }

    private static int loadCsv(
            String filePath,
            String encoding,
            String delimiter,
            List<DataField> fields,
            BatchTableWrite writer)
            throws Exception {
        Charset charset = Charset.forName(encoding);
        int rowCount = 0;
        try (BufferedReader reader =
                new BufferedReader(new InputStreamReader(new FileInputStream(filePath), charset))) {
            String line;
            boolean firstLine = true;
            while ((line = reader.readLine()) != null) {
                while (hasUnclosedQuote(line)) {
                    String nextLine = reader.readLine();
                    if (nextLine == null) {
                        break;
                    }
                    line = line + "\n" + nextLine;
                }
                if (line.trim().isEmpty()) {
                    continue;
                }
                String[] values =
                        ",".equals(delimiter) ? parseCsvLine(line) : line.split(delimiter, -1);
                if (firstLine) {
                    firstLine = false;
                    if (values.length == fields.size() && isHeaderRow(values, fields)) {
                        continue;
                    }
                }
                if (values.length != fields.size()) {
                    System.err.println(
                            "Skipping row (column count mismatch, expected "
                                    + fields.size()
                                    + ", got "
                                    + values.length
                                    + ")");
                    continue;
                }
                GenericRow row = parseRow(values, fields);
                writer.write(row);
                rowCount++;
            }
        }
        return rowCount;
    }

    private static int loadJson(
            String filePath, String encoding, List<DataField> fields, BatchTableWrite writer)
            throws Exception {
        Charset charset = Charset.forName(encoding);
        StringBuilder sb = new StringBuilder();
        try (BufferedReader reader =
                new BufferedReader(new InputStreamReader(new FileInputStream(filePath), charset))) {
            String line;
            while ((line = reader.readLine()) != null) {
                sb.append(line);
            }
        }
        String jsonText = sb.toString().trim();
        if (!jsonText.startsWith("[")) {
            System.err.println("JSON file must be an array: [{...}, ...]");
            return 0;
        }

        ObjectMapper mapper = new ObjectMapper();
        JsonNode arrayNode = mapper.readTree(jsonText);
        if (!arrayNode.isArray()) {
            System.err.println("JSON file must be an array: [{...}, ...]");
            return 0;
        }

        int rowCount = 0;
        for (JsonNode objNode : arrayNode) {
            String[] values = new String[fields.size()];
            for (int i = 0; i < fields.size(); i++) {
                String fieldName = fields.get(i).name();
                JsonNode valNode = objNode.get(fieldName);
                if (valNode == null || valNode.isNull()) {
                    values[i] = "NULL";
                } else if (valNode.isTextual()) {
                    values[i] = valNode.textValue();
                } else {
                    values[i] = valNode.toString();
                }
            }
            GenericRow row = parseRow(values, fields);
            writer.write(row);
            rowCount++;
        }
        return rowCount;
    }

    private static GenericRow parseRow(String[] values, List<DataField> fields) {
        GenericRow row = new GenericRow(values.length);
        for (int i = 0; i < values.length; i++) {
            String value = values[i];
            if (value.isEmpty() || "NULL".equalsIgnoreCase(value)) {
                row.setField(i, null);
                continue;
            }
            row.setField(i, parseValue(value, fields.get(i).type()));
        }
        return row;
    }

    private static Object parseValue(String value, DataType type) {
        DataTypeRoot root = type.getTypeRoot();
        switch (root) {
            case BOOLEAN:
                return Boolean.parseBoolean(value);
            case TINYINT:
                return Byte.parseByte(value);
            case SMALLINT:
                return Short.parseShort(value);
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                return Integer.parseInt(value);
            case BIGINT:
                return Long.parseLong(value);
            case FLOAT:
                return Float.parseFloat(value);
            case DOUBLE:
                return Double.parseDouble(value);
            case DECIMAL:
                {
                    int precision = DataTypeChecks.getPrecision(type);
                    int scale = DataTypeChecks.getScale(type);
                    return Decimal.fromBigDecimal(new BigDecimal(value), precision, scale);
                }
            case CHAR:
            case VARCHAR:
                return BinaryString.fromString(value);
            case BINARY:
            case VARBINARY:
                return Base64.getDecoder().decode(value);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return Timestamp.fromLocalDateTime(LocalDateTime.parse(value.replace(" ", "T")));
            default:
                return BinaryString.fromString(value);
        }
    }

    private static boolean hasUnclosedQuote(String line) {
        boolean inQuotes = false;
        for (int i = 0; i < line.length(); i++) {
            char ch = line.charAt(i);
            if (ch == '"') {
                if (inQuotes && i + 1 < line.length() && line.charAt(i + 1) == '"') {
                    i++;
                } else {
                    inQuotes = !inQuotes;
                }
            }
        }
        return inQuotes;
    }

    private static String[] parseCsvLine(String line) {
        List<String> fields = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;

        for (int i = 0; i < line.length(); i++) {
            char ch = line.charAt(i);
            if (inQuotes) {
                if (ch == '"') {
                    if (i + 1 < line.length() && line.charAt(i + 1) == '"') {
                        current.append('"');
                        i++;
                    } else {
                        inQuotes = false;
                    }
                } else {
                    current.append(ch);
                }
            } else {
                if (ch == '"') {
                    inQuotes = true;
                } else if (ch == ',') {
                    fields.add(current.toString());
                    current.setLength(0);
                } else {
                    current.append(ch);
                }
            }
        }
        fields.add(current.toString());
        return fields.toArray(new String[0]);
    }

    private static boolean isHeaderRow(String[] values, List<DataField> fields) {
        for (int i = 0; i < values.length; i++) {
            if (!values[i].equals(fields.get(i).name())) {
                return false;
            }
        }
        return true;
    }
}
