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
import org.apache.paimon.cli.PredicateParser;
import org.apache.paimon.cli.RowPrinter;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.DataField;

import java.util.ArrayList;
import java.util.List;

/** Reads data from a Paimon table. */
public class ReadCommand implements Command {

    @Override
    public String name() {
        return "read";
    }

    @Override
    public String description() {
        return "Read data from a Paimon table";
    }

    @Override
    public void execute(CommandContext ctx, String[] args) throws Exception {
        CliArgs parsed =
                new CliArgs(
                        args, "-s", "--select", "-l", "--limit", "-f", "--format", "-w", "--where");

        String tableId = parsed.positional(0, "DATABASE.TABLE");
        String[] parts = SchemaCommand.parseTableIdentifier(tableId);

        String selectStr = parsed.get("select");
        String whereStr = parsed.get("where");
        String limitStr = parsed.get("limit");
        int limit = limitStr != null ? Integer.parseInt(limitStr) : 100;
        String format = parsed.getOrDefault("format", "tsv");

        Table table = ctx.getCatalog().getTable(Identifier.create(parts[0], parts[1]));
        ReadBuilder readBuilder = table.newReadBuilder();
        List<DataField> allFields = table.rowType().getFields();

        int[] projection = null;
        if (selectStr != null && !selectStr.isEmpty()) {
            String[] cols = selectStr.split(",");
            projection = new int[cols.length];
            for (int i = 0; i < cols.length; i++) {
                String col = cols[i].trim();
                int idx = findFieldIndex(allFields, col);
                if (idx < 0) {
                    System.err.println("Column not found: " + col);
                    System.exit(1);
                }
                projection[i] = idx;
            }
            readBuilder.withProjection(projection);
        }

        if (whereStr != null && !whereStr.isEmpty()) {
            PredicateParser parser = new PredicateParser(table.rowType());
            Predicate predicate = parser.parse(whereStr);
            readBuilder.withFilter(predicate);
        }

        if (limit > 0) {
            readBuilder.withLimit(limit);
        }

        TableScan scan = readBuilder.newScan();
        List<Split> splits = scan.plan().splits();

        if (splits.isEmpty()) {
            System.err.println("No data in table.");
            return;
        }

        TableRead tableRead = readBuilder.newRead();
        List<DataField> outputFields =
                projection != null ? selectFields(allFields, projection) : allFields;

        String delim = "csv".equals(format) ? "," : "\t";
        boolean isJson = "json".equals(format);

        if (!isJson) {
            printDelimitedHeader(outputFields, delim);
        }

        int rowCount = 0;
        for (Split split : splits) {
            if (limit > 0 && rowCount >= limit) {
                break;
            }
            try (RecordReader<InternalRow> reader = tableRead.createReader(split)) {
                RecordReader.RecordIterator<InternalRow> batch;
                while ((batch = reader.readBatch()) != null) {
                    InternalRow row;
                    while ((row = batch.next()) != null) {
                        if (limit > 0 && rowCount >= limit) {
                            break;
                        }
                        if (isJson) {
                            printJsonRow(row, outputFields);
                        } else {
                            printDelimitedRow(row, outputFields, delim);
                        }
                        rowCount++;
                    }
                    batch.releaseBatch();
                    if (limit > 0 && rowCount >= limit) {
                        break;
                    }
                }
            }
        }
        System.err.println("Read " + rowCount + " rows.");
    }

    @Override
    public String usage() {
        return "Usage: paimon read DATABASE.TABLE [options]\n\n"
                + "Read data from a Paimon table.\n\n"
                + "Options:\n"
                + "  -s, --select   Columns to read (comma-separated)\n"
                + "  -w, --where    Filter condition (SQL-like syntax)\n"
                + "  -l, --limit    Maximum rows to return (default: 100)\n"
                + "  -f, --format   Output format: tsv (default) / csv / json";
    }

    private static int findFieldIndex(List<DataField> fields, String name) {
        for (int i = 0; i < fields.size(); i++) {
            if (fields.get(i).name().equals(name)) {
                return i;
            }
        }
        return -1;
    }

    private static List<DataField> selectFields(List<DataField> allFields, int[] projection) {
        List<DataField> result = new ArrayList<>();
        for (int idx : projection) {
            result.add(allFields.get(idx));
        }
        return result;
    }

    private static void printDelimitedHeader(List<DataField> fields, String delim) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < fields.size(); i++) {
            if (i > 0) {
                sb.append(delim);
            }
            sb.append(fields.get(i).name());
        }
        System.out.println(sb.toString());
    }

    private static void printDelimitedRow(InternalRow row, List<DataField> fields, String delim) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < fields.size(); i++) {
            if (i > 0) {
                sb.append(delim);
            }
            if (row.isNullAt(i)) {
                sb.append("NULL");
            } else {
                String value = RowPrinter.getFieldValue(row, i, fields.get(i).type());
                if (",".equals(delim) && needsCsvQuoting(value)) {
                    sb.append("\"").append(value.replace("\"", "\"\"")).append("\"");
                } else {
                    sb.append(value);
                }
            }
        }
        System.out.println(sb.toString());
    }

    private static boolean needsCsvQuoting(String value) {
        return value.contains(",")
                || value.contains("\"")
                || value.contains("\n")
                || value.contains("\r");
    }

    private static void printJsonRow(InternalRow row, List<DataField> fields) {
        StringBuilder sb = new StringBuilder("{");
        for (int i = 0; i < fields.size(); i++) {
            if (i > 0) {
                sb.append(",");
            }
            sb.append("\"").append(fields.get(i).name()).append("\":");
            if (row.isNullAt(i)) {
                sb.append("null");
            } else {
                String value = RowPrinter.getFieldValue(row, i, fields.get(i).type());
                if (RowPrinter.isNumericOrBoolean(fields.get(i).type().getTypeRoot())) {
                    sb.append(value);
                } else {
                    sb.append("\"").append(escapeJson(value)).append("\"");
                }
            }
        }
        sb.append("}");
        System.out.println(sb.toString());
    }

    private static String escapeJson(String value) {
        return value.replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\n", "\\n")
                .replace("\r", "\\r")
                .replace("\t", "\\t");
    }
}
