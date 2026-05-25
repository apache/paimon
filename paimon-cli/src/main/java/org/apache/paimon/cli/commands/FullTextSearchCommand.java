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
import org.apache.paimon.cli.RowPrinter;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.FullTextSearchBuilder;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.DataField;

import java.util.ArrayList;
import java.util.List;

/** Performs full-text search on a table with a Tantivy index. */
public class FullTextSearchCommand implements Command {

    @Override
    public String name() {
        return "full-text-search";
    }

    @Override
    public String description() {
        return "Full-text search on a table column";
    }

    @Override
    public void execute(CommandContext ctx, String[] args) throws Exception {
        CliArgs parsed =
                new CliArgs(
                        args,
                        "-c",
                        "--column",
                        "-q",
                        "--query",
                        "-l",
                        "--limit",
                        "-s",
                        "--select",
                        "-f",
                        "--format");

        String tableId = parsed.positional(0, "DATABASE.TABLE");
        String[] parts = SchemaCommand.parseTableIdentifier(tableId);

        String column = parsed.require("column", "text column to search");
        String query = parsed.require("query", "query text");
        String limitStr = parsed.get("limit");
        int limit = limitStr != null ? Integer.parseInt(limitStr) : 10;
        String selectStr = parsed.get("select");
        String format = parsed.getOrDefault("format", "tsv");

        Table table = ctx.getCatalog().getTable(Identifier.create(parts[0], parts[1]));

        FullTextSearchBuilder ftsBuilder = table.newFullTextSearchBuilder();
        ftsBuilder.withTextColumn(column);
        ftsBuilder.withQueryText(query);
        ftsBuilder.withLimit(limit);

        GlobalIndexResult indexResult = ftsBuilder.executeLocal();

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
                    return;
                }
                projection[i] = idx;
            }
            readBuilder.withProjection(projection);
        }

        readBuilder.withLimit(limit);

        TableScan scan = readBuilder.newScan();
        scan.withGlobalIndexResult(indexResult);
        List<Split> splits = scan.plan().splits();

        if (splits.isEmpty()) {
            System.err.println("No results found.");
            return;
        }

        TableRead tableRead = readBuilder.newRead();
        List<DataField> outputFields =
                projection != null ? selectFields(allFields, projection) : allFields;

        boolean isJson = "json".equals(format);
        String delim = "csv".equals(format) ? "," : "\t";

        if (!isJson) {
            printHeader(outputFields, delim);
        }

        int rowCount = 0;
        for (Split split : splits) {
            if (rowCount >= limit) {
                break;
            }
            try (RecordReader<InternalRow> reader = tableRead.createReader(split)) {
                RecordReader.RecordIterator<InternalRow> batch;
                while ((batch = reader.readBatch()) != null) {
                    InternalRow row;
                    while ((row = batch.next()) != null) {
                        if (rowCount >= limit) {
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
                    if (rowCount >= limit) {
                        break;
                    }
                }
            }
        }
        System.err.println("Found " + rowCount + " results.");
    }

    @Override
    public String usage() {
        return "Usage: paimon full-text-search DATABASE.TABLE [options]\n\n"
                + "Full-text search on a table column using Tantivy index.\n\n"
                + "Options:\n"
                + "  -c, --column   Text column to search on (required)\n"
                + "  -q, --query    Query text (required)\n"
                + "  -l, --limit    Maximum results (default: 10)\n"
                + "  -s, --select   Columns to display (comma-separated)\n"
                + "  -f, --format   Output format: tsv (default) / csv / json\n\n"
                + "Note: Requires paimon-tantivy-index on the classpath.";
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

    private static void printHeader(List<DataField> fields, String delim) {
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
                sb.append(RowPrinter.getFieldValue(row, i, fields.get(i).type()));
            }
        }
        System.out.println(sb.toString());
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
