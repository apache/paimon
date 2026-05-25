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
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.InternalRowPartitionComputer;

import java.util.ArrayList;
import java.util.List;

/** Lists partitions of a table with statistics. */
public class ListPartitionsCommand implements Command {

    @Override
    public String name() {
        return "list-partitions";
    }

    @Override
    public String description() {
        return "List partitions of a table";
    }

    @Override
    public void execute(CommandContext ctx, String[] args) throws Exception {
        CliArgs parsed = new CliArgs(args, "-f", "--format");
        String tableId = parsed.positional(0, "DATABASE.TABLE");
        String[] parts = SchemaCommand.parseTableIdentifier(tableId);
        String format = parsed.getOrDefault("format", "table");

        Table table = ctx.getCatalog().getTable(Identifier.create(parts[0], parts[1]));
        List<String> partitionKeys = table.partitionKeys();
        if (partitionKeys.isEmpty()) {
            System.out.println("Table '" + tableId + "' is not partitioned.");
            return;
        }

        RowType partType = buildPartitionType(table.rowType(), partitionKeys);
        TableScan scan = table.newReadBuilder().newScan();
        List<PartitionEntry> entries = scan.listPartitionEntries();

        if (entries.isEmpty()) {
            System.out.println("No partitions found for '" + tableId + "'.");
            return;
        }

        if ("json".equals(format)) {
            printJson(entries, partType);
        } else {
            printTable(entries, partType);
        }
    }

    private void printTable(List<PartitionEntry> entries, RowType partType) {
        System.out.println(
                String.format(
                        "%-40s %12s %16s %10s %20s",
                        "Partition",
                        "RecordCount",
                        "FileSizeInBytes",
                        "FileCount",
                        "LastFileCreationTime"));
        for (PartitionEntry entry : entries) {
            String partStr =
                    InternalRowPartitionComputer.partToSimpleString(
                            partType, entry.partition(), ",", 200);
            System.out.println(
                    String.format(
                            "%-40s %12d %16d %10d %20d",
                            partStr,
                            entry.recordCount(),
                            entry.fileSizeInBytes(),
                            entry.fileCount(),
                            entry.lastFileCreationTime()));
        }
    }

    private void printJson(List<PartitionEntry> entries, RowType partType) {
        System.out.println("[");
        for (int i = 0; i < entries.size(); i++) {
            PartitionEntry entry = entries.get(i);
            String partStr =
                    InternalRowPartitionComputer.partToSimpleString(
                            partType, entry.partition(), ",", 200);
            System.out.print(
                    "  {\"partition\":\""
                            + partStr
                            + "\",\"recordCount\":"
                            + entry.recordCount()
                            + ",\"fileSizeInBytes\":"
                            + entry.fileSizeInBytes()
                            + ",\"fileCount\":"
                            + entry.fileCount()
                            + ",\"lastFileCreationTime\":"
                            + entry.lastFileCreationTime()
                            + "}");
            if (i < entries.size() - 1) {
                System.out.println(",");
            } else {
                System.out.println();
            }
        }
        System.out.println("]");
    }

    private static RowType buildPartitionType(RowType rowType, List<String> partitionKeys) {
        List<DataField> partFields = new ArrayList<>();
        for (String key : partitionKeys) {
            int idx = rowType.getFieldIndex(key);
            DataType type = rowType.getTypeAt(idx);
            partFields.add(new DataField(partFields.size(), key, type));
        }
        return new RowType(partFields);
    }

    @Override
    public String usage() {
        return "Usage: paimon list-partitions DATABASE.TABLE [options]\n\n"
                + "List partitions of a table with statistics.\n\n"
                + "Options:\n"
                + "  -f, --format   Output format: table (default) / json";
    }
}
