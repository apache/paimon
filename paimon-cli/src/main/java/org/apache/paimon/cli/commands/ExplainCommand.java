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

import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.cli.CliArgs;
import org.apache.paimon.cli.Command;
import org.apache.paimon.cli.CommandContext;
import org.apache.paimon.cli.PredicateParser;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.DataField;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** Shows the scan plan of a query without reading data. */
public class ExplainCommand implements Command {

    @Override
    public String name() {
        return "explain";
    }

    @Override
    public String description() {
        return "Show the scan plan of a table query without reading data";
    }

    @Override
    public void execute(CommandContext ctx, String[] args) throws Exception {
        CliArgs parsed = new CliArgs(args, "-s", "--select", "-w", "--where", "-l", "--limit");

        String tableId = parsed.positional(0, "DATABASE.TABLE");
        String[] parts = SchemaCommand.parseTableIdentifier(tableId);

        String selectStr = parsed.get("select");
        String whereStr = parsed.get("where");
        String limitStr = parsed.get("limit");

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
                    return;
                }
                projection[i] = idx;
            }
            readBuilder.withProjection(projection);
        }

        Predicate predicate = null;
        if (whereStr != null && !whereStr.isEmpty()) {
            PredicateParser parser = new PredicateParser(table.rowType());
            predicate = parser.parse(whereStr);
            readBuilder.withFilter(predicate);
        }

        int limit = -1;
        if (limitStr != null) {
            limit = Integer.parseInt(limitStr);
            readBuilder.withLimit(limit);
        }

        TableScan scan = readBuilder.newScan();
        List<Split> splits = scan.plan().splits();

        Optional<Snapshot> snapshot = table.latestSnapshot();
        String tableType = inferTableType(table);

        System.out.println("== Paimon Scan Plan ==");
        System.out.println("Table:              " + tableId + " (" + tableType + ")");
        if (snapshot.isPresent()) {
            Snapshot snap = snapshot.get();
            System.out.println(
                    "Snapshot:           " + snap.id() + "  (schema " + snap.schemaId() + ")");
        } else {
            System.out.println("Snapshot:           <none>");
        }

        System.out.println("Predicate:          " + (predicate != null ? whereStr : "<none>"));

        if (projection != null) {
            StringBuilder projStr = new StringBuilder("[");
            for (int i = 0; i < projection.length; i++) {
                if (i > 0) {
                    projStr.append(", ");
                }
                projStr.append(allFields.get(projection[i]).name());
            }
            projStr.append("]");
            System.out.println("Projection:         " + projStr);
        } else {
            System.out.println("Projection:         <all>");
        }

        System.out.println("Limit:              " + (limit > 0 ? limit : "<none>"));

        System.out.println();
        printSplitStats(splits, table);
    }

    private void printSplitStats(List<Split> splits, Table table) {
        int splitCount = splits.size();
        int totalFiles = 0;
        long totalSize = 0;
        long totalRows = 0;
        long mergedRows = 0;
        boolean hasMergedCount = true;
        int rawConvertibleCount = 0;
        int withDvCount = 0;
        Map<Integer, Integer> levelHistogram = new HashMap<>();
        int deletionFileCount = 0;
        Set<String> partitions = new HashSet<>();
        Set<Integer> buckets = new HashSet<>();

        List<Integer> filesPerSplit = new ArrayList<>();
        List<Long> sizePerSplit = new ArrayList<>();

        for (Split split : splits) {
            if (split instanceof DataSplit) {
                DataSplit ds = (DataSplit) split;
                List<DataFileMeta> files = ds.dataFiles();
                int fileCount = files.size();
                totalFiles += fileCount;
                filesPerSplit.add(fileCount);

                long splitSize = 0;
                for (DataFileMeta file : files) {
                    splitSize += file.fileSize();
                    totalRows += file.rowCount();
                    levelHistogram.merge(file.level(), 1, Integer::sum);
                }
                totalSize += splitSize;
                sizePerSplit.add(splitSize);

                if (ds.rawConvertible()) {
                    rawConvertibleCount++;
                }
                if (ds.deletionFiles().isPresent()
                        && ds.deletionFiles().get().stream().anyMatch(f -> f != null)) {
                    withDvCount++;
                    deletionFileCount +=
                            (int) ds.deletionFiles().get().stream().filter(f -> f != null).count();
                }
                if (ds.mergedRowCount().isPresent()) {
                    mergedRows += ds.mergedRowCount().getAsLong();
                } else {
                    hasMergedCount = false;
                }
                partitions.add(ds.partition().toString());
                buckets.add(ds.bucket());
            }
        }

        System.out.println("Splits:             " + splitCount);
        if (splitCount > 0) {
            System.out.println("  raw-convertible:  " + rawConvertibleCount + " / " + splitCount);
            System.out.println("  with DV:          " + withDvCount + " / " + splitCount);

            if (!filesPerSplit.isEmpty()) {
                int minFiles = filesPerSplit.stream().mapToInt(i -> i).min().orElse(0);
                int maxFiles = filesPerSplit.stream().mapToInt(i -> i).max().orElse(0);
                double avgFiles = filesPerSplit.stream().mapToInt(i -> i).average().orElse(0);
                System.out.printf(
                        "  files/split:      min=%d  max=%d  avg=%.2f%n",
                        minFiles, maxFiles, avgFiles);
            }

            if (!sizePerSplit.isEmpty()) {
                long minSize = sizePerSplit.stream().mapToLong(l -> l).min().orElse(0);
                long maxSize = sizePerSplit.stream().mapToLong(l -> l).max().orElse(0);
                sizePerSplit.sort(Long::compareTo);
                long p50Size = sizePerSplit.get(sizePerSplit.size() / 2);
                long p95Size = sizePerSplit.get((int) (sizePerSplit.size() * 0.95));
                System.out.printf(
                        "  size/split:       min=%s  p50=%s  p95=%s  max=%s%n",
                        formatSize(minSize),
                        formatSize(p50Size),
                        formatSize(p95Size),
                        formatSize(maxSize));
            }
        }

        System.out.println("Files:              " + totalFiles);
        System.out.println("Total size:         " + formatSize(totalSize));
        String rowStr = "Estimated rows:     " + totalRows;
        if (hasMergedCount && mergedRows != totalRows) {
            rowStr += "   (merged: " + mergedRows + ")";
        }
        System.out.println(rowStr);

        if (!levelHistogram.isEmpty()) {
            StringBuilder lb = new StringBuilder("Level histogram:    ");
            List<Integer> levels = new ArrayList<>(levelHistogram.keySet());
            levels.sort(Integer::compareTo);
            for (int i = 0; i < levels.size(); i++) {
                if (i > 0) {
                    lb.append("  ");
                }
                lb.append("L")
                        .append(levels.get(i))
                        .append("=")
                        .append(levelHistogram.get(levels.get(i)));
            }
            System.out.println(lb);
        }

        System.out.println("Deletion files:     " + deletionFileCount);
        System.out.println("Partitions hit:     " + partitions.size());
        System.out.println("Buckets hit:        " + buckets.size());
    }

    private static String inferTableType(Table table) {
        List<String> traits = new ArrayList<>();
        if (!table.primaryKeys().isEmpty()) {
            traits.add("PK");
        } else {
            traits.add("Append");
        }
        String bucketMode = table.options().getOrDefault("bucket", "-1");
        if (!"-1".equals(bucketMode)) {
            traits.add("HASH_FIXED");
        } else {
            traits.add("DYNAMIC");
        }
        return String.join(", ", traits);
    }

    private static String formatSize(long bytes) {
        if (bytes < 1024) {
            return bytes + " B";
        } else if (bytes < 1024 * 1024) {
            return String.format("%.1f KiB", bytes / 1024.0);
        } else if (bytes < 1024L * 1024 * 1024) {
            return String.format("%.1f MiB", bytes / (1024.0 * 1024));
        } else {
            return String.format("%.1f GiB", bytes / (1024.0 * 1024 * 1024));
        }
    }

    private static int findFieldIndex(List<DataField> fields, String name) {
        for (int i = 0; i < fields.size(); i++) {
            if (fields.get(i).name().equals(name)) {
                return i;
            }
        }
        return -1;
    }

    @Override
    public String usage() {
        return "Usage: paimon explain DATABASE.TABLE [options]\n\n"
                + "Show the scan plan without reading data.\n\n"
                + "Options:\n"
                + "  -s, --select   Columns to project (comma-separated)\n"
                + "  -w, --where    Filter condition (SQL-like syntax)\n"
                + "  -l, --limit    Row limit to push down";
    }
}
