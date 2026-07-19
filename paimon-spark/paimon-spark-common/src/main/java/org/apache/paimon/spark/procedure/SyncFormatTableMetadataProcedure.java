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

package org.apache.paimon.spark.procedure;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.fs.Path;
import org.apache.paimon.spark.format.FormatTablePartitionCatalog;
import org.apache.paimon.spark.format.FormatTablePartitionPage;
import org.apache.paimon.spark.format.PaimonFormatTable;
import org.apache.paimon.table.FormatTable;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.PartitionPathUtils;
import org.apache.paimon.utils.Preconditions;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.apache.spark.sql.types.DataTypes.BooleanType;
import static org.apache.spark.sql.types.DataTypes.StringType;

/** Reconciles filesystem partitions with managed format-table catalog metadata. */
public class SyncFormatTableMetadataProcedure extends BaseProcedure {

    private static final int CATALOG_PAGE_SIZE = 1000;

    private static final ProcedureParameter[] PARAMETERS =
            new ProcedureParameter[] {
                ProcedureParameter.required("table", StringType),
                ProcedureParameter.optional("mode", StringType),
                ProcedureParameter.optional("dry_run", BooleanType)
            };

    private static final StructType OUTPUT_TYPE =
            new StructType(
                    new StructField[] {
                        new StructField("partition", StringType, false, Metadata.empty()),
                        new StructField("action", StringType, false, Metadata.empty()),
                        new StructField("status", StringType, false, Metadata.empty())
                    });

    protected SyncFormatTableMetadataProcedure(TableCatalog tableCatalog) {
        super(tableCatalog);
    }

    @Override
    public ProcedureParameter[] parameters() {
        return PARAMETERS;
    }

    @Override
    public StructType outputType() {
        return OUTPUT_TYPE;
    }

    @Override
    public InternalRow[] call(InternalRow args) {
        Identifier ident = toIdentifier(args.getString(0), PARAMETERS[0].name());
        String mode = args.isNullAt(1) ? "ADD" : args.getString(1);
        boolean dryRun = args.isNullAt(2) || args.getBoolean(2);
        validateMode(mode);

        PaimonFormatTable sparkTable = loadFormatTable(ident);
        FormatTable formatTable = sparkTable.table();
        FormatTablePartitionCatalog partitionCatalog = managedPartitionCatalog(sparkTable);

        List<Map.Entry<Map<String, String>, String>> actions;
        try {
            actions =
                    syncPartitions(
                            partitionCatalog,
                            listFilesystemPartitionSpecs(formatTable),
                            formatTable.partitionKeys(),
                            dryRun,
                            mode);
        } finally {
            // A failed batch response cannot tell us whether the service committed, so the
            // Spark-side caches are refreshed after every non-preview attempt.
            if (!dryRun) {
                refreshSparkCache(ident, sparkTable);
            }
        }

        RowType partitionType = formatTable.partitionType();
        return actions.stream()
                .map(
                        entry ->
                                newInternalRow(
                                        UTF8String.fromString(
                                                partitionPath(entry.getKey(), partitionType)),
                                        UTF8String.fromString(entry.getValue()),
                                        UTF8String.fromString(
                                                dryRun
                                                        ? "DRY_RUN"
                                                        : ("ADD".equals(entry.getValue())
                                                                ? "REGISTERED"
                                                                : "UNREGISTERED"))))
                .toArray(InternalRow[]::new);
    }

    static void validateMode(String mode) {
        Preconditions.checkArgument(
                "ADD".equals(mode.toUpperCase(Locale.ROOT))
                        || "DROP".equals(mode.toUpperCase(Locale.ROOT))
                        || "SYNC".equals(mode.toUpperCase(Locale.ROOT)),
                "Unsupported mode '%s'; supported modes are ADD, DROP and SYNC",
                mode);
    }

    public static ProcedureBuilder builder() {
        return new BaseProcedure.Builder<SyncFormatTableMetadataProcedure>() {
            @Override
            protected SyncFormatTableMetadataProcedure doBuild() {
                return new SyncFormatTableMetadataProcedure(tableCatalog());
            }
        };
    }

    /**
     * Shared engine for {@code MSCK REPAIR TABLE [{ADD|DROP|SYNC} PARTITIONS]} on a managed Format
     * Table. MSCK applies immediately (never a dry run); the flag pair follows Spark's {@code
     * RepairTable} plan (plain MSCK means ADD). Returns the number of applied actions.
     */
    public static int repairFormatTablePartitions(
            PaimonFormatTable sparkTable, boolean addPartitions, boolean dropPartitions) {
        Preconditions.checkArgument(
                addPartitions || dropPartitions,
                "MSCK REPAIR TABLE must enable ADD and/or DROP partitions");
        FormatTable formatTable = sparkTable.table();
        FormatTablePartitionCatalog partitionCatalog = managedPartitionCatalog(sparkTable);
        String mode = addPartitions && dropPartitions ? "SYNC" : (addPartitions ? "ADD" : "DROP");
        return syncPartitions(
                        partitionCatalog,
                        listFilesystemPartitionSpecs(formatTable),
                        formatTable.partitionKeys(),
                        false,
                        mode)
                .size();
    }

    private static List<Map<String, String>> listFilesystemPartitionSpecs(FormatTable formatTable) {
        // Discover partitions from the raw directory names rather than through the table scan:
        // the scan casts each value to its column type and back (e.g. month=01 -> 1), producing
        // specs that can no longer round-trip to the real directory. The write path registers the
        // raw directory value, so MSCK/sync must diff against the same raw values to avoid
        // spuriously adding/dropping partition metadata.
        boolean onlyValueInPath =
                new CoreOptions(formatTable.options()).formatTablePartitionOnlyValueInPath();
        List<Pair<LinkedHashMap<String, String>, Path>> found =
                PartitionPathUtils.searchPartSpecAndPaths(
                        formatTable.fileIO(),
                        new Path(formatTable.location()),
                        formatTable.partitionKeys().size(),
                        formatTable.partitionKeys(),
                        onlyValueInPath);
        List<Map<String, String>> specs = new ArrayList<>(found.size());
        for (Pair<LinkedHashMap<String, String>, Path> pair : found) {
            PartitionPathUtils.validatePartitionSpecForPath(pair.getKey(), onlyValueInPath);
            specs.add(pair.getKey());
        }
        return specs;
    }

    /**
     * Diff the filesystem partition set against the catalog registration set and, unless dryRun,
     * apply the requested mode. ADD registers "directory exists but unregistered"; DROP is
     * metadata-only cleanup of "registered but directory missing" (data is never touched); SYNC is
     * both. Scan-completeness guard: the filesystem listing that feeds {@code filesystemPartitions}
     * ({@link PartitionPathUtils#searchPartSpecAndPaths}) fails on any mid-scan LIST error instead
     * of returning a truncated set, so a DROP diff can only be produced from a complete listing and
     * a transient failure never deregisters partitions that still exist.
     */
    static List<Map.Entry<Map<String, String>, String>> syncPartitions(
            FormatTablePartitionCatalog catalog,
            List<Map<String, String>> filesystemPartitions,
            List<String> partitionKeys,
            boolean dryRun,
            String mode) {
        String normalizedMode = mode.toUpperCase(Locale.ROOT);
        Set<Map<String, String>> catalogPartitions = new HashSet<>();
        String token = null;
        do {
            FormatTablePartitionPage page = catalog.listPartitions(null, token, CATALOG_PAGE_SIZE);
            if (page.partitions() != null) {
                catalogPartitions.addAll(page.partitions());
            }
            String nextToken = page.nextPageToken();
            if (nextToken != null && nextToken.isEmpty()) {
                // A null or empty next-page token is terminal, matching PaimonPartitionManagement.
                nextToken = null;
            }
            token = nextToken;
        } while (token != null);

        Set<Map<String, String>> filesystemSet = new HashSet<>(filesystemPartitions);

        List<Map<String, String>> addDiff = new ArrayList<>();
        if (!"DROP".equals(normalizedMode)) {
            for (Map<String, String> partition : filesystemPartitions) {
                if (!catalogPartitions.contains(partition)) {
                    addDiff.add(partition);
                }
            }
            sortByCanonicalPath(addDiff, partitionKeys);
        }
        List<Map<String, String>> dropDiff = new ArrayList<>();
        if (!"ADD".equals(normalizedMode)) {
            for (Map<String, String> partition : catalogPartitions) {
                if (!filesystemSet.contains(partition)) {
                    dropDiff.add(partition);
                }
            }
            sortByCanonicalPath(dropDiff, partitionKeys);
        }

        if (!dryRun) {
            if (!addDiff.isEmpty()) {
                applyInBatches(addDiff, batch -> catalog.createPartitions(batch, true));
            }
            if (!dropDiff.isEmpty()) {
                applyInBatches(dropDiff, catalog::dropPartitions);
            }
        }
        List<Map.Entry<Map<String, String>, String>> actions = new ArrayList<>();
        addDiff.forEach(spec -> actions.add(new java.util.AbstractMap.SimpleEntry<>(spec, "ADD")));
        dropDiff.forEach(
                spec -> actions.add(new java.util.AbstractMap.SimpleEntry<>(spec, "DROP")));
        return actions;
    }

    /**
     * Apply a repair diff in bounded batches instead of a single unbounded catalog mutation. A
     * first repair of a pre-existing table can discover far more partitions than any regular write,
     * and a single request carrying all of them monopolizes a server worker and one metadata
     * transaction. Registration is an idempotent upsert and unregistration ignores missing
     * partitions, so a mid-way failure leaves a state a rerun converges from.
     */
    private static void applyInBatches(
            List<Map<String, String>> diff,
            java.util.function.Consumer<List<Map<String, String>>> operation) {
        for (int start = 0; start < diff.size(); start += CATALOG_PAGE_SIZE) {
            operation.accept(diff.subList(start, Math.min(start + CATALOG_PAGE_SIZE, diff.size())));
        }
    }

    /** Sort partitions by their canonical path for stable output and deterministic batches. */
    private static void sortByCanonicalPath(
            List<Map<String, String>> partitions, List<String> partitionKeys) {
        partitions.sort(Comparator.comparing(partition -> canonicalPath(partition, partitionKeys)));
    }

    private static String canonicalPath(Map<String, String> partition, List<String> partitionKeys) {
        LinkedHashMap<String, String> orderedSpec = new LinkedHashMap<>();
        for (String key : partitionKeys) {
            if (partition.containsKey(key)) {
                orderedSpec.put(key, partition.get(key));
            }
        }
        return PartitionPathUtils.generatePartitionPath(orderedSpec);
    }

    @Override
    public String description() {
        return "SyncFormatTableMetadataProcedure";
    }
}
