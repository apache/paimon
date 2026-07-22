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

package org.apache.paimon.spark.format;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.fs.Path;
import org.apache.paimon.partition.Partition;
import org.apache.paimon.table.FormatTable;
import org.apache.paimon.table.format.FormatTablePartitionManager;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.PartitionPathUtils;
import org.apache.paimon.utils.Preconditions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Repairs a Format Table with catalog-managed partitions by reconciling its partition directories
 * with its catalog registration, backing {@code MSCK REPAIR TABLE [{ADD|DROP|SYNC} PARTITIONS]}.
 * Only partition metadata is changed, data files are never touched.
 */
public class FormatTablePartitionRepair {

    private FormatTablePartitionRepair() {}

    /**
     * Repair the partition metadata of a Format Table with catalog-managed partitions. The flag
     * pair follows Spark's {@code RepairTable} plan (plain MSCK means ADD). Returns the number of
     * applied actions.
     */
    public static int repair(
            PaimonFormatTable sparkTable, boolean addPartitions, boolean dropPartitions) {
        Preconditions.checkArgument(
                addPartitions || dropPartitions,
                "MSCK REPAIR TABLE must enable ADD and/or DROP partitions");
        FormatTable formatTable = sparkTable.table();
        FormatTablePartitionManager partitionManager = formatTable.partitionManager();
        Preconditions.checkArgument(
                partitionManager != null,
                "%s does not have catalog-managed partitions",
                formatTable.fullName());

        return apply(
                partitionManager,
                listFilesystemPartitionSpecs(formatTable),
                formatTable.partitionKeys(),
                addPartitions,
                dropPartitions);
    }

    private static List<Map<String, String>> listFilesystemPartitionSpecs(FormatTable formatTable) {
        // Discover partitions from the raw directory names rather than through the table scan:
        // the scan casts each value to its column type and back (e.g. month=01 -> 1), producing
        // specs that can no longer round-trip to the real directory. The write path registers the
        // raw directory value, so a repair must diff against the same raw values to avoid
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
     * Diff the filesystem partition set against the catalog registration set and apply the
     * requested actions. ADD registers "directory exists but unregistered"; DROP is metadata-only
     * cleanup of "registered but directory missing". Scan-completeness guard: the filesystem
     * listing that feeds {@code filesystemPartitions} ({@link
     * PartitionPathUtils#searchPartSpecAndPaths}) fails on any mid-scan LIST error instead of
     * returning a truncated set, so a DROP diff can only be produced from a complete listing and a
     * transient failure never deregisters partitions that still exist.
     */
    static int apply(
            FormatTablePartitionManager partitionManager,
            List<Map<String, String>> filesystemPartitions,
            List<String> partitionKeys,
            boolean addPartitions,
            boolean dropPartitions) {
        Set<Map<String, String>> registeredPartitions = new HashSet<>();
        for (Partition partition :
                partitionManager.listPartitions(Collections.<String, String>emptyMap(), null)) {
            registeredPartitions.add(partition.spec());
        }

        Set<Map<String, String>> filesystemSet = new HashSet<>(filesystemPartitions);

        List<Map<String, String>> addDiff = new ArrayList<>();
        if (addPartitions) {
            for (Map<String, String> partition : filesystemPartitions) {
                if (!registeredPartitions.contains(partition)) {
                    addDiff.add(partition);
                }
            }
            sortByCanonicalPath(addDiff, partitionKeys);
        }
        List<Map<String, String>> dropDiff = new ArrayList<>();
        if (dropPartitions) {
            for (Map<String, String> partition : registeredPartitions) {
                if (!filesystemSet.contains(partition)) {
                    dropDiff.add(partition);
                }
            }
            sortByCanonicalPath(dropDiff, partitionKeys);
        }

        // A first repair of a pre-existing table can discover far more partitions than any regular
        // write. Splitting such a diff into per-request batches is the partition catalog's job;
        // registration is an idempotent upsert and unregistration ignores missing partitions, so a
        // mid-way failure leaves a state a rerun converges from.
        if (!addDiff.isEmpty()) {
            partitionManager.createPartitions(addDiff, true);
        }
        if (!dropDiff.isEmpty()) {
            partitionManager.dropPartitions(dropDiff);
        }
        return addDiff.size() + dropDiff.size();
    }

    /** Sort partitions by their canonical path for a stable, deterministic apply order. */
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
}
