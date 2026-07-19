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

package org.apache.paimon.table.format;

import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.partition.Partition;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.FormatTable;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.PartitionPathUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** A format table scan whose partition visibility is owned by a catalog. */
public class ManagedFormatTableScan extends FormatTableScan {

    private static final Logger LOG = LoggerFactory.getLogger(ManagedFormatTableScan.class);

    private final FormatTableCatalogProvider catalogProvider;

    public ManagedFormatTableScan(
            FormatTable table,
            @Nullable PartitionPredicate partitionFilter,
            @Nullable Integer limit) {
        super(table, partitionFilter, limit);
        FormatTableCatalogProvider provider = table.catalogProvider();
        if (provider == null) {
            throw new IllegalStateException(
                    String.format(
                            "Managed format table %s has no catalog partition provider.",
                            table.fullName()));
        }
        this.catalogProvider = provider;
    }

    @Override
    protected List<Pair<LinkedHashMap<String, String>, Path>> findPartitions() {
        String partitionNamePattern = createPartitionNamePattern();
        List<Partition> partitions = catalogProvider.listPartitions(partitionNamePattern);
        if (partitions.isEmpty() && partitionNamePattern == null) {
            warnIfFilesystemPartitionsExist();
        }
        List<Pair<LinkedHashMap<String, String>, Path>> result = new ArrayList<>(partitions.size());
        Path tablePath = new Path(table.location());
        // Do not trust the catalog to be duplicate-free: a repeated spec would double every split
        // of that partition and silently duplicate query results.
        Set<String> seenPartitionPaths = new HashSet<>(partitions.size());
        for (Partition partition : partitions) {
            LinkedHashMap<String, String> partitionSpec = normalizeSpec(partition.spec());
            String partitionPath =
                    PartitionPathUtils.generatePartitionPathUtil(
                            partitionSpec, coreOptions.formatTablePartitionOnlyValueInPath());
            if (!seenPartitionPaths.add(partitionPath)) {
                continue;
            }
            result.add(Pair.of(partitionSpec, new Path(tablePath, partitionPath)));
        }
        return result;
    }

    @Override
    public List<PartitionEntry> listPartitionEntries() {
        List<Partition> partitions = catalogProvider.listPartitions(null);
        if (partitions.isEmpty()) {
            warnIfFilesystemPartitionsExist();
        }
        List<PartitionEntry> entries = new ArrayList<>(partitions.size());
        for (Partition partition : partitions) {
            entries.add(
                    new PartitionEntry(
                            toPartitionRow(normalizeSpec(partition.spec())),
                            partition.recordCount(),
                            partition.fileSizeInBytes(),
                            partition.fileCount(),
                            partition.lastFileCreationTime(),
                            partition.totalBuckets()));
        }
        return entries;
    }

    /**
     * Warn when the catalog knows no partitions but the table directory contains subdirectories:
     * typically a table that predates managed partition support (or was written by a client that
     * does not register partitions) and needs a metadata sync before its data becomes visible.
     */
    private void warnIfFilesystemPartitionsExist() {
        try {
            for (FileStatus status : table.fileIO().listStatus(new Path(table.location()))) {
                if (status.isDir() && !status.getPath().getName().startsWith(".")) {
                    LOG.warn(
                            "Managed format table {} has no partitions registered in the catalog "
                                    + "but its location {} contains directories. Data written "
                                    + "before enabling catalog-managed partitions (or by clients "
                                    + "that do not register partitions) is invisible until the "
                                    + "partition metadata is synced, e.g. with the Spark procedure "
                                    + "sys.sync_format_table_metadata or MSCK REPAIR TABLE.",
                            table.fullName(),
                            table.location());
                    return;
                }
            }
        } catch (IOException ignored) {
            // Best-effort hint only; never fail or slow down the scan because of it.
        }
    }

    /**
     * Build the partition-name prefix pattern pushed down to the catalog, or {@code null} to list
     * all partitions without pushdown. See {@link
     * PartitionPathUtils#buildPartitionNamePrefixPattern} for the pattern contract; this returns
     * {@code null} when there is no partition predicate, when the predicate has no leading equality
     * prefix, or when the prefix cannot be expressed in the pattern contract.
     */
    @Nullable
    String createPartitionNamePattern() {
        Optional<Predicate> predicate = extractPartitionPredicate(partitionFilter);
        if (!predicate.isPresent()) {
            return null;
        }

        Map<String, String> equalityPrefix =
                extractLeadingEqualityPartitionSpecWhenOnlyAnd(
                        table.partitionKeys(), predicate.get(), table.partitionType());
        return PartitionPathUtils.buildPartitionNamePrefixPattern(
                table.partitionKeys(), equalityPrefix);
    }

    private LinkedHashMap<String, String> normalizeSpec(Map<String, String> spec) {
        if (spec == null
                || spec.size() != table.partitionKeys().size()
                || !spec.keySet().containsAll(table.partitionKeys())) {
            throw corruptPartitionSpec(spec);
        }
        LinkedHashMap<String, String> normalized = new LinkedHashMap<>();
        boolean onlyValueInPath = coreOptions.formatTablePartitionOnlyValueInPath();
        for (String partitionKey : table.partitionKeys()) {
            String value = spec.get(partitionKey);
            // Catalog metadata is not trusted for path construction. In a value-only layout,
            // reject complete path components such as '.' and '..' that would escape the table.
            try {
                PartitionPathUtils.validatePartitionValueForPath(value, onlyValueInPath);
            } catch (IllegalArgumentException e) {
                throw corruptPartitionSpec(spec);
            }
            normalized.put(partitionKey, value);
        }
        return normalized;
    }

    private IllegalStateException corruptPartitionSpec(@Nullable Map<String, String> spec) {
        return new IllegalStateException(
                String.format(
                        "Catalog returned corrupt partition metadata %s for managed format table %s; "
                                + "expected exactly the partition keys %s with values usable as "
                                + "path components.",
                        spec, table.fullName(), table.partitionKeys()));
    }

    @Override
    protected void onPartitionFileNotFound(
            LinkedHashMap<String, String> partitionSpec,
            Path partitionPath,
            FileNotFoundException exception) {
        // A registered partition without a directory reads as empty, matching Hive semantics
        // (e.g. ADD PARTITION before the first INSERT). Warn so genuine drift — a directory
        // removed behind the catalog's back — is still discoverable.
        LOG.warn(
                "Partition '{}' of managed format table {} is registered in the catalog but its "
                        + "directory '{}' does not exist; treating the partition as empty. If the "
                        + "directory was removed on purpose, drop the partition or repair the "
                        + "metadata, e.g. with the Spark procedure sys.sync_format_table_metadata "
                        + "or MSCK REPAIR TABLE.",
                PartitionPathUtils.generatePartitionName(partitionSpec, false),
                table.fullName(),
                partitionPath);
    }
}
