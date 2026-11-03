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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.partition.Partition;
import org.apache.paimon.partition.actions.PartitionMarkDoneAction;
import org.apache.paimon.partition.actions.SuccessFileMarkDoneAction;
import org.apache.paimon.table.FormatTable;
import org.apache.paimon.utils.PartitionPathUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * A {@link PartitionMarkDoneAction} which creates an empty "_SUCCESS" file in a {@link FormatTable}
 * partition directory, as Hadoop jobs do. Catalog-managed partitions at a custom location or not
 * registered are skipped, so the file is only written inside the table directory.
 */
public class FormatTableSuccessFileMarkDoneAction implements PartitionMarkDoneAction {

    private static final Logger LOG =
            LoggerFactory.getLogger(FormatTableSuccessFileMarkDoneAction.class);

    private final FileIO fileIO;
    private final Path tablePath;
    private final List<String> partitionKeys;
    private final boolean onlyValueInPath;
    @Nullable private final FormatTablePartitionManager partitionManager;
    private final String tableName;

    public FormatTableSuccessFileMarkDoneAction(FormatTable table) {
        checkArgument(
                !table.partitionKeys().isEmpty(),
                "Format Table %s is not partitioned, so it has no partition to mark done.",
                table.fullName());
        this.fileIO = table.fileIO();
        this.tablePath = new Path(table.location());
        this.partitionKeys = table.partitionKeys();
        this.onlyValueInPath =
                new CoreOptions(table.options()).formatTablePartitionOnlyValueInPath();
        this.partitionManager = table.partitionManager();
        this.tableName = table.fullName();
    }

    @Override
    public void markDone(String partition) throws Exception {
        markDone(Collections.singletonList(partition));
    }

    @Override
    public void markDone(List<String> partitions) throws Exception {
        List<LinkedHashMap<String, String>> specs = new ArrayList<>(partitions.size());
        for (String partition : partitions) {
            LinkedHashMap<String, String> spec =
                    PartitionPathUtils.extractPartitionSpecFromPath(
                            new Path(partition), partitionKeys);
            checkArgument(
                    spec != null,
                    "Partition '%s' does not name the partition keys %s of Format Table %s in "
                            + "order.",
                    partition,
                    partitionKeys,
                    tableName);
            specs.add(spec);
        }

        Map<Map<String, String>, Partition> registered = registeredPartitions(specs);
        for (LinkedHashMap<String, String> spec : specs) {
            if (registered != null && !inTableDirectory(spec, registered.get(spec))) {
                continue;
            }
            fileIO.overwriteFileUtf8(
                    new Path(partitionPath(spec), SuccessFileMarkDoneAction.SUCCESS_FILE_NAME), "");
        }
    }

    /** One catalog lookup for all partitions, or null when partitions are not catalog-managed. */
    @Nullable
    private Map<Map<String, String>, Partition> registeredPartitions(
            List<LinkedHashMap<String, String>> specs) {
        if (partitionManager == null) {
            return null;
        }
        Map<Map<String, String>, Partition> registered = new HashMap<>();
        for (Partition partition :
                partitionManager.listPartitionsByNames(
                        new ArrayList<>(new LinkedHashSet<>(specs)))) {
            registered.put(partition.spec(), partition);
        }
        return registered;
    }

    private boolean inTableDirectory(Map<String, String> spec, @Nullable Partition partition) {
        if (partition == null) {
            LOG.warn(
                    "Skip creating the success file of partition {} of Format Table {}: the "
                            + "catalog does not register this partition.",
                    spec,
                    tableName);
            return false;
        }
        String customLocation = FormatTablePartitionPathResolver.customLocation(partition);
        if (customLocation != null) {
            LOG.warn(
                    "Skip creating the success file of partition {} of Format Table {}: the "
                            + "partition is registered at custom location '{}', outside the "
                            + "table directory.",
                    spec,
                    tableName,
                    customLocation);
            return false;
        }
        return true;
    }

    private Path partitionPath(LinkedHashMap<String, String> spec) {
        return FormatTablePartitionPathResolver.defaultPartitionPath(
                tablePath, spec, onlyValueInPath);
    }

    @Override
    public void close() {}
}
