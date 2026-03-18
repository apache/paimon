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

package org.apache.paimon.flink.pipeline.cdc.source.enumerator;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.metrics.FlinkMetricRegistry;
import org.apache.paimon.flink.pipeline.cdc.source.TableAwareFileStoreSourceSplit;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.EndOfScanException;
import org.apache.paimon.table.source.SnapshotNotExistPlan;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.StreamDataTableScan;
import org.apache.paimon.table.source.StreamTableScan;
import org.apache.paimon.table.source.TableScan;

import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.metrics.groups.SplitEnumeratorMetricGroup;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.paimon.flink.pipeline.cdc.CDCOptions.DATABASE;
import static org.apache.paimon.flink.pipeline.cdc.CDCOptions.TABLE;
import static org.apache.paimon.flink.pipeline.cdc.CDCOptions.TABLE_DISCOVERY_INTERVAL;
import static org.apache.paimon.flink.pipeline.cdc.CDCOptions.toCDCOption;
import static org.apache.paimon.flink.pipeline.cdc.util.CDCUtils.createCatalog;

/** {@link SplitEnumerator} for CDC source. */
public class CDCSourceEnumerator
        implements SplitEnumerator<TableAwareFileStoreSourceSplit, CDCCheckpoint> {
    private static final Logger LOG = LoggerFactory.getLogger(CDCSourceEnumerator.class);

    private final SplitEnumeratorContext<TableAwareFileStoreSourceSplit> context;
    private final long discoveryInterval;
    private final long tableDiscoveryInterval;
    private long lastTableDiscoveryTime = 0L;
    private final AtomicInteger currentTableIndex = new AtomicInteger(0);
    private final String database;
    @Nullable private final String table;
    private final Catalog catalog;

    private final int splitMaxNum;
    private final CDCSplitAssigner splitAssigner;
    private final Set<Integer> readersAwaitingSplit;
    private final SplitIdGenerator splitIdGenerator;
    private final Map<Identifier, TableStatus> tableStatusMap = new HashMap<>();
    private final Set<Identifier> discoveredNonFileStoreTables = new HashSet<>();
    private int numTablesWithSubtaskAssigned;

    private boolean stopTriggerScan = false;

    public CDCSourceEnumerator(
            SplitEnumeratorContext<TableAwareFileStoreSourceSplit> context,
            org.apache.flink.configuration.Configuration flinkConfig,
            long discoveryInterval,
            CatalogContext catalogContext,
            Configuration cdcConfig,
            @Nullable CDCCheckpoint checkpoint) {
        this.context = context;
        this.discoveryInterval = discoveryInterval;
        this.tableDiscoveryInterval =
                cdcConfig.get(toCDCOption(TABLE_DISCOVERY_INTERVAL)).toMillis();

        this.database = cdcConfig.get(toCDCOption(DATABASE));
        this.table = cdcConfig.get(toCDCOption(TABLE));
        this.catalog = createCatalog(catalogContext, flinkConfig);

        this.numTablesWithSubtaskAssigned = 0;

        int splitMaxPerTask = catalogContext.options().get(CoreOptions.SCAN_MAX_SPLITS_PER_TASK);
        this.splitMaxNum = context.currentParallelism() * splitMaxPerTask;

        this.splitAssigner = new CDCSplitAssigner(splitMaxPerTask, context.currentParallelism());
        this.readersAwaitingSplit = new LinkedHashSet<>();
        this.splitIdGenerator = new SplitIdGenerator();

        if (checkpoint != null) {
            for (Identifier identifier : checkpoint.getCurrentSnapshotIdMap().keySet()) {
                Table table;
                try {
                    table = catalog.getTable(identifier);
                } catch (Catalog.TableNotExistException e) {
                    LOG.warn(
                            "Table {} not found after recovery. The most possible cause is it has been deleted. Skipping this table.",
                            identifier,
                            e);
                    continue;
                }

                if (!(table instanceof FileStoreTable)) {
                    LOG.warn(
                            "Table {} used to be a FileStoreTable in the last checkpoint, but after recovery it turned out to be a {}. Skipping this table.",
                            identifier,
                            table.getClass().getSimpleName());
                    continue;
                }

                TableStatus tableStatus = new TableStatus(context, (FileStoreTable) table);
                long currentSnapshotId = checkpoint.getCurrentSnapshotIdMap().get(identifier);
                tableStatus.restore(currentSnapshotId);
                tableStatusMap.put(identifier, tableStatus);
                LOG.info(
                        "Restoring state for table {}. Next snapshot id: {}",
                        identifier,
                        currentSnapshotId);
            }

            addSplits(checkpoint.getSplits());
        }
    }

    protected void addSplits(Collection<TableAwareFileStoreSourceSplit> splits) {
        splits.forEach(this::addSplit);
    }

    private void addSplit(TableAwareFileStoreSourceSplit split) {
        splitAssigner.addSplit(assignSuggestedTask(split), split);
    }

    @Override
    public void start() {
        context.callAsync(
                this::scanNextSnapshot, this::processDiscoveredSplits, 0, discoveryInterval);
    }

    @Override
    public void close() throws IOException {
        // no resources to close
    }

    @Override
    public void addReader(int subtaskId) {
        // this source is purely lazy-pull-based, nothing to do upon registration
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        readersAwaitingSplit.add(subtaskId);
        assignSplits();
        // if current task assigned no split, we check conditions to scan one more time
        if (readersAwaitingSplit.contains(subtaskId)) {
            if (stopTriggerScan) {
                return;
            }
            stopTriggerScan = true;
            context.callAsync(this::scanNextSnapshot, this::processDiscoveredSplits);
        }
    }

    @Override
    public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
        LOG.error("Received unrecognized event: {}", sourceEvent);
    }

    @Override
    public void addSplitsBack(List<TableAwareFileStoreSourceSplit> splits, int subtaskId) {
        LOG.debug("File Source Enumerator adds splits back: {}", splits);
        splitAssigner.addSplitsBack(subtaskId, new ArrayList<>(splits));
    }

    @Override
    public CDCCheckpoint snapshotState(long checkpointId) {
        Collection<TableAwareFileStoreSourceSplit> splits = splitAssigner.remainingSplits();

        Map<Identifier, Long> nextSnapshotIdMap = new HashMap<>();
        for (Map.Entry<Identifier, TableStatus> entry : tableStatusMap.entrySet()) {
            nextSnapshotIdMap.put(entry.getKey(), entry.getValue().nextSnapshotId);
        }
        final CDCCheckpoint checkpoint = new CDCCheckpoint(splits, nextSnapshotIdMap);

        LOG.debug("Source Checkpoint is {}", checkpoint);
        return checkpoint;
    }

    private synchronized Optional<TableAwarePlan> scanNextSnapshot() throws Exception {
        if (splitAssigner.numberOfRemainingSplits() >= splitMaxNum) {
            return Optional.empty();
        }

        discoverTables();

        if (tableStatusMap.isEmpty()) {
            return Optional.empty();
        }

        // Round-robin selection of tables
        List<Identifier> tableIdentifiers = new ArrayList<>(tableStatusMap.keySet());
        int index = currentTableIndex.getAndUpdate(i -> (i + 1) % tableIdentifiers.size());
        Identifier currentIdentifier = tableIdentifiers.get(index);
        StreamTableScan currentScan = tableStatusMap.get(currentIdentifier).scan;

        TableScan.Plan plan = currentScan.plan();
        return Optional.of(new TableAwarePlan(plan, currentIdentifier, currentScan.checkpoint()));
    }

    // this method could not be synchronized, because it runs in coordinatorThread, which will make
    // it serialize.
    private void processDiscoveredSplits(Optional<TableAwarePlan> planOptional, Throwable error) {
        if (error != null) {
            if (error instanceof EndOfScanException) {
                // finished
                LOG.debug("Catching EndOfStreamException, the stream is finished.");
                assignSplits();
            } else {
                LOG.error("Failed to enumerate files", error);
                throw new RuntimeException(error);
            }
            return;
        }

        if (!planOptional.isPresent()) {
            return;
        }
        TableAwarePlan tableAwarePlan = planOptional.get();
        TableScan.Plan plan = tableAwarePlan.plan;
        tableStatusMap.get(tableAwarePlan.identifier).nextSnapshotId =
                tableAwarePlan.nextSnapshotId;
        if (plan.equals(SnapshotNotExistPlan.INSTANCE)) {
            stopTriggerScan = true;
            return;
        }

        stopTriggerScan = false;
        if (plan.splits().isEmpty()) {
            return;
        }

        FileStoreTable table = tableStatusMap.get(tableAwarePlan.identifier).table;
        List<TableAwareFileStoreSourceSplit> splits = new ArrayList<>();
        for (Split split : plan.splits()) {
            Long lastSchemaId = tableStatusMap.get(tableAwarePlan.identifier).schemaId;
            TableAwareFileStoreSourceSplit tableAwareFileStoreSourceSplit =
                    toTableAwareSplit(
                            splitIdGenerator.getNextId(),
                            split,
                            table,
                            tableAwarePlan.identifier,
                            lastSchemaId);
            tableStatusMap.get(tableAwarePlan.identifier).schemaId =
                    tableAwareFileStoreSourceSplit.getSchemaId();
            splits.add(tableAwareFileStoreSourceSplit);
        }

        addSplits(splits);
        assignSplits();
    }

    @VisibleForTesting
    protected TableAwareFileStoreSourceSplit toTableAwareSplit(
            String splitId,
            Split split,
            FileStoreTable table,
            Identifier identifier,
            @Nullable Long lastSchemaId) {
        Preconditions.checkState(split instanceof DataSplit);
        long snapshotId = ((DataSplit) split).snapshotId();
        long schemaId = table.snapshot(snapshotId).schemaId();
        return new TableAwareFileStoreSourceSplit(
                splitId, split, 0, identifier, lastSchemaId, schemaId);
    }

    /**
     * Method should be synchronized because {@link #handleSplitRequest} and {@link
     * #processDiscoveredSplits} have thread conflicts.
     */
    protected synchronized void assignSplits() {
        // create assignment
        Map<Integer, List<TableAwareFileStoreSourceSplit>> assignment = new HashMap<>();
        Iterator<Integer> readersAwait = readersAwaitingSplit.iterator();
        Set<Integer> subtaskIds = context.registeredReaders().keySet();
        while (readersAwait.hasNext()) {
            Integer task = readersAwait.next();
            if (!subtaskIds.contains(task)) {
                readersAwait.remove();
                continue;
            }
            List<TableAwareFileStoreSourceSplit> splits = splitAssigner.getNext(task);
            if (!splits.isEmpty()) {
                assignment.put(task, splits);
            }
        }

        assignment.keySet().forEach(readersAwaitingSplit::remove);
        context.assignSplits(new SplitsAssignment<>(assignment));
    }

    protected int assignSuggestedTask(TableAwareFileStoreSourceSplit split) {
        if (tableStatusMap.get(split.getIdentifier()).subtaskId != null) {
            return tableStatusMap.get(split.getIdentifier()).subtaskId;
        }

        int subtaskId = numTablesWithSubtaskAssigned % context.currentParallelism();
        this.tableStatusMap.get(split.getIdentifier()).subtaskId = subtaskId;
        this.numTablesWithSubtaskAssigned++;
        LOG.info("Assigning table {} to subtask {}", split.getIdentifier(), subtaskId);
        return subtaskId;
    }

    private void discoverTables() throws Exception {
        if (System.currentTimeMillis() - lastTableDiscoveryTime < tableDiscoveryInterval) {
            return;
        }
        lastTableDiscoveryTime = System.currentTimeMillis();

        List<String> databaseNames;
        if (database == null || database.isEmpty()) {
            Preconditions.checkArgument(
                    table == null || table.isEmpty(),
                    "Tables should not be specified when databases is null. But tables is specified as "
                            + table);
            // If database parameter is not specified, dynamically scan all databases
            databaseNames = catalog.listDatabases();
        } else {
            databaseNames = Collections.singletonList(database);
        }

        for (String dbName : databaseNames) {
            List<String> tableNames;
            if (table == null || table.isEmpty()) {
                // If tables parameter is not specified but database is specified, dynamically scan
                // all tables in the database
                tableNames = catalog.listTables(dbName);
            } else {
                tableNames = Collections.singletonList(table);
            }

            for (String tableName : tableNames) {
                Identifier identifier = Identifier.create(dbName, tableName);
                if (!tableStatusMap.containsKey(identifier)
                        && !discoveredNonFileStoreTables.contains(identifier)) {
                    // New table found, add it to scans
                    Table table = catalog.getTable(identifier);
                    if (table instanceof FileStoreTable) {
                        tableStatusMap.put(
                                identifier, new TableStatus(context, (FileStoreTable) table));
                        LOG.info("Discovered new table {} and added to scan list", identifier);
                    } else if (!discoveredNonFileStoreTables.contains(identifier)) {
                        LOG.info(
                                "Discovered new table {}, but it is a {} instead of a FileStoreTable. Skipping this table.",
                                identifier,
                                table.getClass().getSimpleName());
                        discoveredNonFileStoreTables.add(identifier);
                    }
                }
            }
        }
    }

    @VisibleForTesting
    void setScan(Identifier identifier, FileStoreTable table, StreamDataTableScan scan) {
        tableStatusMap.put(identifier, new TableStatus(table, scan));
    }

    private static class TableAwarePlan {
        private final TableScan.Plan plan;
        private final Identifier identifier;
        private final Long nextSnapshotId;

        private TableAwarePlan(TableScan.Plan plan, Identifier identifier, Long nextSnapshotId) {
            this.plan = plan;
            this.identifier = identifier;
            this.nextSnapshotId = nextSnapshotId;
        }
    }

    private static class TableStatus {
        private final FileStoreTable table;
        private final StreamDataTableScan scan;
        private Long schemaId;
        private Integer subtaskId;
        private Long nextSnapshotId;

        private TableStatus(SplitEnumeratorContext<?> context, FileStoreTable table) {
            this.table = table;
            this.scan = table.newStreamScan();
            if (metricGroup(context) != null) {
                this.scan.withMetricRegistry(new FlinkMetricRegistry(context.metricGroup()));
            }
        }

        @VisibleForTesting
        private TableStatus(FileStoreTable table, StreamDataTableScan scan) {
            this.table = table;
            this.scan = scan;
        }

        private void restore(Long nextSnapshotId) {
            this.subtaskId = null;
            this.nextSnapshotId = nextSnapshotId;
            this.scan.restore(nextSnapshotId);
        }

        @Nullable
        private SplitEnumeratorMetricGroup metricGroup(SplitEnumeratorContext<?> context) {
            try {
                return context.metricGroup();
            } catch (NullPointerException ignore) {
                // ignore NPE for some Flink versions
                return null;
            }
        }
    }

    private static class SplitIdGenerator {
        private final String uuid = UUID.randomUUID().toString();
        private final AtomicInteger idCounter = new AtomicInteger(1);

        private String getNextId() {
            return uuid + "-" + idCounter.getAndIncrement();
        }
    }
}
