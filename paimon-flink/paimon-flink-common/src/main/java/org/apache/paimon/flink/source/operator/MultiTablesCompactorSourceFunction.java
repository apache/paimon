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

package org.apache.paimon.flink.source.operator;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.StreamTableScan;
import org.apache.paimon.table.system.BucketsMultiTable;

import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.OptionalLong;
import java.util.TreeMap;
import java.util.regex.Pattern;

import static org.apache.paimon.flink.utils.MultiTablesCompactorUtil.compactOptions;
import static org.apache.paimon.flink.utils.MultiTablesCompactorUtil.shouldCompactionTable;

/** this is a doc. */
public abstract class MultiTablesCompactorSourceFunction
        extends RichSourceFunction<Tuple2<Split, String>>
        implements CheckpointedFunction, CheckpointListener {
    private static final long serialVersionUID = 1L;

    private static final Logger LOG =
            LoggerFactory.getLogger(MultiTablesCompactorSourceFunction.class);

    protected final Catalog.Loader catalogLoader;
    protected final Pattern includingPattern;
    protected final Pattern excludingPattern;
    protected final Pattern databasePattern;
    protected final boolean isStreaming;
    protected final long monitorInterval;

    protected transient Catalog catalog;
    protected transient Map<Identifier, BucketsMultiTable> tablesMap;
    protected transient Map<Identifier, StreamTableScan> scansMap;

    public MultiTablesCompactorSourceFunction(
            Catalog.Loader catalogLoader,
            Pattern includingPattern,
            Pattern excludingPattern,
            Pattern databasePattern,
            boolean isStreaming,
            long monitorInterval) {
        this.catalogLoader = catalogLoader;
        this.includingPattern = includingPattern;
        this.excludingPattern = excludingPattern;
        this.databasePattern = databasePattern;
        this.isStreaming = isStreaming;
        this.monitorInterval = monitorInterval;
    }

    protected volatile boolean isRunning = true;

    protected transient SourceContext<Tuple2<Split, String>> ctx;

    protected transient ListState<Long> checkpointState;
    protected transient ListState<Tuple2<Long, Long>> nextSnapshotState;
    protected transient TreeMap<Long, Long> nextSnapshotPerCheckpoint;

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        tablesMap = new HashMap<>();
        scansMap = new HashMap<>();

        catalog = catalogLoader.load();

        updateTableMap();

        this.checkpointState =
                context.getOperatorStateStore()
                        .getListState(
                                new ListStateDescriptor<>(
                                        "next-snapshot", LongSerializer.INSTANCE));

        @SuppressWarnings("unchecked")
        final Class<Tuple2<Long, Long>> typedTuple =
                (Class<Tuple2<Long, Long>>) (Class<?>) Tuple2.class;
        this.nextSnapshotState =
                context.getOperatorStateStore()
                        .getListState(
                                new ListStateDescriptor<>(
                                        "next-snapshot-per-checkpoint",
                                        new TupleSerializer<>(
                                                typedTuple,
                                                new TypeSerializer[] {
                                                    LongSerializer.INSTANCE, LongSerializer.INSTANCE
                                                })));

        this.nextSnapshotPerCheckpoint = new TreeMap<>();

        if (context.isRestored()) {
            LOG.info("Restoring state for the {}.", getClass().getSimpleName());

            List<Long> retrievedStates = new ArrayList<>();
            for (Long entry : this.checkpointState.get()) {
                retrievedStates.add(entry);
            }

            // given that the parallelism of the function is 1, we can only have 1 retrieved items.

            Preconditions.checkArgument(
                    retrievedStates.size() <= 1,
                    getClass().getSimpleName() + " retrieved invalid state.");

            if (retrievedStates.size() == 1) {
                for (StreamTableScan scan : this.scansMap.values()) {
                    scan.restore(retrievedStates.get(0));
                }
            }

            for (Tuple2<Long, Long> tuple2 : nextSnapshotState.get()) {
                nextSnapshotPerCheckpoint.put(tuple2.f0, tuple2.f1);
            }
        } else {
            LOG.info("No state to restore for the {}.", getClass().getSimpleName());
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext ctx) throws Exception {
        this.checkpointState.clear();
        // TODO: how to get nextSnapshot better?
        Long nextSnapshot = this.scansMap.values().iterator().next().checkpoint();
        if (nextSnapshot != null) {
            this.checkpointState.add(nextSnapshot);
            this.nextSnapshotPerCheckpoint.put(ctx.getCheckpointId(), nextSnapshot);
        }

        List<Tuple2<Long, Long>> nextSnapshots = new ArrayList<>();
        this.nextSnapshotPerCheckpoint.forEach((k, v) -> nextSnapshots.add(new Tuple2<>(k, v)));
        this.nextSnapshotState.update(nextSnapshots);

        if (LOG.isDebugEnabled()) {
            LOG.debug("{} checkpoint {}.", getClass().getSimpleName(), nextSnapshot);
        }
    }

    @Override
    public void run(SourceContext<Tuple2<Split, String>> ctx) throws Exception {}

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        NavigableMap<Long, Long> nextSnapshots =
                nextSnapshotPerCheckpoint.headMap(checkpointId, true);
        OptionalLong max = nextSnapshots.values().stream().mapToLong(Long::longValue).max();
        // // TODO: scansMap.values().iterator().next()?
        max.ifPresent(scansMap.values().iterator().next()::notifyCheckpointComplete);
        nextSnapshots.clear();
    }

    @Override
    public void cancel() {
        // this is to cover the case where cancel() is called before the run()
        if (ctx != null) {
            synchronized (ctx.getCheckpointLock()) {
                isRunning = false;
            }
        } else {
            isRunning = false;
        }
    }

    protected void updateTableMap()
            throws Catalog.DatabaseNotExistException, Catalog.TableNotExistException {
        List<String> databases = catalog.listDatabases();

        for (String databaseName : databases) {
            if (databasePattern.matcher(databaseName).matches()) {
                List<String> tables = catalog.listTables(databaseName);
                for (String tableName : tables) {
                    Identifier identifier = Identifier.create(databaseName, tableName);
                    if (shouldCompactionTable(identifier, includingPattern, excludingPattern)
                            && (!tablesMap.containsKey(identifier))) {
                        Table table = catalog.getTable(identifier);
                        if (!(table instanceof FileStoreTable)) {
                            LOG.error(
                                    String.format(
                                            "Only FileStoreTable supports compact action. The table type is '%s'.",
                                            table.getClass().getName()));
                            continue;
                        }
                        FileStoreTable fileStoreTable = (FileStoreTable) table;
                        if (fileStoreTable.bucketMode() == BucketMode.UNAWARE) {
                            LOG.info(
                                    String.format(
                                                    "the bucket mode of %s is unware. ",
                                                    identifier.getFullName())
                                            + "currently, the table with unware bucket mode is not support in combined mode.");
                            continue;
                        }
                        BucketsMultiTable bucketsTable =
                                new BucketsMultiTable(
                                                fileStoreTable,
                                                isStreaming,
                                                identifier.getDatabaseName(),
                                                identifier.getObjectName())
                                        .copy(compactOptions(isStreaming));
                        tablesMap.put(identifier, bucketsTable);
                        scansMap.put(identifier, bucketsTable.newReadBuilder().newStreamScan());
                    }
                }
            }
        }
    }
}
