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

package org.apache.paimon.flink.lookup;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.JoinedRow;
import org.apache.paimon.flink.FlinkConnectorOptions.LookupCacheMode;
import org.apache.paimon.flink.FlinkRowData;
import org.apache.paimon.flink.FlinkRowWrapper;
import org.apache.paimon.flink.utils.TableScanUtils;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.OutOfRangeException;
import org.apache.paimon.utils.FileIOUtils;
import org.apache.paimon.utils.Filter;

import org.apache.paimon.shade.guava30.com.google.common.primitives.Ints;

import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.paimon.CoreOptions.CONTINUOUS_DISCOVERY_INTERVAL;
import static org.apache.paimon.flink.FlinkConnectorOptions.LOOKUP_CACHE_MODE;
import static org.apache.paimon.flink.FlinkConnectorOptions.LOOKUP_REFRESH_TIME_PERIODS_BLACKLIST;
import static org.apache.paimon.flink.query.RemoteTableQuery.isRemoteServiceAvailable;
import static org.apache.paimon.lookup.RocksDBOptions.LOOKUP_CACHE_ROWS;
import static org.apache.paimon.lookup.RocksDBOptions.LOOKUP_CONTINUOUS_DISCOVERY_INTERVAL;
import static org.apache.paimon.predicate.PredicateBuilder.transformFieldMapping;

/** A lookup {@link TableFunction} for file store. */
public class FileStoreLookupFunction implements Serializable, Closeable {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(FileStoreLookupFunction.class);

    private final Table table;
    @Nullable private final DynamicPartitionLoader partitionLoader;
    private final List<String> projectFields;
    private final List<String> joinKeys;
    @Nullable private final Predicate predicate;
    @Nullable private final RefreshBlacklist refreshBlacklist;

    private final List<InternalRow.FieldGetter> projectFieldsGetters;

    private transient File path;
    private transient LookupTable lookupTable;

    // interval of refreshing lookup table
    private transient Duration refreshInterval;
    // timestamp when refreshing lookup table
    private transient long nextRefreshTime;

    protected FunctionContext functionContext;

    @Nullable private Filter<InternalRow> cacheRowFilter;

    public FileStoreLookupFunction(
            Table table, int[] projection, int[] joinKeyIndex, @Nullable Predicate predicate) {
        if (!TableScanUtils.supportCompactDiffStreamingReading(table)) {
            TableScanUtils.streamingReadingValidate(table);
        }

        this.table = table;
        this.partitionLoader = DynamicPartitionLoader.of(table);

        // join keys are based on projection fields
        this.joinKeys =
                Arrays.stream(joinKeyIndex)
                        .mapToObj(i -> table.rowType().getFieldNames().get(projection[i]))
                        .collect(Collectors.toList());

        this.projectFields =
                Arrays.stream(projection)
                        .mapToObj(i -> table.rowType().getFieldNames().get(i))
                        .collect(Collectors.toList());

        this.projectFieldsGetters =
                Arrays.stream(projection)
                        .mapToObj(i -> table.rowType().fieldGetters()[i])
                        .collect(Collectors.toList());

        // add primary keys
        for (String field : table.primaryKeys()) {
            if (!projectFields.contains(field)) {
                projectFields.add(field);
            }
        }

        if (partitionLoader != null) {
            partitionLoader.addPartitionKeysTo(joinKeys, projectFields);
        }

        this.predicate = predicate;

        this.refreshBlacklist =
                RefreshBlacklist.create(
                        table.options().get(LOOKUP_REFRESH_TIME_PERIODS_BLACKLIST.key()));
    }

    public void open(FunctionContext context) throws Exception {
        this.functionContext = context;
        String tmpDirectory = getTmpDirectory(context);
        open(tmpDirectory);
    }

    // we tag this method friendly for testing
    void open(String tmpDirectory) throws Exception {
        this.path = new File(tmpDirectory, "lookup-" + UUID.randomUUID());
        if (!path.mkdirs()) {
            throw new RuntimeException("Failed to create dir: " + path);
        }
        open();
    }

    private void open() throws Exception {
        this.nextRefreshTime = -1;

        Options options = Options.fromMap(table.options());
        this.refreshInterval =
                options.getOptional(LOOKUP_CONTINUOUS_DISCOVERY_INTERVAL)
                        .orElse(options.get(CONTINUOUS_DISCOVERY_INTERVAL));

        List<String> fieldNames = table.rowType().getFieldNames();
        int[] projection = projectFields.stream().mapToInt(fieldNames::indexOf).toArray();
        LOG.info(
                "lookup projection fields in lookup table:{}, join fields in lookup table:{}",
                projectFields,
                joinKeys);

        FileStoreTable storeTable = (FileStoreTable) table;

        LOG.info("Creating lookup table for {}.", table.name());
        if (options.get(LOOKUP_CACHE_MODE) == LookupCacheMode.AUTO
                && new HashSet<>(table.primaryKeys()).equals(new HashSet<>(joinKeys))) {
            if (isRemoteServiceAvailable(storeTable)) {
                this.lookupTable =
                        PrimaryKeyPartialLookupTable.createRemoteTable(
                                storeTable, projection, joinKeys);
                LOG.info(
                        "Remote service is available. Created PrimaryKeyPartialLookupTable with remote service.");
            } else {
                try {
                    this.lookupTable =
                            PrimaryKeyPartialLookupTable.createLocalTable(
                                    storeTable,
                                    projection,
                                    path,
                                    joinKeys,
                                    getRequireCachedBucketIds());
                    LOG.info(
                            "Remote service isn't available. Created PrimaryKeyPartialLookupTable with LocalQueryExecutor.");
                } catch (UnsupportedOperationException ignore) {
                    LOG.info(
                            "Remote service isn't available. Cannot create PrimaryKeyPartialLookupTable with LocalQueryExecutor "
                                    + "because bucket mode isn't {}. Will create FullCacheLookupTable.",
                            BucketMode.HASH_FIXED);
                }
            }
        }

        if (lookupTable == null) {
            FullCacheLookupTable.Context context =
                    new FullCacheLookupTable.Context(
                            storeTable,
                            projection,
                            predicate,
                            createProjectedPredicate(projection),
                            path,
                            joinKeys,
                            getRequireCachedBucketIds());
            this.lookupTable = FullCacheLookupTable.create(context, options.get(LOOKUP_CACHE_ROWS));
            LOG.info("Created {}.", lookupTable.getClass().getSimpleName());
        }

        if (partitionLoader != null) {
            partitionLoader.open();
            partitionLoader.checkRefresh();
            List<BinaryRow> partitions = partitionLoader.partitions();
            if (!partitions.isEmpty()) {
                lookupTable.specificPartitionFilter(partitionLoader.createSpecificPartFilter());
            }
        }

        if (cacheRowFilter != null) {
            lookupTable.specifyCacheRowFilter(cacheRowFilter);
        }
        lookupTable.open();
    }

    @Nullable
    private Predicate createProjectedPredicate(int[] projection) {
        Predicate adjustedPredicate = null;
        if (predicate != null) {
            // adjust to projection index
            adjustedPredicate =
                    transformFieldMapping(
                                    this.predicate,
                                    IntStream.range(0, table.rowType().getFieldCount())
                                            .map(i -> Ints.indexOf(projection, i))
                                            .toArray())
                            .orElse(null);
        }
        return adjustedPredicate;
    }

    public Collection<RowData> lookup(RowData keyRow) {
        try {
            tryRefresh();

            if (LOG.isDebugEnabled()) {
                LOG.debug("lookup key:{}", keyRow.toString());
            }
            InternalRow key = new FlinkRowWrapper(keyRow);
            if (partitionLoader == null) {
                return lookupInternal(key);
            }

            if (partitionLoader.partitions().isEmpty()) {
                return Collections.emptyList();
            }

            List<RowData> rows = new ArrayList<>();
            for (BinaryRow partition : partitionLoader.partitions()) {
                rows.addAll(lookupInternal(JoinedRow.join(key, partition)));
            }
            return rows;
        } catch (OutOfRangeException | ReopenException e) {
            reopen();
            return lookup(keyRow);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private List<RowData> lookupInternal(InternalRow key) throws IOException {
        List<RowData> rows = new ArrayList<>();
        List<InternalRow> lookupResults = lookupTable.get(key);
        for (InternalRow matchedRow : lookupResults) {
            rows.add(new FlinkRowData(matchedRow));
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "matched rows in lookup table, size:{}, rows:{}",
                    lookupResults.size(),
                    lookupResults.stream()
                            .map(row -> logRow(projectFieldsGetters, row))
                            .collect(Collectors.toList()));
        }

        return rows;
    }

    private void reopen() {
        try {
            close();
            open();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @VisibleForTesting
    void tryRefresh() throws Exception {
        // 1. check if this time is in black list
        if (refreshBlacklist != null && !refreshBlacklist.canRefresh()) {
            return;
        }

        // 2. refresh dynamic partition
        if (partitionLoader != null) {
            boolean partitionChanged = partitionLoader.checkRefresh();
            List<BinaryRow> partitions = partitionLoader.partitions();
            if (partitions.isEmpty()) {
                // no data to be load, fast exit
                return;
            }

            if (partitionChanged) {
                // reopen with latest partition
                lookupTable.specificPartitionFilter(partitionLoader.createSpecificPartFilter());
                lookupTable.close();
                lookupTable.open();
                // no need to refresh the lookup table because it is reopened
                return;
            }
        }

        // 3. refresh lookup table
        if (shouldRefreshLookupTable()) {
            lookupTable.refresh();
            nextRefreshTime = System.currentTimeMillis() + refreshInterval.toMillis();
        }
    }

    private boolean shouldRefreshLookupTable() {
        if (nextRefreshTime > System.currentTimeMillis()) {
            return false;
        }

        if (nextRefreshTime > 0) {
            LOG.info(
                    "Lookup table {} has refreshed after {} second(s), refreshing",
                    table.name(),
                    refreshInterval.toMillis() / 1000);
        }
        return true;
    }

    @VisibleForTesting
    LookupTable lookupTable() {
        return lookupTable;
    }

    @VisibleForTesting
    long nextBlacklistCheckTime() {
        return refreshBlacklist == null ? -1 : refreshBlacklist.nextBlacklistCheckTime();
    }

    @Override
    public void close() throws IOException {
        if (lookupTable != null) {
            lookupTable.close();
            lookupTable = null;
        }

        if (path != null) {
            FileIOUtils.deleteDirectoryQuietly(path);
        }
    }

    private static String getTmpDirectory(FunctionContext context) {
        try {
            Field field = context.getClass().getDeclaredField("context");
            field.setAccessible(true);
            StreamingRuntimeContext runtimeContext =
                    extractStreamingRuntimeContext(field.get(context));
            String[] tmpDirectories =
                    runtimeContext.getTaskManagerRuntimeInfo().getTmpDirectories();
            return tmpDirectories[ThreadLocalRandom.current().nextInt(tmpDirectories.length)];
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private static StreamingRuntimeContext extractStreamingRuntimeContext(Object runtimeContext)
            throws NoSuchFieldException, IllegalAccessException {
        if (runtimeContext instanceof StreamingRuntimeContext) {
            return (StreamingRuntimeContext) runtimeContext;
        }

        Field field = runtimeContext.getClass().getDeclaredField("runtimeContext");
        field.setAccessible(true);
        return extractStreamingRuntimeContext(field.get(runtimeContext));
    }

    /**
     * Get the set of bucket IDs that need to be cached by the current lookup join subtask.
     *
     * <p>The Flink Planner will distribute data to lookup join nodes based on buckets. This allows
     * paimon to cache only the necessary buckets for each subtask, improving efficiency.
     *
     * @return the set of bucket IDs to be cached
     */
    protected Set<Integer> getRequireCachedBucketIds() {
        // TODO: Implement the method when Flink support bucket shuffle for lookup join.
        return null;
    }

    protected void setCacheRowFilter(@Nullable Filter<InternalRow> cacheRowFilter) {
        this.cacheRowFilter = cacheRowFilter;
    }

    private String logRow(List<InternalRow.FieldGetter> fieldGetters, InternalRow row) {
        List<String> rowValues = new ArrayList<>(fieldGetters.size());

        for (InternalRow.FieldGetter fieldGetter : fieldGetters) {
            Object fieldValue = fieldGetter.getFieldOrNull(row);
            String value = fieldValue == null ? "null" : fieldValue.toString();
            rowValues.add(value);
        }
        return rowValues.toString();
    }
}
