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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.flink.FlinkRowData;
import org.apache.paimon.flink.FlinkRowWrapper;
import org.apache.paimon.flink.utils.TableScanUtils;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateFilter;
import org.apache.paimon.shade.guava30.com.google.common.primitives.Ints;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileIOUtils;
import org.apache.paimon.utils.TypeUtils;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;
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
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.paimon.flink.RocksDBOptions.LOOKUP_CACHE_ROWS;
import static org.apache.paimon.predicate.PredicateBuilder.transformFieldMapping;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** A lookup {@link TableFunction} for file store. */
public class FileStoreLookupFunction implements Serializable, Closeable {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(FileStoreLookupFunction.class);

    private static final int ASYNC_THREAD_POOL_SIZE = 16;

    private final Table table;
    private final List<String> projectFields;
    private final List<String> joinKeys;
    @Nullable private final Predicate predicate;

    private transient Duration refreshInterval;
    private transient File path;
    private transient RocksDBStateFactory stateFactory;
    private transient LookupTable lookupTable;

    // timestamp when cache expires
    private transient volatile long nextLoadTime;

    private transient TableStreamingReader streamingReader;

    private transient Boolean lookupAsync;

    private transient Lock lock;

    private transient ExecutorService asyncThreadPool;

    private volatile long time = 0L;

    public FileStoreLookupFunction(
            Table table, int[] projection, int[] joinKeyIndex, @Nullable Predicate predicate) {
        checkArgument(
                table.partitionKeys().isEmpty(),
                String.format(
                        "Currently only support non-partitioned table, the lookup table is [%s].",
                        table.name()));
        checkArgument(
                table.primaryKeys().size() > 0,
                String.format(
                        "Currently only support primary key table, the lookup table is [%s].",
                        table.name()));
        TableScanUtils.streamingReadingValidate(table);

        this.table = table;

        // join keys are based on projection fields
        this.joinKeys =
                Arrays.stream(joinKeyIndex)
                        .mapToObj(i -> table.rowType().getFieldNames().get(projection[i]))
                        .collect(Collectors.toList());

        this.projectFields =
                Arrays.stream(projection)
                        .mapToObj(i -> table.rowType().getFieldNames().get(i))
                        .collect(Collectors.toList());

        // add primary keys
        for (String field : table.primaryKeys()) {
            if (!projectFields.contains(field)) {
                projectFields.add(field);
            }
        }

        this.predicate = predicate;
    }

    public void open(FunctionContext context) throws Exception {
        Options options = Options.fromMap(table.options());

        this.lookupAsync = options.get(FlinkConnectorOptions.LOOKUP_ASYNC);
        if (lookupAsync) {
            this.lock = new ReentrantLock();
            this.asyncThreadPool =
                    Executors.newFixedThreadPool(
                            ASYNC_THREAD_POOL_SIZE,
                            new ExecutorThreadFactory("paimon-async-lookup-worker"));
        }

        String tmpDirectory = getTmpDirectory(context, lookupAsync);
        this.path = new File(tmpDirectory, "lookup-" + UUID.randomUUID());

        this.refreshInterval = options.get(CoreOptions.CONTINUOUS_DISCOVERY_INTERVAL);
        this.stateFactory = new RocksDBStateFactory(path.toString(), options);

        List<String> fieldNames = table.rowType().getFieldNames();
        int[] projection = projectFields.stream().mapToInt(fieldNames::indexOf).toArray();
        RowType rowType = TypeUtils.project(table.rowType(), projection);

        PredicateFilter recordFilter = createRecordFilter(projection);
        this.lookupTable =
                LookupTable.create(
                        stateFactory,
                        rowType,
                        table.primaryKeys(),
                        joinKeys,
                        recordFilter,
                        options.get(LOOKUP_CACHE_ROWS));
        this.nextLoadTime = -1;
        this.streamingReader = new TableStreamingReader(table, projection, this.predicate);

        // do first load
        refresh();
    }

    private PredicateFilter createRecordFilter(int[] projection) {
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
        return new PredicateFilter(
                TypeUtils.project(table.rowType(), projection), adjustedPredicate);
    }

    public boolean isAsyncEnabled() {
        if (lookupAsync == null) {
            this.lookupAsync =
                    Boolean.valueOf(table.options().get(FlinkConnectorOptions.LOOKUP_ASYNC.key()));
        }
        return lookupAsync;
    }

    public Collection<RowData> lookup(RowData keyRow) {
        try {
            checkRefresh();
            return convertRowData(lookupTable.get(new FlinkRowWrapper(keyRow)));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public CompletableFuture<Collection<RowData>> asyncLookup(RowData keyRow) {
        return CompletableFuture.supplyAsync(
                () -> {
                    try {
                        checkRefresh();
                        // TODO introduce retry logic.
                        return convertRowData(lookupTable.get(new FlinkRowWrapper(keyRow)));
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                },
                asyncThreadPool);
    }

    private void checkRefresh() throws Exception {
        if (nextLoadTime > System.currentTimeMillis()) {
            return;
        }
        if (isAsyncEnabled()) {
            // since we would check asynchronously, introduce a lock here to avoid concurrent
            // refresh.
            lock.lock();
            try {
                if (nextLoadTime <= System.currentTimeMillis()) {
                    refreshAndUpdateNextLoadTime();
                }
            } finally {
                lock.unlock();
            }
        } else {
            refreshAndUpdateNextLoadTime();
        }
    }

    private void refreshAndUpdateNextLoadTime() throws Exception {
        if (nextLoadTime > 0) {
            LOG.info(
                    "Lookup table has refreshed after {} second(s), refreshing",
                    refreshInterval.toMillis() / 1000.0);
        }

        refresh();

        nextLoadTime = System.currentTimeMillis() + refreshInterval.toMillis();
    }

    private void refresh() throws Exception {
        while (true) {
            Iterator<InternalRow> batch = streamingReader.nextBatch();
            if (!batch.hasNext()) {
                return;
            }
            this.lookupTable.refresh(batch);
        }
    }

    private Collection<RowData> convertRowData(Collection<InternalRow> internalRows) {
        List<RowData> rows = new ArrayList<>(internalRows.size());
        for (InternalRow matchedRow : internalRows) {
            rows.add(new FlinkRowData(matchedRow));
        }
        return rows;
    }

    @Override
    public void close() throws IOException {
        if (stateFactory != null) {
            stateFactory.close();
            stateFactory = null;
        }

        if (path != null) {
            FileIOUtils.deleteDirectoryQuietly(path);
        }
    }

    private static String getTmpDirectory(FunctionContext context, boolean isAsyncContext) {
        try {
            Field field = context.getClass().getDeclaredField("context");
            field.setAccessible(true);
            RuntimeContext runtimeContext = (RuntimeContext) field.get(context);
            StreamingRuntimeContext streamingRuntimeContext;
            // for async lookup, the runtimeContext is wrapped in RichAsyncFunctionRuntimeContext
            if (isAsyncContext) {
                Class<?> innerClass =
                        Class.forName(
                                "org.apache.flink.streaming.api.functions.async.RichAsyncFunction$RichAsyncFunctionRuntimeContext");
                Field innerField = innerClass.getDeclaredField("runtimeContext");
                innerField.setAccessible(true);
                streamingRuntimeContext =
                        (StreamingRuntimeContext) innerField.get(innerField.get(runtimeContext));
            } else {
                streamingRuntimeContext = (StreamingRuntimeContext) runtimeContext;
            }
            String[] tmpDirectories =
                    streamingRuntimeContext.getTaskManagerRuntimeInfo().getTmpDirectories();
            return tmpDirectories[ThreadLocalRandom.current().nextInt(tmpDirectories.length)];
        } catch (ClassNotFoundException | NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }
}
