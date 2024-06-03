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

package org.apache.paimon.flink.sink;

import org.apache.paimon.append.AppendOnlyCompactionTask;
import org.apache.paimon.append.MultiTableAppendOnlyCompactionTask;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.compact.UnawareBucketCompactor;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.ExceptionUtils;
import org.apache.paimon.utils.ExecutorThreadFactory;

import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Operator to execute {@link AppendOnlyCompactionTask} passed for support compacting multi unaware
 * bucket tables in combined mode.
 */
public class AppendOnlyMultiTableCompactionWorkerOperator
        extends PrepareCommitOperator<MultiTableAppendOnlyCompactionTask, MultiTableCommittable> {

    private static final Logger LOG =
            LoggerFactory.getLogger(AppendOnlyMultiTableCompactionWorkerOperator.class);

    private final String commitUser;
    private final Catalog.Loader catalogLoader;

    // support multi table compaction
    private transient Map<Identifier, UnawareBucketCompactor> compactorContainer;

    private transient ExecutorService lazyCompactExecutor;

    private transient Catalog catalog;

    public AppendOnlyMultiTableCompactionWorkerOperator(
            Catalog.Loader catalogLoader, String commitUser, Options options) {
        super(options);
        this.commitUser = commitUser;
        this.catalogLoader = catalogLoader;
    }

    @Override
    public void open() throws Exception {
        LOG.debug("Opened a append-only multi table compaction worker.");
        compactorContainer = new HashMap<>();
        catalog = catalogLoader.load();
    }

    @Override
    protected List<MultiTableCommittable> prepareCommit(boolean waitCompaction, long checkpointId)
            throws IOException {
        List<MultiTableCommittable> result = new ArrayList<>();
        for (Map.Entry<Identifier, UnawareBucketCompactor> compactorWithTable :
                compactorContainer.entrySet()) {
            Identifier tableId = compactorWithTable.getKey();
            UnawareBucketCompactor compactor = compactorWithTable.getValue();

            for (Committable committable : compactor.prepareCommit(waitCompaction, checkpointId)) {
                result.add(
                        new MultiTableCommittable(
                                tableId.getDatabaseName(),
                                tableId.getObjectName(),
                                committable.checkpointId(),
                                committable.kind(),
                                committable.wrappedCommittable()));
            }
        }

        return result;
    }

    @Override
    public void processElement(StreamRecord<MultiTableAppendOnlyCompactionTask> element)
            throws Exception {
        Identifier identifier = element.getValue().tableIdentifier();
        compactorContainer
                .computeIfAbsent(identifier, this::compactor)
                .processElement(element.getValue());
    }

    private UnawareBucketCompactor compactor(Identifier tableId) {
        try {
            return new UnawareBucketCompactor(
                    (FileStoreTable) catalog.getTable(tableId), commitUser, this::workerExecutor);
        } catch (Catalog.TableNotExistException e) {
            throw new RuntimeException(e);
        }
    }

    private ExecutorService workerExecutor() {
        if (lazyCompactExecutor == null) {
            lazyCompactExecutor =
                    Executors.newSingleThreadScheduledExecutor(
                            new ExecutorThreadFactory(
                                    Thread.currentThread().getName()
                                            + "-append-only-compact-worker"));
        }
        return lazyCompactExecutor;
    }

    @Override
    public void close() throws Exception {
        List<Exception> exceptions = new ArrayList<>();

        if (lazyCompactExecutor != null) {
            try {
                lazyCompactExecutor.shutdownNow();
                if (!lazyCompactExecutor.awaitTermination(120, TimeUnit.SECONDS)) {
                    LOG.warn(
                            "Executors shutdown timeout, there may be some files that aren't deleted correctly");
                }
            } catch (Exception e) {
                LOG.warn(
                        String.format(
                                "Fail to stop the compaction executor. Reason: %s, please check the thread stack of append-only-compact-worker.",
                                e.getMessage()),
                        e);
                exceptions.add(e);
            }

            for (Map.Entry<Identifier, UnawareBucketCompactor> compactorEntry :
                    compactorContainer.entrySet()) {
                try {
                    UnawareBucketCompactor compactor = compactorEntry.getValue();
                    compactor.close();
                } catch (Exception e) {
                    Identifier id = compactorEntry.getKey();
                    LOG.warn(
                            String.format(
                                    "Fail to roll back the compactor of %s. Reason: %s",
                                    id, e.getMessage()),
                            e);
                    exceptions.add(e);
                }
            }
        }

        ExceptionUtils.throwMultiException(exceptions);
    }
}
