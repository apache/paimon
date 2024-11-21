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

import org.apache.paimon.append.UnawareAppendCompactionTask;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.table.source.Split;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

/**
 * This is the single (non-parallel) monitoring task, it is responsible for:
 *
 * <ol>
 *   <li>Monitoring snapshots of the Paimon table.
 *   <li>Creating the splits {@link Split} or compaction task {@link UnawareAppendCompactionTask}
 *       corresponding to the incremental files
 *   <li>Assigning them to downstream tasks for further processing.
 * </ol>
 *
 * <p>The splits to be read are forwarded to the downstream {@link MultiTablesReadOperator} which
 * can have parallelism greater than one.
 *
 * <p>Currently, only dedicated compaction job for multi-tables rely on this monitor. This is the
 * single (non-parallel) monitoring task, it is responsible for the new Paimon table.
 */
public abstract class CombinedCompactorSourceFunction<T> extends RichSourceFunction<T> {

    private static final long serialVersionUID = 2L;

    protected final Catalog.Loader catalogLoader;
    protected final Pattern includingPattern;
    protected final Pattern excludingPattern;
    protected final Pattern databasePattern;
    protected final boolean isStreaming;

    protected transient AtomicBoolean isRunning;
    protected transient SourceContext<T> ctx;

    public CombinedCompactorSourceFunction(
            Catalog.Loader catalogLoader,
            Pattern includingPattern,
            Pattern excludingPattern,
            Pattern databasePattern,
            boolean isStreaming) {
        this.catalogLoader = catalogLoader;
        this.includingPattern = includingPattern;
        this.excludingPattern = excludingPattern;
        this.databasePattern = databasePattern;
        this.isStreaming = isStreaming;
    }

    /**
     * Do not annotate with <code>@override</code> here to maintain compatibility with Flink 1.18-.
     */
    public void open(OpenContext openContext) throws Exception {
        open(new Configuration());
    }

    /**
     * Do not annotate with <code>@override</code> here to maintain compatibility with Flink 2.0+.
     */
    public void open(Configuration parameters) throws Exception {
        isRunning = new AtomicBoolean(true);
    }

    @Override
    public void run(SourceContext<T> sourceContext) throws Exception {
        this.ctx = sourceContext;
        scanTable();
    }

    @Override
    public void cancel() {
        // this is to cover the case where cancel() is called before the run()
        if (ctx != null) {
            synchronized (ctx.getCheckpointLock()) {
                isRunning.set(false);
            }
        } else {
            isRunning.set(false);
        }
    }

    abstract void scanTable() throws Exception;
}
