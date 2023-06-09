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

package org.apache.paimon.flink.source;

import org.apache.paimon.append.AppendOnlyCompactionTask;
import org.apache.paimon.append.AppendOnlyTableCompactionCoordinator;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.AppendOnlyFileStoreTable;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

/** Source Function for unaware-bucket Compaction. */
public class UnawareBucketSourceFunction extends RichSourceFunction<AppendOnlyCompactionTask> {

    private final AppendOnlyFileStoreTable table;
    private transient AppendOnlyTableCompactionCoordinator compactionCoordinator;
    private transient boolean closed = false;
    private boolean streaming;
    private long scanInterval = 10_000;
    private Predicate filter;

    public UnawareBucketSourceFunction(AppendOnlyFileStoreTable table) {
        this.table = table;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        compactionCoordinator = new AppendOnlyTableCompactionCoordinator(table);
        if (filter != null) {
            compactionCoordinator.withFilter(filter);
        }
    }

    @Override
    public void run(SourceContext<AppendOnlyCompactionTask> sourceContext) throws Exception {
        // scan loop
        do {
            // do scan and plan action, emit append-only compaction tasks.
            compactionCoordinator.run().forEach(sourceContext::collect);

            // sleep for scanInterval
            Thread.sleep(scanInterval);
        } while (!closed && streaming);
    }

    public void withStreaming(boolean streaming) {
        this.streaming = streaming;
    }

    public void withScanInterval(long scanInterval) {
        this.scanInterval = scanInterval;
    }

    public void withFilter(Predicate filter) {
        // we can filter partitions here
        this.filter = filter;
    }

    @Override
    public void cancel() {
        closed = true;
    }
}
