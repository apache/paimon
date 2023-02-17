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

package org.apache.flink.table.store.connector.source;

import org.apache.flink.api.connector.source.Source;
import org.apache.flink.connector.base.source.hybrid.HybridSource;
import org.apache.flink.connector.base.source.hybrid.HybridSource.SourceFactory;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.store.file.Snapshot;
import org.apache.flink.table.store.log.LogSourceProvider;

import java.util.Map;

/** Log {@link SourceFactory} from {@link StaticFileStoreSplitEnumerator}. */
public class LogHybridSourceFactory
        implements SourceFactory<RowData, Source<RowData, ?, ?>, StaticFileStoreSplitEnumerator> {

    private final LogSourceProvider provider;

    public LogHybridSourceFactory(LogSourceProvider provider) {
        this.provider = provider;
    }

    @Override
    public Source<RowData, ?, ?> create(
            HybridSource.SourceSwitchContext<StaticFileStoreSplitEnumerator> context) {
        StaticFileStoreSplitEnumerator enumerator = context.getPreviousEnumerator();
        Snapshot snapshot = enumerator.snapshot();
        Map<Integer, Long> logOffsets = null;
        if (snapshot != null) {
            logOffsets = snapshot.logOffsets();
        }
        return provider.createSource(logOffsets);
    }
}
