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

package org.apache.paimon.flink.lakestream;

import org.apache.flink.api.connector.sink2.InitContext;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.RowLevelModificationScanContext;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.connector.sink.abilities.SupportsDeletePushDown;
import org.apache.flink.table.connector.sink.abilities.SupportsPartitioning;
import org.apache.flink.table.connector.sink.abilities.SupportsRowLevelDelete;
import org.apache.flink.table.connector.sink.abilities.SupportsRowLevelDelete.RowLevelDeleteInfo;
import org.apache.flink.table.connector.sink.abilities.SupportsRowLevelUpdate;
import org.apache.flink.table.connector.sink.abilities.SupportsRowLevelUpdate.RowLevelUpdateInfo;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.expressions.ResolvedExpression;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * A runnable fake lake-stream store {@link DynamicTableSink}. Rows written through it are appended
 * to {@link FakeStoreData}; ability callbacks ({@code applyStaticPartition} and the row-level ones)
 * are recorded statically so tests can assert what the planner applied to the store's sink.
 *
 * <p>Deliberately not a {@code SupportsOverwrite}: a real store's sink does not implement it
 * either, which is what makes an {@code INSERT OVERWRITE} on a lake-stream table fail.
 */
public class FakeStoreSink
        implements DynamicTableSink,
                SupportsPartitioning,
                SupportsRowLevelDelete,
                SupportsRowLevelUpdate,
                SupportsDeletePushDown {

    private static final List<String> ABILITY_CALLS =
            Collections.synchronizedList(new ArrayList<String>());

    private final String tableKey;

    public FakeStoreSink(String tableKey) {
        this.tableKey = tableKey;
    }

    /** Recorded ability callbacks, e.g. {@code applyStaticPartition} / {@code executeDeletion}. */
    public static List<String> abilityCalls() {
        synchronized (ABILITY_CALLS) {
            return new ArrayList<>(ABILITY_CALLS);
        }
    }

    public static void resetAbilityCalls() {
        ABILITY_CALLS.clear();
    }

    public String tableKey() {
        return tableKey;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return requestedMode;
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        return SinkV2Provider.of(new FakeStoreWriteSink(tableKey));
    }

    @Override
    public DynamicTableSink copy() {
        return new FakeStoreSink(tableKey);
    }

    @Override
    public String asSummaryString() {
        return "FakeStoreSink";
    }

    @Override
    public void applyStaticPartition(Map<String, String> staticPartition) {
        ABILITY_CALLS.add("applyStaticPartition(" + staticPartition + ")");
    }

    @Override
    public RowLevelDeleteInfo applyRowLevelDelete(
            @Nullable RowLevelModificationScanContext context) {
        ABILITY_CALLS.add("applyRowLevelDelete");
        return new RowLevelDeleteInfo() {};
    }

    @Override
    public RowLevelUpdateInfo applyRowLevelUpdate(
            List<Column> updatedColumns, @Nullable RowLevelModificationScanContext context) {
        ABILITY_CALLS.add("applyRowLevelUpdate(" + updatedColumns.size() + ")");
        return new RowLevelUpdateInfo() {};
    }

    @Override
    public boolean applyDeleteFilters(List<ResolvedExpression> filters) {
        ABILITY_CALLS.add("applyDeleteFilters(" + filters.size() + ")");
        return false;
    }

    @Override
    public Optional<Long> executeDeletion() {
        ABILITY_CALLS.add("executeDeletion");
        return Optional.empty();
    }

    /** The sink2 runtime sink that appends to {@link FakeStoreData}. */
    static class FakeStoreWriteSink implements Sink<RowData> {

        private static final long serialVersionUID = 1L;

        private final String tableKey;

        FakeStoreWriteSink(String tableKey) {
            this.tableKey = tableKey;
        }

        /**
         * Do not annotate with <code>@override</code> here to maintain compatibility with Flink
         * 2.0+.
         */
        public SinkWriter<RowData> createWriter(InitContext context) {
            return new SinkWriter<RowData>() {
                @Override
                public void write(RowData element, Context context) {
                    FakeStoreData.add(tableKey, element);
                }

                @Override
                public void flush(boolean endOfInput) {}

                @Override
                public void close() {}
            };
        }

        /**
         * Do not annotate with <code>@override</code> here to maintain compatibility with Flink
         * 1.18-.
         */
        public SinkWriter<RowData> createWriter(WriterInitContext context) {
            return new SinkWriter<RowData>() {
                @Override
                public void write(RowData element, Context context) {
                    FakeStoreData.add(tableKey, element);
                }

                @Override
                public void flush(boolean endOfInput) {}

                @Override
                public void close() {}
            };
        }
    }
}
