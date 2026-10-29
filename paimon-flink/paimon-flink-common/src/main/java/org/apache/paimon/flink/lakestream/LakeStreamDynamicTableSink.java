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

import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.RowLevelModificationScanContext;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.abilities.SupportsPartitioning;
import org.apache.flink.table.connector.sink.abilities.SupportsRowLevelDelete;
import org.apache.flink.table.connector.sink.abilities.SupportsRowLevelUpdate;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;

/**
 * Runs a regular write on the Lakestream Store's own sink and refuses row-level UPDATE and DELETE.
 *
 * <p>A modification reads the table to find the rows it applies to, and on a lake-stream table that
 * read resolves to Paimon snapshots while the write goes to the store. The two halves would
 * therefore act on different views: rows the store has not tiered yet are invisible to the scan,
 * and an update computed from a scanned value can be computed from a stale one. Neither is
 * detectable from the sink, so the statement is refused rather than served from the wrong view.
 *
 * <p>{@code SupportsOverwrite} and {@code SupportsDeletePushDown} are deliberately not exposed. The
 * former keeps INSERT OVERWRITE off the store until it is routed to Paimon; withholding the latter
 * makes a DELETE fall back to the row-level path above instead of being pushed into the store,
 * where it would only reach the rows the store happens to hold.
 */
public class LakeStreamDynamicTableSink
        implements DynamicTableSink,
                SupportsPartitioning,
                SupportsRowLevelUpdate,
                SupportsRowLevelDelete {

    private final DynamicTableSink storeSink;
    private final ObjectIdentifier identifier;

    public LakeStreamDynamicTableSink(DynamicTableSink storeSink, ObjectIdentifier identifier) {
        this.storeSink = storeSink;
        this.identifier = identifier;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return storeSink.getChangelogMode(requestedMode);
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        return storeSink.getSinkRuntimeProvider(context);
    }

    @Override
    public DynamicTableSink copy() {
        return new LakeStreamDynamicTableSink(storeSink.copy(), identifier);
    }

    @Override
    public String asSummaryString() {
        return storeSink.asSummaryString();
    }

    @Override
    public void applyStaticPartition(Map<String, String> partition) {
        if (!(storeSink instanceof SupportsPartitioning)) {
            throw new UnsupportedOperationException(
                    String.format(
                            "The Lakestream Store sink for '%s' does not support static partitions.",
                            identifier.asSummaryString()));
        }
        ((SupportsPartitioning) storeSink).applyStaticPartition(partition);
    }

    @Override
    public RowLevelUpdateInfo applyRowLevelUpdate(
            List<Column> updatedColumns, @Nullable RowLevelModificationScanContext context) {
        throw modificationNotSupported("UPDATE");
    }

    @Override
    public RowLevelDeleteInfo applyRowLevelDelete(
            @Nullable RowLevelModificationScanContext context) {
        throw modificationNotSupported("DELETE");
    }

    private UnsupportedOperationException modificationNotSupported(String statement) {
        return new UnsupportedOperationException(
                String.format(
                        "%s on lake-stream table '%s' is not supported yet: the statement reads "
                                + "Paimon snapshots while the modification is written to the "
                                + "Lakestream Store, so it could miss rows that have not been tiered "
                                + "into Paimon or compute from stale values.",
                        statement, identifier.asSummaryString()));
    }
}
