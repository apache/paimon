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

import org.apache.paimon.flink.PaimonDataStreamSinkProvider;
import org.apache.paimon.table.FormatTable;

import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.abilities.SupportsPartitioning;
import org.apache.flink.table.factories.DynamicTableFactory;

import java.util.HashMap;
import java.util.Map;

/** Table sink for format tables. */
public class FlinkFormatTableSink implements DynamicTableSink, SupportsPartitioning {

    private final ObjectIdentifier tableIdentifier;
    private final FormatTable table;
    private final DynamicTableFactory.Context context;
    private Map<String, String> staticPartitions = new HashMap<>();

    public FlinkFormatTableSink(
            ObjectIdentifier tableIdentifier,
            FormatTable table,
            DynamicTableFactory.Context context) {
        this.tableIdentifier = tableIdentifier;
        this.table = table;
        this.context = context;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return ChangelogMode.insertOnly();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        return new PaimonDataStreamSinkProvider(
                (dataStream) -> new FlinkFormatTableDataStreamSink(table).sinkFrom(dataStream));
    }

    @Override
    public DynamicTableSink copy() {
        FlinkFormatTableSink copied = new FlinkFormatTableSink(tableIdentifier, table, context);
        copied.staticPartitions = new HashMap<>(staticPartitions);
        return copied;
    }

    @Override
    public String asSummaryString() {
        return "Paimon-FormatTable";
    }

    @Override
    public void applyStaticPartition(Map<String, String> partition) {
        table.partitionKeys()
                .forEach(
                        partitionKey -> {
                            if (partition.containsKey(partitionKey)) {
                                this.staticPartitions.put(
                                        partitionKey, partition.get(partitionKey));
                            }
                        });
    }
}
