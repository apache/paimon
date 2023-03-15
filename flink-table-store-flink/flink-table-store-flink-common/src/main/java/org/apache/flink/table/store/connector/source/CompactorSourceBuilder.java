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

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.store.connector.LogicalTypeConversion;
import org.apache.flink.table.store.file.predicate.Predicate;
import org.apache.flink.table.store.file.predicate.PredicateBuilder;
import org.apache.flink.table.store.table.FileStoreTable;
import org.apache.flink.table.store.table.source.StreamDataTableScan;
import org.apache.flink.table.store.table.source.snapshot.ContinuousCompactorFollowUpScanner;
import org.apache.flink.table.store.table.source.snapshot.ContinuousCompactorStartingScanner;
import org.apache.flink.table.store.table.source.snapshot.FullStartingScanner;
import org.apache.flink.table.store.table.system.BucketsTable;
import org.apache.flink.table.store.types.RowType;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Source builder to build a Flink {@link StaticFileStoreSource} or {@link
 * ContinuousFileStoreSource}. This is for dedicated compactor jobs.
 */
public class CompactorSourceBuilder {

    private final String tableIdentifier;
    private final FileStoreTable table;

    private boolean isContinuous = false;
    private StreamExecutionEnvironment env;
    @Nullable private List<Map<String, String>> specifiedPartitions = null;

    public CompactorSourceBuilder(String tableIdentifier, FileStoreTable table) {
        this.tableIdentifier = tableIdentifier;
        this.table = table;
    }

    public CompactorSourceBuilder withContinuousMode(boolean isContinuous) {
        this.isContinuous = isContinuous;
        return this;
    }

    public CompactorSourceBuilder withEnv(StreamExecutionEnvironment env) {
        this.env = env;
        return this;
    }

    public CompactorSourceBuilder withPartition(Map<String, String> partition) {
        return withPartitions(Collections.singletonList(partition));
    }

    public CompactorSourceBuilder withPartitions(List<Map<String, String>> partitions) {
        this.specifiedPartitions = partitions;
        return this;
    }

    private Source<RowData, ?, ?> buildSource(BucketsTable bucketsTable) {
        Predicate partitionPredicate = null;
        if (specifiedPartitions != null) {
            // This predicate is based on the row type of the original table, not bucket table.
            // Because TableScan in BucketsTable is the same with FileStoreTable,
            // and partition filter is done by scan.
            partitionPredicate =
                    PredicateBuilder.or(
                            specifiedPartitions.stream()
                                    .map(p -> PredicateBuilder.partition(p, table.rowType()))
                                    .toArray(Predicate[]::new));
        }

        if (isContinuous) {
            return new ContinuousFileStoreSource(
                    bucketsTable,
                    null,
                    partitionPredicate,
                    null,
                    (table, nextSnapshotId) -> {
                        StreamDataTableScan scan =
                                table.newStreamScan()
                                        .withStartingScanner(
                                                new ContinuousCompactorStartingScanner())
                                        .withFollowUpScanner(
                                                new ContinuousCompactorFollowUpScanner());
                        scan.restore(nextSnapshotId);
                        return scan;
                    });
        } else {
            return new StaticFileStoreSource(
                    bucketsTable,
                    null,
                    partitionPredicate,
                    null,
                    table -> table.newScan().withStartingScanner(new FullStartingScanner()));
        }
    }

    public DataStreamSource<RowData> build() {
        if (env == null) {
            throw new IllegalArgumentException("StreamExecutionEnvironment should not be null.");
        }

        BucketsTable bucketsTable = new BucketsTable(table, isContinuous);
        RowType produceType = bucketsTable.rowType();
        return env.fromSource(
                buildSource(bucketsTable),
                WatermarkStrategy.noWatermarks(),
                tableIdentifier + "-compact-source",
                InternalTypeInfo.of(LogicalTypeConversion.toLogicalType(produceType)));
    }
}
