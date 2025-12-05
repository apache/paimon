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

package org.apache.paimon.table.source;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.data.variant.VariantAccessInfo;
import org.apache.paimon.data.variant.VariantAccessInfoUtils;
import org.apache.paimon.globalindex.GlobalIndexScanBuilder;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.predicate.TopN;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.InnerTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.Range;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static org.apache.paimon.globalindex.GlobalIndexScanBuilder.parallelScan;
import static org.apache.paimon.globalindex.GlobalIndexScanBuilder.scan;
import static org.apache.paimon.partition.PartitionPredicate.createPartitionPredicate;
import static org.apache.paimon.partition.PartitionPredicate.fromPredicate;
import static org.apache.paimon.utils.Preconditions.checkState;

/** Implementation for {@link ReadBuilder}. */
public class ReadBuilderImpl implements ReadBuilder {

    private static final long serialVersionUID = 1L;

    private final InnerTable table;
    private final RowType partitionType;
    private final String defaultPartitionName;

    private Predicate filter;

    private Integer limit = null;
    private TopN topN = null;

    private Integer shardIndexOfThisSubtask;
    private Integer shardNumberOfParallelSubtasks;

    private @Nullable PartitionPredicate partitionFilter;

    private @Nullable Integer specifiedBucket = null;
    private Filter<Integer> bucketFilter;

    private @Nullable RowType readType;
    private @Nullable VariantAccessInfo[] variantAccessInfo;
    private @Nullable List<Range> rowRanges;

    private boolean dropStats = false;

    public ReadBuilderImpl(InnerTable table) {
        this.table = table;
        this.partitionType = table.rowType().project(table.partitionKeys());
        this.defaultPartitionName = new CoreOptions(table.options()).partitionDefaultName();
    }

    @Override
    public String tableName() {
        return table.name();
    }

    @Override
    public RowType readType() {
        RowType finalReadType = readType != null ? readType : table.rowType();
        // When variantAccessInfo is not null, replace the variant with the actual readType.
        if (variantAccessInfo != null) {
            finalReadType =
                    VariantAccessInfoUtils.buildReadRowType(finalReadType, variantAccessInfo);
        }
        return finalReadType;
    }

    @Override
    public ReadBuilder withFilter(Predicate filter) {
        if (this.filter == null) {
            this.filter = filter;
        } else {
            this.filter = PredicateBuilder.and(this.filter, filter);
        }
        return this;
    }

    @Override
    public ReadBuilder withPartitionFilter(Map<String, String> partitionSpec) {
        if (partitionSpec != null) {
            this.partitionFilter =
                    fromPredicate(
                            partitionType,
                            createPartitionPredicate(
                                    partitionSpec, partitionType, defaultPartitionName));
        }
        return this;
    }

    @Override
    public ReadBuilder withPartitionFilter(@Nullable PartitionPredicate partitionPredicate) {
        this.partitionFilter = partitionPredicate;
        return this;
    }

    @Override
    public ReadBuilder withReadType(RowType readType) {
        this.readType = readType;
        return this;
    }

    @Override
    public ReadBuilder withVariantAccess(VariantAccessInfo[] variantAccessInfo) {
        this.variantAccessInfo = variantAccessInfo;
        return this;
    }

    @Override
    public ReadBuilder withProjection(int[] projection) {
        if (projection == null) {
            return this;
        }
        return withReadType(table.rowType().project(projection));
    }

    @Override
    public ReadBuilder withLimit(int limit) {
        this.limit = limit;
        return this;
    }

    @Override
    public ReadBuilder withTopN(TopN topN) {
        this.topN = topN;
        return this;
    }

    @Override
    public ReadBuilder withShard(int indexOfThisSubtask, int numberOfParallelSubtasks) {
        this.shardIndexOfThisSubtask = indexOfThisSubtask;
        this.shardNumberOfParallelSubtasks = numberOfParallelSubtasks;
        return this;
    }

    @Override
    public ReadBuilder withRowRanges(List<Range> indices) {
        this.rowRanges = indices;
        return this;
    }

    @Override
    public ReadBuilder withBucket(int bucket) {
        this.specifiedBucket = bucket;
        return this;
    }

    @Override
    public ReadBuilder withBucketFilter(Filter<Integer> bucketFilter) {
        this.bucketFilter = bucketFilter;
        return this;
    }

    @Override
    public ReadBuilder dropStats() {
        this.dropStats = true;
        return this;
    }

    @Override
    public TableScan newScan() {
        InnerTableScan tableScan = configureScan(table.newScan());
        if (limit != null) {
            tableScan.withLimit(limit);
        }
        if (topN != null) {
            tableScan.withTopN(topN);
        }
        return tableScan;
    }

    @Override
    public StreamTableScan newStreamScan() {
        return (StreamTableScan) configureScan(table.newStreamScan());
    }

    private InnerTableScan configureScan(InnerTableScan scan) {
        // `filter` may contains partition related predicate, but `partitionFilter` will overwrite
        // it if `partitionFilter` is not null. So we must avoid to put part of partition filter in
        // `filter`, another part in `partitionFilter`
        scan.withFilter(filter).withReadType(readType).withPartitionFilter(partitionFilter);

        // please configure this after filter and partitionFilter are set
        configureGlobalIndex(scan);

        checkState(
                bucketFilter == null || shardIndexOfThisSubtask == null,
                "Bucket filter and shard configuration cannot be used together. "
                        + "Please choose one method to specify the data subset.");
        if (shardIndexOfThisSubtask != null) {
            if (scan instanceof DataTableScan) {
                return ((DataTableScan) scan)
                        .withShard(shardIndexOfThisSubtask, shardNumberOfParallelSubtasks);
            } else {
                throw new UnsupportedOperationException(
                        "Unsupported table scan type for shard configuring, the scan is: " + scan);
            }
        }
        if (specifiedBucket != null) {
            scan.withBucket(specifiedBucket);
        }
        if (bucketFilter != null) {
            scan.withBucketFilter(bucketFilter);
        }
        if (dropStats) {
            scan.dropStats();
        }
        return scan;
    }

    private void configureGlobalIndex(InnerTableScan scan) {
        if (rowRanges == null && filter != null && searchGlobalIndex(table)) {
            FileStoreTable fileStoreTable = (FileStoreTable) table;
            PartitionPredicate partitionPredicate = scan.partitionFilter();
            GlobalIndexScanBuilder globalIndexScanBuilder = fileStoreTable.newIndexScanBuilder();
            Snapshot snapshot = fileStoreTable.snapshotManager().latestSnapshot();
            globalIndexScanBuilder
                    .withPartitionPredicate(partitionPredicate)
                    .withSnapshot(snapshot);
            List<Range> indexedRowRanges = globalIndexScanBuilder.shardList();
            if (!indexedRowRanges.isEmpty()) {
                List<Range> nonIndexedRowRanges =
                        new Range(0, snapshot.nextRowId() - 1).exclude(indexedRowRanges);
                Optional<List<Range>> combined =
                        parallelScan(indexedRowRanges, globalIndexScanBuilder, filter);
                if (combined.isPresent()) {
                    List<Range> finalResult = new ArrayList<>(combined.get());
                    if (!nonIndexedRowRanges.isEmpty()) {
                        finalResult.addAll(nonIndexedRowRanges);
                        finalResult.sort(Comparator.comparingLong(f -> f.from));
                    }
                    this.rowRanges = finalResult;
                }
            }
        }

        if (this.rowRanges != null) {
            scan.withRowRanges(this.rowRanges);
        }
    }

    private boolean searchGlobalIndex(Table table) {
        return table instanceof FileStoreTable
                && ((FileStoreTable) table).coreOptions().dataEvolutionEnabled()
                && ((FileStoreTable) table).coreOptions().globalIndexEnabledInScan();
    }

    @Override
    public TableRead newRead() {
        InnerTableRead read = table.newRead().withFilter(filter);
        if (readType != null) {
            read.withReadType(readType);
        }
        if (topN != null) {
            read.withTopN(topN);
        }
        if (limit != null) {
            read.withLimit(limit);
        }
        if (rowRanges != null) {
            read.withRowRanges(rowRanges);
        }
        if (variantAccessInfo != null) {
            read.withVariantAccess(variantAccessInfo);
        }
        return read;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ReadBuilderImpl that = (ReadBuilderImpl) o;
        return Objects.equals(table.name(), that.table.name())
                && Objects.equals(filter, that.filter)
                && Objects.equals(partitionFilter, that.partitionFilter)
                && Objects.equals(readType, that.readType);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(table.name(), filter);
        result = 31 * result + Objects.hash(readType);
        return result;
    }
}
