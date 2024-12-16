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

import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.table.InnerTable;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Filter;

import javax.annotation.Nullable;

import java.util.Map;
import java.util.Objects;

import static org.apache.paimon.utils.Preconditions.checkState;

/** Implementation for {@link ReadBuilder}. */
public class ReadBuilderImpl implements ReadBuilder {

    private static final long serialVersionUID = 1L;

    private final InnerTable table;

    private Predicate filter;

    private Integer limit = null;

    private Integer shardIndexOfThisSubtask;
    private Integer shardNumberOfParallelSubtasks;

    private Map<String, String> partitionSpec;

    private Filter<Integer> bucketFilter;

    private @Nullable RowType readType;

    private boolean dropStats = false;

    public ReadBuilderImpl(InnerTable table) {
        this.table = table;
    }

    @Override
    public String tableName() {
        return table.name();
    }

    @Override
    public RowType readType() {
        if (readType != null) {
            return readType;
        } else {
            return table.rowType();
        }
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
        this.partitionSpec = partitionSpec;
        return this;
    }

    @Override
    public ReadBuilder withReadType(RowType readType) {
        RowType tableRowType = table.rowType();
        checkState(
                readType.isPrunedFrom(tableRowType),
                "read row type must be a pruned type from table row type, read row type: %s, table row type: %s",
                readType,
                tableRowType);
        this.readType = readType;
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
    public ReadBuilder withShard(int indexOfThisSubtask, int numberOfParallelSubtasks) {
        this.shardIndexOfThisSubtask = indexOfThisSubtask;
        this.shardNumberOfParallelSubtasks = numberOfParallelSubtasks;
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
        return tableScan;
    }

    @Override
    public StreamTableScan newStreamScan() {
        return (StreamTableScan) configureScan(table.newStreamScan());
    }

    private InnerTableScan configureScan(InnerTableScan scan) {
        scan.withFilter(filter).withPartitionFilter(partitionSpec);
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
        if (bucketFilter != null) {
            scan.withBucketFilter(bucketFilter);
        }
        if (dropStats) {
            scan.dropStats();
        }
        return scan;
    }

    @Override
    public TableRead newRead() {
        InnerTableRead read = table.newRead().withFilter(filter);
        if (readType != null) {
            read.withReadType(readType);
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
                && Objects.equals(readType, that.readType);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(table.name(), filter);
        result = 31 * result + Objects.hash(readType);
        return result;
    }
}
