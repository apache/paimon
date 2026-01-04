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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.metrics.MetricRegistry;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.TopN;
import org.apache.paimon.predicate.VectorSearch;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.Range;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;

/** Inner {@link TableScan} contains filter push down. */
public interface InnerTableScan extends TableScan {

    InnerTableScan withFilter(Predicate predicate);

    default InnerTableScan withVectorSearch(VectorSearch vectorSearch) {
        return this;
    }

    default InnerTableScan withReadType(@Nullable RowType readType) {
        return this;
    }

    default InnerTableScan withLimit(int limit) {
        return this;
    }

    default InnerTableScan withPartitionFilter(Map<String, String> partitionSpec) {
        return this;
    }

    default InnerTableScan withPartitionsFilter(List<Map<String, String>> partitions) {
        return this;
    }

    default InnerTableScan withPartitionFilter(List<BinaryRow> partitions) {
        return this;
    }

    default InnerTableScan withPartitionFilter(PartitionPredicate partitionPredicate) {
        return this;
    }

    default InnerTableScan withPartitionFilter(Predicate predicate) {
        return this;
    }

    default InnerTableScan withRowRanges(List<Range> rowRanges) {
        return this;
    }

    default InnerTableScan withBucket(int bucket) {
        return this;
    }

    default InnerTableScan withBucketFilter(Filter<Integer> bucketFilter) {
        return this;
    }

    default InnerTableScan withLevelFilter(Filter<Integer> levelFilter) {
        return this;
    }

    @Override
    default InnerTableScan withMetricRegistry(MetricRegistry metricRegistry) {
        // do nothing, should implement this if need
        return this;
    }

    default InnerTableScan withTopN(TopN topN) {
        return this;
    }

    default InnerTableScan dropStats() {
        // do nothing, should implement this if need
        return this;
    }
}
