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

package org.apache.paimon.spark.read;

import org.apache.paimon.globalindex.VectorSearchMetric;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.InstantiationUtil;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RoaringNavigableMap64;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.paimon.CoreOptions.GLOBAL_INDEX_THREAD_NUM;

/** Shared split grouping and serialization for Spark-dispatched vector search. */
final class SparkVectorSearchSupport {

    private SparkVectorSearchSupport() {}

    static int parallelism(FileStoreTable table) {
        return Math.max(1, table.coreOptions().toConfiguration().get(GLOBAL_INDEX_THREAD_NUM));
    }

    static SparkEngineContext engineContext() {
        return new SparkEngineContext();
    }

    static <T> List<List<T>> splitGroups(List<T> items, int parallelism) {
        List<List<T>> groups = new ArrayList<>(parallelism);
        int groupSize = (items.size() + parallelism - 1) / parallelism;
        for (int start = 0; start < items.size(); start += groupSize) {
            groups.add(
                    new ArrayList<>(
                            items.subList(start, Math.min(start + groupSize, items.size()))));
        }
        return groups;
    }

    static List<List<Range>> rangeGroups(List<Range> ranges, int parallelism) {
        long rowCount = rawRowCount(ranges);
        int groupCount = (int) Math.max(1, Math.min(parallelism, rowCount));
        long targetRowsPerGroup = (rowCount - 1) / groupCount + 1;

        List<List<Range>> groups = new ArrayList<>(groupCount);
        List<Range> currentGroup = new ArrayList<>();
        long currentRows = 0;
        for (Range range : ranges) {
            long from = range.from;
            while (from <= range.to) {
                if (currentRows == targetRowsPerGroup) {
                    groups.add(currentGroup);
                    currentGroup = new ArrayList<>();
                    currentRows = 0;
                }
                long remainingGroupRows = targetRowsPerGroup - currentRows;
                long to = Math.min(range.to, from + remainingGroupRows - 1);
                Range next = new Range(from, to);
                currentGroup.add(next);
                currentRows += next.count();
                from = to + 1;
            }
        }
        if (!currentGroup.isEmpty()) {
            groups.add(currentGroup);
        }
        return groups;
    }

    static <T> T deserialize(byte[] bytes, String failureMessage) {
        try {
            return InstantiationUtil.deserializeObject(
                    bytes, Thread.currentThread().getContextClassLoader());
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(failureMessage, e);
        }
    }

    @Nullable
    static RoaringNavigableMap64 deserializePreFilter(@Nullable byte[] bytes) {
        if (bytes == null) {
            return null;
        }
        return deserialize(bytes, "Failed to deserialize vector pre-filter");
    }

    static long rawRowCount(List<Range> ranges) {
        long rowCount = 0;
        for (Range range : ranges) {
            long count = range.count();
            if (Long.MAX_VALUE - rowCount < count) {
                return Long.MAX_VALUE;
            }
            rowCount += count;
        }
        return rowCount;
    }

    /**
     * One metric learned by executor indexers, or null when no shard reader was opened. Different
     * persisted metrics cannot be merged.
     */
    @Nullable
    static String resolveVectorMetric(List<String> metrics) {
        String resolved = null;
        for (String metric : metrics) {
            if (metric == null) {
                continue;
            }
            String normalized = VectorSearchMetric.normalize(metric);
            if (resolved == null) {
                resolved = normalized;
            } else if (!resolved.equals(normalized)) {
                throw new IllegalArgumentException(
                        "Cannot combine vector index shards with different vector metrics: "
                                + resolved
                                + " and "
                                + normalized
                                + ".");
            }
        }
        return resolved;
    }

    static final class BatchIndexGroupResult implements java.io.Serializable {

        private static final long serialVersionUID = 1L;

        @Nullable final byte[][] results;
        @Nullable final String metric;

        BatchIndexGroupResult(@Nullable byte[][] results, @Nullable String metric) {
            this.results = results;
            this.metric = metric;
        }
    }

    static final class SingleIndexGroupResult implements java.io.Serializable {

        private static final long serialVersionUID = 1L;

        @Nullable final byte[] result;
        @Nullable final String metric;

        SingleIndexGroupResult(@Nullable byte[] result, @Nullable String metric) {
            this.result = result;
            this.metric = metric;
        }
    }

    static final class SerializedSplit implements java.io.Serializable {

        private static final long serialVersionUID = 1L;

        final byte[] split;
        @Nullable final byte[] preFilter;

        SerializedSplit(byte[] split, @Nullable byte[] preFilter) {
            this.split = split;
            this.preFilter = preFilter;
        }
    }
}
