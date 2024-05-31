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

package org.apache.paimon.flink.sorter;

import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.flink.sorter.TableSorter.OrderType;

import java.util.Collections;
import java.util.List;

import static org.apache.flink.shaded.guava31.com.google.common.base.Preconditions.checkArgument;
import static org.apache.flink.shaded.guava31.com.google.common.base.Preconditions.checkNotNull;

/**
 * {@link TableSortInfo} is used to indicate the configuration details for table data sorting. This
 * includes information about which columns to sort by, the sorting strategy (e.g., order, Z-order),
 * whether to sort within each cluster, and sample sizes for local and global sample nodes.
 */
public class TableSortInfo {

    private final List<String> sortColumns;

    private final OrderType sortStrategy;

    private final boolean sortInCluster;

    private final int rangeNumber;

    private final int sinkParallelism;

    private final int localSampleSize;

    private final int globalSampleSize;

    private TableSortInfo(
            List<String> sortColumns,
            OrderType sortStrategy,
            boolean sortInCluster,
            int rangeNumber,
            int sinkParallelism,
            int localSampleSize,
            int globalSampleSize) {
        this.sortColumns = sortColumns;
        this.sortStrategy = sortStrategy;
        this.sortInCluster = sortInCluster;
        this.rangeNumber = rangeNumber;
        this.sinkParallelism = sinkParallelism;
        this.localSampleSize = localSampleSize;
        this.globalSampleSize = globalSampleSize;
    }

    public List<String> getSortColumns() {
        return sortColumns;
    }

    public OrderType getSortStrategy() {
        return sortStrategy;
    }

    public boolean isSortInCluster() {
        return sortInCluster;
    }

    public int getRangeNumber() {
        return rangeNumber;
    }

    public int getLocalSampleSize() {
        return localSampleSize;
    }

    public int getGlobalSampleSize() {
        return globalSampleSize;
    }

    public int getSinkParallelism() {
        return sinkParallelism;
    }

    /** Builder for {@link TableSortInfo}. */
    public static class Builder {

        private List<String> sortColumns = Collections.emptyList();

        private OrderType sortStrategy = OrderType.ORDER;

        private boolean sortInCluster = true;

        private int rangeNumber = -1;

        private int sinkParallelism = -1;

        private int localSampleSize = -1;

        private int globalSampleSize = -1;

        public Builder setSortColumns(List<String> sortColumns) {
            this.sortColumns = sortColumns;
            return this;
        }

        public Builder setSortStrategy(OrderType sortStrategy) {
            this.sortStrategy = sortStrategy;
            return this;
        }

        public Builder setSortInCluster(boolean sortInCluster) {
            this.sortInCluster = sortInCluster;
            return this;
        }

        public Builder setRangeNumber(int rangeNumber) {
            this.rangeNumber = rangeNumber;
            return this;
        }

        public Builder setSinkParallelism(int sinkParallelism) {
            this.sinkParallelism = sinkParallelism;
            return this;
        }

        public Builder setLocalSampleSize(int localSampleSize) {
            this.localSampleSize = localSampleSize;
            return this;
        }

        public Builder setGlobalSampleSize(int globalSampleSize) {
            this.globalSampleSize = globalSampleSize;
            return this;
        }

        public TableSortInfo build() {
            checkArgument(!sortColumns.isEmpty(), "Sort columns cannot be empty");
            checkNotNull(sortStrategy, "Sort strategy cannot be null");
            checkArgument(
                    sinkParallelism > 0,
                    "The sink parallelism must be specified when sorting the table data. Please set it using the key: %s",
                    FlinkConnectorOptions.SINK_PARALLELISM.key());
            checkArgument(rangeNumber > 0, "Range number must be positive");
            checkArgument(localSampleSize > 0, "Local sample size must be positive");
            checkArgument(globalSampleSize > 0, "Global sample size must be positive");
            return new TableSortInfo(
                    sortColumns,
                    sortStrategy,
                    sortInCluster,
                    rangeNumber,
                    sinkParallelism,
                    localSampleSize,
                    globalSampleSize);
        }
    }
}
