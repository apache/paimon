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

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

/** The unit test for {@link TableSortInfo}. */
public class TableSortInfoTest {

    @Test
    public void testTableSortInfoBuilderWithValidParameters() {
        TableSortInfo tableSortInfo =
                new TableSortInfo.Builder()
                        .setSortColumns(Arrays.asList("column1", "column2"))
                        .setSortStrategy(OrderType.ORDER)
                        .setSortInCluster(true)
                        .setRangeNumber(10)
                        .setSinkParallelism(5)
                        .setLocalSampleSize(100)
                        .setGlobalSampleSize(500)
                        .build();

        assertThat(tableSortInfo.getSortColumns()).containsExactly("column1", "column2");
        assertThat(tableSortInfo.getSortStrategy()).isEqualTo(OrderType.ORDER);
        assertThat(tableSortInfo.isSortInCluster()).isTrue();
        assertThat(tableSortInfo.getRangeNumber()).isEqualTo(10);
        assertThat(tableSortInfo.getSinkParallelism()).isEqualTo(5);
        assertThat(tableSortInfo.getLocalSampleSize()).isEqualTo(100);
        assertThat(tableSortInfo.getGlobalSampleSize()).isEqualTo(500);
    }

    @Test
    public void testTableSortInfoBuilderWithEmptySortColumns() {
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(
                        () ->
                                new TableSortInfo.Builder()
                                        .setSortColumns(Collections.emptyList())
                                        .setSortStrategy(OrderType.ORDER)
                                        .setSortInCluster(true)
                                        .setRangeNumber(10)
                                        .setSinkParallelism(5)
                                        .setLocalSampleSize(100)
                                        .setGlobalSampleSize(500)
                                        .build())
                .withMessage("Sort columns cannot be empty");
    }

    @Test
    public void testTableSortInfoBuilderWithNullSortStrategy() {
        assertThatExceptionOfType(NullPointerException.class)
                .isThrownBy(
                        () ->
                                new TableSortInfo.Builder()
                                        .setSortColumns(Arrays.asList("column1", "column2"))
                                        .setSortStrategy(null)
                                        .setSortInCluster(true)
                                        .setRangeNumber(10)
                                        .setSinkParallelism(5)
                                        .setLocalSampleSize(100)
                                        .setGlobalSampleSize(500)
                                        .build())
                .withMessage("Sort strategy cannot be null");
    }

    @Test
    public void testTableSortInfoBuilderWithNegativeRangeNumber() {
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(
                        () ->
                                new TableSortInfo.Builder()
                                        .setSortColumns(Arrays.asList("column1", "column2"))
                                        .setSortStrategy(OrderType.ORDER)
                                        .setSortInCluster(true)
                                        .setRangeNumber(-1)
                                        .setSinkParallelism(5)
                                        .setLocalSampleSize(100)
                                        .setGlobalSampleSize(500)
                                        .build())
                .withMessage("Range number must be positive");
    }

    @Test
    public void testTableSortInfoBuilderWithZeroSinkParallelism() {
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(
                        () ->
                                new TableSortInfo.Builder()
                                        .setSortColumns(Arrays.asList("column1", "column2"))
                                        .setSortStrategy(OrderType.ORDER)
                                        .setSortInCluster(true)
                                        .setRangeNumber(10)
                                        .setSinkParallelism(0)
                                        .setLocalSampleSize(100)
                                        .setGlobalSampleSize(500)
                                        .build())
                .withMessageContaining(
                        "The sink parallelism must be specified when sorting the table data. Please set it using the key: %s",
                        FlinkConnectorOptions.SINK_PARALLELISM.key());
    }

    @Test
    public void testTableSortInfoBuilderWithZeroLocalSampleSize() {
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(
                        () ->
                                new TableSortInfo.Builder()
                                        .setSortColumns(Arrays.asList("column1", "column2"))
                                        .setSortStrategy(OrderType.ORDER)
                                        .setSortInCluster(true)
                                        .setRangeNumber(10)
                                        .setSinkParallelism(5)
                                        .setLocalSampleSize(0) // This should trigger an exception
                                        .setGlobalSampleSize(500)
                                        .build())
                .withMessage("Local sample size must be positive");
    }

    @Test
    public void testTableSortInfoBuilderWithNegativeGlobalSampleSize() {
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(
                        () ->
                                new TableSortInfo.Builder()
                                        .setSortColumns(Arrays.asList("column1", "column2"))
                                        .setSortStrategy(OrderType.ORDER)
                                        .setSortInCluster(true)
                                        .setRangeNumber(10)
                                        .setSinkParallelism(5)
                                        .setLocalSampleSize(100)
                                        .setGlobalSampleSize(-1) // This should trigger an exception
                                        .build())
                .withMessage("Global sample size must be positive");
    }
}
