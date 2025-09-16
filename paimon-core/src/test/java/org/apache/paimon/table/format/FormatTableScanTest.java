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

package org.apache.paimon.table.format;

import org.apache.paimon.fs.Path;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Pair;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Test for {@link FormatTableScan}. */
class FormatTableScanTest {

    @ParameterizedTest
    @ValueSource(
            strings = {
                "File.txt",
                "file.txt",
                "123file.txt",
                "F",
                "File-1.txt",
                "a",
                "0",
                "9test",
                "Test_file.log"
            })
    void testValidDataFileNames(String fileName) {
        assertTrue(
                FormatTableScan.isDataFileName(fileName),
                "Filename '" + fileName + "' should be valid");
    }

    @ParameterizedTest
    @ValueSource(strings = {".hidden", "_file.txt"})
    void testInvalidDataFileNames(String fileName) {
        assertFalse(
                FormatTableScan.isDataFileName(fileName),
                "Filename '" + fileName + "' should be invalid");
    }

    @Test
    void testNullInput() {
        assertFalse(FormatTableScan.isDataFileName(null), "Null input should return false");
    }

    @Test
    void testGetScanPathAndPartitionFilterNoPartitionKeys() {
        Path tableLocation = new Path("/test/table");
        List<String> partitionKeys = Collections.emptyList();
        RowType partitionType = RowType.of();
        PartitionPredicate partitionFilter = PartitionPredicate.alwaysTrue();

        Pair<Path, PartitionPredicate> result =
                FormatTableScan.getScanPathAndPartitionFilter(
                        tableLocation, partitionKeys, partitionFilter, partitionType);

        assertThat(result.getLeft()).isEqualTo(tableLocation);
        assertThat(result.getRight()).isEqualTo(partitionFilter);
    }

    @Test
    void testGetScanPathAndPartitionFilterNullFilter() {
        Path tableLocation = new Path("/test/table");
        List<String> partitionKeys = Arrays.asList("year", "month");
        RowType partitionType =
                RowType.builder()
                        .field("year", DataTypes.INT())
                        .field("month", DataTypes.INT())
                        .build();

        Pair<Path, PartitionPredicate> result =
                FormatTableScan.getScanPathAndPartitionFilter(
                        tableLocation, partitionKeys, null, partitionType);

        assertThat(result.getLeft()).isEqualTo(tableLocation);
        assertThat(result.getRight()).isNull();
    }

    @Test
    void testGetScanPathAndPartitionFilterWithEqualityFilter() {
        Path tableLocation = new Path("/test/table");
        List<String> partitionKeys = Arrays.asList("year", "month");
        RowType partitionType =
                RowType.builder()
                        .field("year", DataTypes.INT())
                        .field("month", DataTypes.INT())
                        .build();

        // Create equality predicate for all partition keys
        PredicateBuilder builder = new PredicateBuilder(partitionType);
        Predicate equalityPredicate =
                PredicateBuilder.and(builder.equal(0, 2023), builder.equal(1, 12));
        PartitionPredicate partitionFilter =
                PartitionPredicate.fromPredicate(partitionType, equalityPredicate);

        Pair<Path, PartitionPredicate> result =
                FormatTableScan.getScanPathAndPartitionFilter(
                        tableLocation, partitionKeys, partitionFilter, partitionType);

        // Should optimize to specific partition path
        assertThat(result.getLeft().toString()).isEqualTo("/test/table/year=2023/month=12");
        assertThat(result.getRight()).isNull();
    }

    @Test
    void testGetScanPathAndPartitionFilterWithFirstPartitionKeyEqualityFilter() {
        Path tableLocation = new Path("/test/table");
        List<String> partitionKeys = Arrays.asList("year", "month");
        RowType partitionType =
                RowType.builder()
                        .field("year", DataTypes.INT())
                        .field("month", DataTypes.INT())
                        .build();
        // Create equality predicate for only the first partition key
        PredicateBuilder builder = new PredicateBuilder(partitionType);
        Predicate firstKeyEqualityPredicate = builder.equal(0, 2023);
        PartitionPredicate partitionFilter =
                PartitionPredicate.fromPredicate(partitionType, firstKeyEqualityPredicate);

        Pair<Path, PartitionPredicate> result =
                FormatTableScan.getScanPathAndPartitionFilter(
                        tableLocation, partitionKeys, partitionFilter, partitionType);

        // Should optimize to specific partition path for first key
        assertThat(result.getLeft().toString()).isEqualTo("/test/table/year=2023");
        assertThat(result.getRight()).isEqualTo(partitionFilter);
    }

    @Test
    void testGetScanPathAndPartitionFilter() {
        Path tableLocation = new Path("/test/table");
        List<String> partitionKeys = Arrays.asList("year", "month");
        RowType partitionType =
                RowType.builder()
                        .field("year", DataTypes.INT())
                        .field("month", DataTypes.INT())
                        .build();

        // Create non-equality predicate
        PredicateBuilder builder = new PredicateBuilder(partitionType);
        Predicate nonEqualityPredicate = builder.greaterThan(0, 2022);
        PartitionPredicate partitionFilter =
                PartitionPredicate.fromPredicate(partitionType, nonEqualityPredicate);

        Pair<Path, PartitionPredicate> result =
                FormatTableScan.getScanPathAndPartitionFilter(
                        tableLocation, partitionKeys, partitionFilter, partitionType);

        // Should not optimize, keep original path and filter
        assertThat(result.getLeft()).isEqualTo(tableLocation);
        assertThat(result.getRight()).isEqualTo(partitionFilter);
    }

    @Test
    void testGetScanPathAndPartitionFilterWithOrPredicate() {
        Path tableLocation = new Path("/test/table");
        List<String> partitionKeys = Arrays.asList("year", "month");
        RowType partitionType =
                RowType.builder()
                        .field("year", DataTypes.INT())
                        .field("month", DataTypes.INT())
                        .build();

        // Create OR predicate (not equality-only)
        PredicateBuilder builder = new PredicateBuilder(partitionType);
        Predicate orPredicate = PredicateBuilder.or(builder.equal(0, 2023), builder.equal(0, 2024));
        PartitionPredicate partitionFilter =
                PartitionPredicate.fromPredicate(partitionType, orPredicate);

        Pair<Path, PartitionPredicate> result =
                FormatTableScan.getScanPathAndPartitionFilter(
                        tableLocation, partitionKeys, partitionFilter, partitionType);

        // Should not optimize, keep original path and filter
        assertThat(result.getLeft()).isEqualTo(tableLocation);
        assertThat(result.getRight()).isEqualTo(partitionFilter);
    }

    @Test
    void testExtractEqualityPartitionSpecWithLeadingConsecutiveEquality() {
        List<String> partitionKeys = Arrays.asList("year", "month", "day");
        RowType partitionType =
                RowType.builder()
                        .field("year", DataTypes.INT())
                        .field("month", DataTypes.INT())
                        .field("day", DataTypes.INT())
                        .build();

        // Create predicate: year = 2023 AND month = 12 AND day > 15
        PredicateBuilder builder = new PredicateBuilder(partitionType);
        Predicate mixedPredicate =
                PredicateBuilder.and(
                        PredicateBuilder.and(builder.equal(0, 2023), builder.equal(1, 12)),
                        builder.greaterThan(2, 15));
        PartitionPredicate partitionFilter =
                PartitionPredicate.fromPredicate(partitionType, mixedPredicate);

        Path tableLocation = new Path("/test/table");
        Pair<Path, PartitionPredicate> result =
                FormatTableScan.getScanPathAndPartitionFilter(
                        tableLocation, partitionKeys, partitionFilter, partitionType);

        // Should optimize to year and month path (leading consecutive equality)
        assertThat(result.getLeft().toString()).isEqualTo("/test/table/year=2023/month=12");
        assertThat(result.getRight()).isEqualTo(partitionFilter);
    }

    @Test
    void testExtractEqualityPartitionSpecWithNonConsecutiveEquality() {
        List<String> partitionKeys = Arrays.asList("year", "month", "day");
        RowType partitionType =
                RowType.builder()
                        .field("year", DataTypes.INT())
                        .field("month", DataTypes.INT())
                        .field("day", DataTypes.INT())
                        .build();

        // Create predicate: year = 2023 AND month > 6 AND day = 15
        PredicateBuilder builder = new PredicateBuilder(partitionType);
        Predicate mixedPredicate =
                PredicateBuilder.and(
                        PredicateBuilder.and(builder.equal(0, 2023), builder.greaterThan(1, 6)),
                        builder.equal(2, 15));
        PartitionPredicate partitionFilter =
                PartitionPredicate.fromPredicate(partitionType, mixedPredicate);

        Path tableLocation = new Path("/test/table");
        Pair<Path, PartitionPredicate> result =
                FormatTableScan.getScanPathAndPartitionFilter(
                        tableLocation, partitionKeys, partitionFilter, partitionType);

        // Should optimize only to year path (first equality, then stop at non-equality)
        assertThat(result.getLeft().toString()).isEqualTo("/test/table/year=2023");
        assertThat(result.getRight()).isEqualTo(partitionFilter);
    }

    @Test
    void testExtractEqualityPartitionSpecWithSecondPartitionKeyEqualityOnly() {
        List<String> partitionKeys = Arrays.asList("year", "month", "day");
        RowType partitionType =
                RowType.builder()
                        .field("year", DataTypes.INT())
                        .field("month", DataTypes.INT())
                        .field("day", DataTypes.INT())
                        .build();

        // Create predicate: year > 2020 AND month = 12 AND day = 15
        PredicateBuilder builder = new PredicateBuilder(partitionType);
        Predicate mixedPredicate =
                PredicateBuilder.and(
                        PredicateBuilder.and(builder.greaterThan(0, 2020), builder.equal(1, 12)),
                        builder.equal(2, 15));
        PartitionPredicate partitionFilter =
                PartitionPredicate.fromPredicate(partitionType, mixedPredicate);

        Path tableLocation = new Path("/test/table");
        Pair<Path, PartitionPredicate> result =
                FormatTableScan.getScanPathAndPartitionFilter(
                        tableLocation, partitionKeys, partitionFilter, partitionType);

        // Should not optimize because first partition key is not equality
        assertThat(result.getLeft()).isEqualTo(tableLocation);
        assertThat(result.getRight()).isEqualTo(partitionFilter);
    }

    @Test
    void testExtractEqualityPartitionSpecWithAllEqualityConditions() {
        List<String> partitionKeys = Arrays.asList("year", "month", "day");
        RowType partitionType =
                RowType.builder()
                        .field("year", DataTypes.INT())
                        .field("month", DataTypes.INT())
                        .field("day", DataTypes.INT())
                        .build();

        // Create predicate: year = 2023 AND month = 12 AND day = 25
        PredicateBuilder builder = new PredicateBuilder(partitionType);
        Predicate allEqualityPredicate =
                PredicateBuilder.and(
                        PredicateBuilder.and(builder.equal(0, 2023), builder.equal(1, 12)),
                        builder.equal(2, 25));
        PartitionPredicate partitionFilter =
                PartitionPredicate.fromPredicate(partitionType, allEqualityPredicate);

        Path tableLocation = new Path("/test/table");
        Pair<Path, PartitionPredicate> result =
                FormatTableScan.getScanPathAndPartitionFilter(
                        tableLocation, partitionKeys, partitionFilter, partitionType);

        // Should optimize to full partition path and no further filtering needed
        assertThat(result.getLeft().toString()).isEqualTo("/test/table/year=2023/month=12/day=25");
        assertThat(result.getRight()).isNull();
    }
}
