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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Pair;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.utils.PartitionPathUtils.searchPartSpecAndPaths;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Test for {@link FormatTableScan}. */
class FormatTableScanTest {

    @TempDir java.nio.file.Path tmpPath;

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
    void testComputeScanPathAndLevelNoPartitionKeys() {
        Path tableLocation = new Path("/test/table");
        List<String> partitionKeys = Collections.emptyList();
        RowType partitionType = RowType.of();
        PartitionPredicate partitionFilter = PartitionPredicate.alwaysTrue();

        Pair<Path, Integer> result =
                FormatTableScan.computeScanPathAndLevel(
                        tableLocation, partitionKeys, partitionFilter, partitionType, false);

        assertThat(result.getLeft()).isEqualTo(tableLocation);
        assertThat(result.getRight()).isEqualTo(0);
    }

    @Test
    void testGeneratePartitions() {
        Path tableLocation = new Path("/test/table");
        List<String> partitionKeys = Arrays.asList("year", "month");
        RowType partitionType =
                RowType.builder()
                        .field("year", DataTypes.INT())
                        .field("month", DataTypes.INT())
                        .build();

        BinaryRow partition =
                new InternalRowSerializer(partitionType).toBinaryRow(GenericRow.of(2023, 2));
        List<Pair<LinkedHashMap<String, String>, Path>> result =
                FormatTableScan.generatePartitions(
                        partitionKeys,
                        partitionType,
                        "",
                        tableLocation,
                        Collections.singleton(partition),
                        false);
        assertThat(result.size()).isEqualTo(1);
        assertThat(result.get(0).getLeft().toString()).isEqualTo("{year=2023, month=2}");
        assertThat(result.get(0).getRight().toString()).isEqualTo("/test/table/year=2023/month=2");
    }

    @Test
    void testGeneratePartitionsOnlyValue() {
        Path tableLocation = new Path("/test/table");
        List<String> partitionKeys = Arrays.asList("year", "month");
        RowType partitionType =
                RowType.builder()
                        .field("year", DataTypes.INT())
                        .field("month", DataTypes.INT())
                        .build();

        BinaryRow partition =
                new InternalRowSerializer(partitionType).toBinaryRow(GenericRow.of(2023, 2));
        List<Pair<LinkedHashMap<String, String>, Path>> result =
                FormatTableScan.generatePartitions(
                        partitionKeys,
                        partitionType,
                        "",
                        tableLocation,
                        Collections.singleton(partition),
                        true);
        assertThat(result.size()).isEqualTo(1);
        assertThat(result.get(0).getLeft().toString()).isEqualTo("{year=2023, month=2}");
        assertThat(result.get(0).getRight().toString()).isEqualTo("/test/table/2023/2");
    }

    @Test
    void testGetScanPathAndLevelNullFilter() {
        Path tableLocation = new Path("/test/table");
        List<String> partitionKeys = Arrays.asList("year", "month");
        RowType partitionType =
                RowType.builder()
                        .field("year", DataTypes.INT())
                        .field("month", DataTypes.INT())
                        .build();

        Pair<Path, Integer> result =
                FormatTableScan.computeScanPathAndLevel(
                        tableLocation, partitionKeys, null, partitionType, false);

        assertThat(result.getLeft()).isEqualTo(tableLocation);
        assertThat(result.getRight()).isEqualTo(2);
    }

    @Test
    void testGetScanPathAndLevelWithEqualityFilter() throws IOException {
        Path tableLocation = new Path(tmpPath.toUri());
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

        Pair<Path, Integer> result =
                FormatTableScan.computeScanPathAndLevel(
                        tableLocation, partitionKeys, partitionFilter, partitionType, false);

        // Should optimize to specific partition path
        assertThat(result.getLeft().toString()).isEqualTo(tableLocation + "year=2023/month=12");
        assertThat(result.getRight()).isEqualTo(0);

        // test searchPartSpecAndPaths
        LocalFileIO fileIO = LocalFileIO.create();
        fileIO.mkdirs(new Path(tableLocation, "year=2023/month=12"));
        List<Pair<LinkedHashMap<String, String>, Path>> searched =
                searchPartSpecAndPaths(fileIO, result.getLeft(), result.getRight());
        assertThat(searched.size()).isEqualTo(1);
    }

    @Test
    void testComputeScanPathWithFirstLevel() throws IOException {
        Path tableLocation = new Path(tmpPath.toUri());
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

        Pair<Path, Integer> result =
                FormatTableScan.computeScanPathAndLevel(
                        tableLocation, partitionKeys, partitionFilter, partitionType, false);

        // Should optimize to specific partition path for first key
        assertThat(result.getLeft().toString()).isEqualTo(tableLocation + "year=2023");
        assertThat(result.getRight()).isEqualTo(1);

        // test searchPartSpecAndPaths
        LocalFileIO fileIO = LocalFileIO.create();
        fileIO.mkdirs(new Path(tableLocation, "year=2023/month=12"));
        List<Pair<LinkedHashMap<String, String>, Path>> searched =
                searchPartSpecAndPaths(fileIO, result.getLeft(), result.getRight());
        assertThat(searched.size()).isEqualTo(1);
    }

    @Test
    void testComputeScanPathAndLevel() {
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

        Pair<Path, Integer> result =
                FormatTableScan.computeScanPathAndLevel(
                        tableLocation, partitionKeys, partitionFilter, partitionType, false);

        // Should not optimize, keep original path and level
        assertThat(result.getLeft()).isEqualTo(tableLocation);
        assertThat(result.getRight()).isEqualTo(2);
    }

    @Test
    void testComputeScanPathAndLevelWithOrPredicate() {
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

        Pair<Path, Integer> result =
                FormatTableScan.computeScanPathAndLevel(
                        tableLocation, partitionKeys, partitionFilter, partitionType, false);

        // Should not optimize, keep original path and level
        assertThat(result.getLeft()).isEqualTo(tableLocation);
        assertThat(result.getRight()).isEqualTo(2);
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
        Pair<Path, Integer> result =
                FormatTableScan.computeScanPathAndLevel(
                        tableLocation, partitionKeys, partitionFilter, partitionType, false);

        // Should optimize to year and month path (leading consecutive equality)
        assertThat(result.getLeft().toString()).isEqualTo("/test/table/year=2023/month=12");
        assertThat(result.getRight()).isEqualTo(1);
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
        Pair<Path, Integer> result =
                FormatTableScan.computeScanPathAndLevel(
                        tableLocation, partitionKeys, partitionFilter, partitionType, false);

        // Should optimize only to year path (first equality, then stop at non-equality)
        assertThat(result.getLeft().toString()).isEqualTo("/test/table/year=2023");
        assertThat(result.getRight()).isEqualTo(2);
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
        Pair<Path, Integer> result =
                FormatTableScan.computeScanPathAndLevel(
                        tableLocation, partitionKeys, partitionFilter, partitionType, false);

        // Should not optimize because first partition key is not equality
        assertThat(result.getLeft()).isEqualTo(tableLocation);
        assertThat(result.getRight()).isEqualTo(3);
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
        Pair<Path, Integer> result =
                FormatTableScan.computeScanPathAndLevel(
                        tableLocation, partitionKeys, partitionFilter, partitionType, false);

        // Should optimize to full partition path and no further filtering needed
        assertThat(result.getLeft().toString()).isEqualTo("/test/table/year=2023/month=12/day=25");
        assertThat(result.getRight()).isEqualTo(0);
    }

    @Test
    public void testExtractEqualityPartitionSpecWithAllEqualityWhenAllIsAnd() {
        RowType type =
                RowType.builder()
                        .field("year", DataTypes.INT())
                        .field("month", DataTypes.INT())
                        .field("day", DataTypes.INT())
                        .build();
        List<String> partitionKeys = Arrays.asList("year", "month", "day");

        // Create predicate: year = 2023 AND month = 12 AND day = 25
        PredicateBuilder builder = new PredicateBuilder(type);
        Predicate equalityPredicate =
                PredicateBuilder.and(
                        PredicateBuilder.and(builder.equal(0, 2023), builder.equal(1, 12)),
                        builder.equal(2, 25));

        Map<String, String> result =
                FormatTableScan.extractLeadingEqualityPartitionSpecWhenOnlyAnd(
                        partitionKeys, equalityPredicate);

        assertThat(result).hasSize(3);
        assertThat(result.get("year")).isEqualTo("2023");
        assertThat(result.get("month")).isEqualTo("12");
        assertThat(result.get("day")).isEqualTo("25");
    }

    @Test
    public void testExtractEqualityPartitionSpecWithLeadingConsecutiveEqualityWhenAllIsAnd() {
        RowType type =
                RowType.builder()
                        .field("year", DataTypes.INT())
                        .field("month", DataTypes.INT())
                        .field("day", DataTypes.INT())
                        .build();
        List<String> partitionKeys = Arrays.asList("year", "month", "day");

        // Create predicate: year = 2023 AND month = 12 AND day > 15
        PredicateBuilder builder = new PredicateBuilder(type);
        Predicate mixedPredicate =
                PredicateBuilder.and(
                        PredicateBuilder.and(builder.equal(0, 2023), builder.equal(1, 12)),
                        builder.greaterThan(2, 15));

        Map<String, String> result =
                FormatTableScan.extractLeadingEqualityPartitionSpecWhenOnlyAnd(
                        partitionKeys, mixedPredicate);

        assertThat(result).isNotNull();
        assertThat(result).hasSize(2);
        assertThat(result.get("year")).isEqualTo("2023");
        assertThat(result.get("month")).isEqualTo("12");
        assertThat(result.containsKey("day")).isFalse();
    }

    @Test
    public void testExtractEqualityPartitionSpecWithFirstPartitionKeyEqualityWhenAllIsAnd() {
        RowType type =
                RowType.builder()
                        .field("year", DataTypes.INT())
                        .field("month", DataTypes.INT())
                        .field("day", DataTypes.INT())
                        .build();
        List<String> partitionKeys = Arrays.asList("year", "month", "day");

        // Create predicate: year = 2023 AND month > 6 AND day = 15
        PredicateBuilder builder = new PredicateBuilder(type);
        Predicate mixedPredicate =
                PredicateBuilder.and(
                        PredicateBuilder.and(builder.equal(0, 2023), builder.greaterThan(1, 6)),
                        builder.equal(2, 15));

        Map<String, String> result =
                FormatTableScan.extractLeadingEqualityPartitionSpecWhenOnlyAnd(
                        partitionKeys, mixedPredicate);
        assertThat(result).hasSize(1);
        assertThat(result.get("year")).isEqualTo("2023");
        assertThat(result.containsKey("month")).isFalse();
        assertThat(result.containsKey("day")).isFalse();
    }

    @Test
    public void testExtractEqualityPartitionSpecWithNoLeadingEqualityWhenAllIsAnd() {
        RowType type =
                RowType.builder()
                        .field("year", DataTypes.INT())
                        .field("month", DataTypes.INT())
                        .field("day", DataTypes.INT())
                        .build();
        List<String> partitionKeys = Arrays.asList("year", "month", "day");

        // Create predicate: year > 2020 AND month = 12 AND day = 15
        PredicateBuilder builder = new PredicateBuilder(type);
        Predicate mixedPredicate =
                PredicateBuilder.and(
                        PredicateBuilder.and(builder.greaterThan(0, 2020), builder.equal(1, 12)),
                        builder.equal(2, 15));

        Map<String, String> result =
                FormatTableScan.extractLeadingEqualityPartitionSpecWhenOnlyAnd(
                        partitionKeys, mixedPredicate);

        assertThat(result).isEmpty();
    }

    @Test
    public void testExtractEqualityPartitionSpecWithNonEqualityPredicateWhenAllIsAnd() {
        RowType type =
                RowType.builder()
                        .field("year", DataTypes.INT())
                        .field("month", DataTypes.INT())
                        .build();
        List<String> partitionKeys = Arrays.asList("year", "month");

        // Create predicate: year > 2020 AND month > 6
        PredicateBuilder builder = new PredicateBuilder(type);
        Predicate nonEqualityPredicate =
                PredicateBuilder.and(builder.greaterThan(0, 2020), builder.greaterThan(1, 6));

        Map<String, String> result =
                FormatTableScan.extractLeadingEqualityPartitionSpecWhenOnlyAnd(
                        partitionKeys, nonEqualityPredicate);

        assertThat(result).isEmpty();
    }

    @Test
    public void testExtractLeadingEqualityPartitionSpecWhenOnlyAndWithOrPredicate() {
        RowType type =
                RowType.builder()
                        .field("year", DataTypes.INT())
                        .field("month", DataTypes.INT())
                        .build();
        List<String> partitionKeys = Arrays.asList("year", "month");

        // Create predicate: year = 2023 OR year = 2024
        PredicateBuilder builder = new PredicateBuilder(type);
        Predicate orPredicate = PredicateBuilder.or(builder.equal(0, 2023), builder.equal(0, 2024));

        Map<String, String> result =
                FormatTableScan.extractLeadingEqualityPartitionSpecWhenOnlyAnd(
                        partitionKeys, orPredicate);

        assertThat(result).isEmpty();
    }
}
