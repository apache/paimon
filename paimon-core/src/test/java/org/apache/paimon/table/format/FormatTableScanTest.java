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

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.format.csv.CsvOptions;
import org.apache.paimon.format.json.JsonOptions;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.table.FormatTable;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.testutils.junit.parameterized.ParameterizedTestExtension;
import org.apache.paimon.testutils.junit.parameterized.Parameters;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Pair;

import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.CoreOptions.SOURCE_SPLIT_TARGET_SIZE;
import static org.apache.paimon.utils.PartitionPathUtils.searchPartSpecAndPaths;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Test for {@link FormatTableScan}. */
@ExtendWith(ParameterizedTestExtension.class)
public class FormatTableScanTest {

    @TempDir java.nio.file.Path tmpPath;

    private final boolean enablePartitionValueOnly;
    private final Path defaultTableLocation = new Path("/test/table_scan");
    private final RowType partitionType =
            RowType.builder()
                    .field("year", DataTypes.INT())
                    .field("month", DataTypes.INT())
                    .build();
    private final BinaryRow partition =
            new InternalRowSerializer(partitionType).toBinaryRow(GenericRow.of(2023, 2));
    private final String partitionPath;
    private final List<String> partitionKeys = partitionType.getFieldNames();

    public FormatTableScanTest(boolean enablePartitionValueOnly, String partitionPath) {
        this.enablePartitionValueOnly = enablePartitionValueOnly;
        this.partitionPath = defaultTableLocation + partitionPath;
    }

    @Parameters(name = "enablePartitionValueOnly={0},partitionPath={1}")
    public static List<Object[]> parameters() {
        return Arrays.asList(
                new Object[] {false, "/year=2023/month=2"}, new Object[] {true, "/2023/2"});
    }

    @TestTemplate
    void testValidDataFileNames() {
        // Test valid data file names
        String[] fileNames = {"File.txt", "file.txt", "123file.txt", "data", "Test_file.log"};
        for (String fileName : fileNames) {
            assertTrue(
                    FormatTableScan.isDataFileName(fileName),
                    "Filename '" + fileName + "' should be valid");
        }
    }

    @TestTemplate
    void testInvalidDataFileNames() {
        String[] fileNames = {".hidden", "_file.txt"};
        for (String fileName : fileNames) {
            assertFalse(
                    FormatTableScan.isDataFileName(fileName),
                    "Filename '" + fileName + "' should be invalid");
        }
    }

    @TestTemplate
    void testNullInput() {
        assertFalse(FormatTableScan.isDataFileName(null), "Null input should return false");
    }

    @TestTemplate
    void testComputeScanPathAndLevelNoPartitionKeys() {
        List<String> partitionKeys = Collections.emptyList();
        RowType partitionType = RowType.of();
        PartitionPredicate partitionFilter = PartitionPredicate.ALWAYS_TRUE;

        Pair<Path, Integer> result =
                FormatTableScan.computeScanPathAndLevel(
                        defaultTableLocation, partitionKeys, partitionFilter, partitionType, false);

        assertThat(result.getLeft()).isEqualTo(defaultTableLocation);
        assertThat(result.getRight()).isEqualTo(0);
    }

    @TestTemplate
    void testGeneratePartitions() {
        List<Pair<LinkedHashMap<String, String>, Path>> result =
                FormatTableScan.generatePartitions(
                        partitionKeys,
                        partitionType,
                        "",
                        defaultTableLocation,
                        Collections.singleton(partition),
                        enablePartitionValueOnly);
        assertThat(result.size()).isEqualTo(1);
        assertThat(result.get(0).getLeft().toString()).isEqualTo("{year=2023, month=2}");
        assertThat(result.get(0).getRight().toString()).isEqualTo(partitionPath);
    }

    @TestTemplate
    void testGetScanPathAndLevelNullFilter() {
        Pair<Path, Integer> result =
                FormatTableScan.computeScanPathAndLevel(
                        defaultTableLocation,
                        partitionType.getFieldNames(),
                        null,
                        partitionType,
                        false);

        assertThat(result.getLeft()).isEqualTo(defaultTableLocation);
        assertThat(result.getRight()).isEqualTo(2);
    }

    @TestTemplate
    void testComputeScanPathWithoutFilter() throws IOException {
        Path tableLocation = new Path(tmpPath.toUri());
        PredicateBuilder builder = new PredicateBuilder(partitionType);
        Predicate predicate = PredicateBuilder.and(builder.greaterThan(0, 2022));
        PartitionPredicate partitionFilter =
                PartitionPredicate.fromPredicate(partitionType, predicate);
        Pair<Path, Integer> result =
                FormatTableScan.computeScanPathAndLevel(
                        tableLocation,
                        partitionKeys,
                        partitionFilter,
                        partitionType,
                        enablePartitionValueOnly);

        // Should not be optimized because of greater than
        assertThat(result.getLeft()).isEqualTo(tableLocation);
        assertThat(result.getRight()).isEqualTo(2);

        // test searchPartSpecAndPaths
        LocalFileIO fileIO = LocalFileIO.create();
        String partitionPath = enablePartitionValueOnly ? "2023/12" : "year=2023/month=12";
        fileIO.mkdirs(new Path(tableLocation, partitionPath));
        List<Pair<LinkedHashMap<String, String>, Path>> searched =
                searchPartSpecAndPaths(
                        fileIO,
                        result.getLeft(),
                        result.getRight(),
                        partitionKeys,
                        enablePartitionValueOnly);
        LinkedHashMap<String, String> expectPartitionSpec =
                new LinkedHashMap<>(partitionKeys.size());
        expectPartitionSpec.put("year", "2023");
        expectPartitionSpec.put("month", "12");
        assertThat(searched.get(0).getLeft()).isEqualTo(expectPartitionSpec);
        assertThat(searched.size()).isEqualTo(1);
    }

    @TestTemplate
    void testGetScanPathAndLevelWithEqualityFilter() throws IOException {
        Path tableLocation = new Path(tmpPath.toUri());
        // Create equality predicate for all partition keys
        PredicateBuilder builder = new PredicateBuilder(partitionType);
        Predicate equalityPredicate =
                PredicateBuilder.and(builder.equal(0, 2023), builder.equal(1, 12));
        PartitionPredicate partitionFilter =
                PartitionPredicate.fromPredicate(partitionType, equalityPredicate);

        Pair<Path, Integer> result =
                FormatTableScan.computeScanPathAndLevel(
                        tableLocation,
                        partitionKeys,
                        partitionFilter,
                        partitionType,
                        enablePartitionValueOnly);
        String partitionPath = enablePartitionValueOnly ? "2023/12" : "year=2023/month=12";

        // Should optimize to specific partition path
        assertThat(result.getLeft().toString()).isEqualTo(tableLocation + partitionPath);
        assertThat(result.getRight()).isEqualTo(0);

        // test searchPartSpecAndPaths
        LocalFileIO fileIO = LocalFileIO.create();
        fileIO.mkdirs(new Path(tableLocation, partitionPath));
        List<Pair<LinkedHashMap<String, String>, Path>> searched =
                searchPartSpecAndPaths(
                        fileIO,
                        result.getLeft(),
                        result.getRight(),
                        partitionKeys,
                        enablePartitionValueOnly);
        LinkedHashMap<String, String> expectPartitionSpec =
                new LinkedHashMap<>(partitionKeys.size());
        expectPartitionSpec.put("year", "2023");
        expectPartitionSpec.put("month", "12");
        assertThat(searched.get(0).getLeft()).isEqualTo(expectPartitionSpec);
        assertThat(searched.size()).isEqualTo(1);
    }

    @TestTemplate
    void testComputeScanPathWithFirstLevel() throws IOException {
        Path tableLocation = new Path(tmpPath.toUri());
        // Create equality predicate for only the first partition key
        PredicateBuilder builder = new PredicateBuilder(partitionType);
        Predicate firstKeyEqualityPredicate = builder.equal(0, 2023);
        PartitionPredicate partitionFilter =
                PartitionPredicate.fromPredicate(partitionType, firstKeyEqualityPredicate);

        Pair<Path, Integer> result =
                FormatTableScan.computeScanPathAndLevel(
                        tableLocation,
                        partitionKeys,
                        partitionFilter,
                        partitionType,
                        enablePartitionValueOnly);

        // Should optimize to specific partition path for first key
        String partitionPath = enablePartitionValueOnly ? "2023" : "year=2023";
        assertThat(result.getLeft().toString()).isEqualTo(tableLocation + partitionPath);
        assertThat(result.getRight()).isEqualTo(1);

        // test searchPartSpecAndPaths
        LocalFileIO fileIO = LocalFileIO.create();
        partitionPath = enablePartitionValueOnly ? "2023/12" : "year=2023/month=12";
        fileIO.mkdirs(new Path(tableLocation, partitionPath));
        List<Pair<LinkedHashMap<String, String>, Path>> searched =
                searchPartSpecAndPaths(
                        fileIO,
                        result.getLeft(),
                        result.getRight(),
                        partitionKeys,
                        enablePartitionValueOnly);
        LinkedHashMap<String, String> expectPartitionSpec =
                new LinkedHashMap<>(partitionKeys.size());
        expectPartitionSpec.put("year", "2023");
        expectPartitionSpec.put("month", "12");
        assertThat(searched.get(0).getLeft()).isEqualTo(expectPartitionSpec);
        assertThat(searched.size()).isEqualTo(1);
    }

    @TestTemplate
    void testNoOptimizationWithSecondEquality() throws IOException {
        Path tableLocation = new Path(tmpPath.toUri());
        // Create equality predicate for only the second partition key
        PredicateBuilder builder = new PredicateBuilder(partitionType);
        Predicate predicate =
                PredicateBuilder.and(builder.greaterOrEqual(0, 2023), builder.equal(1, 12));
        PartitionPredicate partitionFilter =
                PartitionPredicate.fromPredicate(partitionType, predicate);

        Pair<Path, Integer> result =
                FormatTableScan.computeScanPathAndLevel(
                        tableLocation,
                        partitionKeys,
                        partitionFilter,
                        partitionType,
                        enablePartitionValueOnly);

        // Should not optimize with second equality filter
        assertThat(result.getLeft()).isEqualTo(tableLocation);
        assertThat(result.getRight()).isEqualTo(2);

        // test searchPartSpecAndPaths
        LocalFileIO fileIO = LocalFileIO.create();
        String partitionPath = enablePartitionValueOnly ? "2023/12" : "year=2023/month=12";
        fileIO.mkdirs(new Path(tableLocation, partitionPath));
        List<Pair<LinkedHashMap<String, String>, Path>> searched =
                searchPartSpecAndPaths(
                        fileIO,
                        result.getLeft(),
                        result.getRight(),
                        partitionKeys,
                        enablePartitionValueOnly);
        LinkedHashMap<String, String> expectPartitionSpec =
                new LinkedHashMap<>(partitionKeys.size());
        expectPartitionSpec.put("year", "2023");
        expectPartitionSpec.put("month", "12");
        assertThat(searched.get(0).getLeft()).isEqualTo(expectPartitionSpec);
        assertThat(searched.size()).isEqualTo(1);
    }

    @TestTemplate
    void testSkipIllegalPath() throws IOException {
        Path tableLocation = new Path(tmpPath.toUri());
        PartitionPredicate partitionFilter = PartitionPredicate.fromPredicate(partitionType, null);
        Pair<Path, Integer> result =
                FormatTableScan.computeScanPathAndLevel(
                        tableLocation,
                        partitionKeys,
                        partitionFilter,
                        partitionType,
                        enablePartitionValueOnly);

        LocalFileIO fileIO = LocalFileIO.create();
        String illegalPath =
                enablePartitionValueOnly
                        ? "_unknown-year/unknown-month"
                        : "unknown-year/unknown-month";
        fileIO.mkdirs(new Path(tableLocation, illegalPath));
        String partitionPath = enablePartitionValueOnly ? "2023/12" : "year=2023/month=12";
        fileIO.mkdirs(new Path(tableLocation, partitionPath));
        List<Pair<LinkedHashMap<String, String>, Path>> searched =
                searchPartSpecAndPaths(
                        fileIO,
                        result.getLeft(),
                        result.getRight(),
                        partitionKeys,
                        enablePartitionValueOnly);
        LinkedHashMap<String, String> expectPartitionSpec =
                new LinkedHashMap<>(partitionKeys.size());
        expectPartitionSpec.put("year", "2023");
        expectPartitionSpec.put("month", "12");
        assertThat(searched.get(0).getLeft()).isEqualTo(expectPartitionSpec);
        assertThat(searched.size()).isEqualTo(1);
    }

    @TestTemplate
    void testComputeScanPathAndLevel() {
        Path tableLocation = new Path(tmpPath.toUri());
        // Create non-equality predicate
        PredicateBuilder builder = new PredicateBuilder(partitionType);
        Predicate nonEqualityPredicate = builder.greaterThan(0, 2022);
        PartitionPredicate partitionFilter =
                PartitionPredicate.fromPredicate(partitionType, nonEqualityPredicate);

        Pair<Path, Integer> result =
                FormatTableScan.computeScanPathAndLevel(
                        tableLocation,
                        partitionKeys,
                        partitionFilter,
                        partitionType,
                        enablePartitionValueOnly);

        // Should not optimize, keep original path and level
        assertThat(result.getLeft()).isEqualTo(tableLocation);
        assertThat(result.getRight()).isEqualTo(2);
    }

    @TestTemplate
    void testComputeScanPathAndLevelWithOrPredicate() {
        Path tableLocation = new Path(tmpPath.toUri());

        // Create OR predicate (not equality-only)
        PredicateBuilder builder = new PredicateBuilder(partitionType);
        Predicate orPredicate = PredicateBuilder.or(builder.equal(0, 2023), builder.equal(0, 2024));
        PartitionPredicate partitionFilter =
                PartitionPredicate.fromPredicate(partitionType, orPredicate);

        Pair<Path, Integer> result =
                FormatTableScan.computeScanPathAndLevel(
                        tableLocation,
                        partitionKeys,
                        partitionFilter,
                        partitionType,
                        enablePartitionValueOnly);

        // Should not optimize, keep original path and level
        assertThat(result.getLeft()).isEqualTo(tableLocation);
        assertThat(result.getRight()).isEqualTo(2);
    }

    @TestTemplate
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
                        tableLocation,
                        partitionKeys,
                        partitionFilter,
                        partitionType,
                        enablePartitionValueOnly);

        // Should optimize to year and month path (leading consecutive equality)
        String partitionPath = enablePartitionValueOnly ? "/2023/12" : "/year=2023/month=12";
        assertThat(result.getLeft().toString()).isEqualTo(tableLocation + partitionPath);
        assertThat(result.getRight()).isEqualTo(1);
    }

    @TestTemplate
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
                        tableLocation,
                        partitionKeys,
                        partitionFilter,
                        partitionType,
                        enablePartitionValueOnly);

        // Should optimize only to year path (first equality, then stop at non-equality)
        String partitionPath = enablePartitionValueOnly ? "/2023" : "/year=2023";
        assertThat(result.getLeft().toString()).isEqualTo(tableLocation + partitionPath);
        assertThat(result.getRight()).isEqualTo(2);
    }

    @TestTemplate
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
                        tableLocation,
                        partitionKeys,
                        partitionFilter,
                        partitionType,
                        enablePartitionValueOnly);

        // Should not optimize because first partition key is not equality
        assertThat(result.getLeft()).isEqualTo(tableLocation);
        assertThat(result.getRight()).isEqualTo(3);
    }

    @TestTemplate
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
                        tableLocation,
                        partitionKeys,
                        partitionFilter,
                        partitionType,
                        enablePartitionValueOnly);

        // Should optimize to full partition path and no further filtering needed
        String partitionPath =
                enablePartitionValueOnly ? "/2023/12/25" : "/year=2023/month=12/day=25";
        assertThat(result.getLeft().toString()).isEqualTo(tableLocation + partitionPath);
        assertThat(result.getRight()).isEqualTo(0);
    }

    @TestTemplate
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

    @TestTemplate
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

    @TestTemplate
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

    @TestTemplate
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

    @TestTemplate
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

    @TestTemplate
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

    @TestTemplate
    public void testCreateSplitsWithMultipleFiles() throws IOException {
        for (String format : Arrays.asList("csv", "json")) {
            Path tableLocation = new Path(new Path(tmpPath.toUri()), format);
            LocalFileIO fileIO = LocalFileIO.create();
            Path file1 = new Path(tableLocation, "data1." + format);
            Path file2 = new Path(tableLocation, "data2." + format);
            Path file3 = new Path(tableLocation, "data3." + format);
            writeTestFile(fileIO, file1, 80);
            writeTestFile(fileIO, file2, 120);
            writeTestFile(fileIO, file3, 200);

            Map<String, String> options = new HashMap<>();
            options.put(SOURCE_SPLIT_TARGET_SIZE.key(), "100b");

            FormatTable formatTable =
                    createFormatTableWithOptions(
                            tableLocation,
                            FormatTable.Format.valueOf(format.toUpperCase()),
                            options);
            FormatTableScan scan = new FormatTableScan(formatTable, null, null);
            List<Split> splits = scan.plan().splits();
            assertThat(splits).hasSize(5);
        }
    }

    @TestTemplate
    public void testCreateSplitsWhenDefineLineDelimiter() throws IOException {
        for (String format : Arrays.asList("csv", "json")) {
            Path tableLocation = new Path(new Path(tmpPath.toUri()), format);
            LocalFileIO fileIO = LocalFileIO.create();
            Path file1 = new Path(tableLocation, "data1." + format);
            Path file2 = new Path(tableLocation, "data2." + format);
            Path file3 = new Path(tableLocation, "data3." + format);
            writeTestFile(fileIO, file1, 80);
            writeTestFile(fileIO, file2, 120);
            writeTestFile(fileIO, file3, 200);

            Map<String, String> options = new HashMap<>();
            options.put(SOURCE_SPLIT_TARGET_SIZE.key(), "100b");
            if ("csv".equals(format)) {
                options.put(CsvOptions.LINE_DELIMITER.key(), "\001");
            } else {
                options.put(JsonOptions.LINE_DELIMITER.key(), "\001");
            }

            FormatTable formatTable =
                    createFormatTableWithOptions(
                            tableLocation,
                            FormatTable.Format.valueOf(format.toUpperCase()),
                            options);
            FormatTableScan scan = new FormatTableScan(formatTable, null, null);
            List<Split> splits = scan.plan().splits();
            assertThat(splits).hasSize(3);
        }
    }

    @TestTemplate
    public void testCreateSplitsWithParquetFile() throws IOException {
        Path tableLocation = new Path(tmpPath.toUri());
        LocalFileIO fileIO = LocalFileIO.create();

        // Create a large Parquet file (non-splittable)
        Path parquetFile = new Path(tableLocation, "data.parquet");
        long fileSize = 300; // 300 bytes
        writeTestFile(fileIO, parquetFile, fileSize);

        // Set split max size to 100 bytes
        Map<String, String> options = new HashMap<>();
        options.put(SOURCE_SPLIT_TARGET_SIZE.key(), "100b");

        FormatTable formatTable =
                createFormatTableWithOptions(tableLocation, FormatTable.Format.PARQUET, options);
        FormatTableScan scan = new FormatTableScan(formatTable, null, null);
        List<Split> splits = scan.plan().splits();

        // Parquet files should NOT be split, should be a single split
        assertThat(splits).hasSize(1);
        FormatDataSplit split = (FormatDataSplit) splits.get(0);
        assertThat(split.filePath()).isEqualTo(parquetFile);
        assertThat(split.offset()).isEqualTo(0);
    }

    @TestTemplate
    public void testCreateSplitsWithEmptyDirectory() throws IOException {
        Path tableLocation = new Path(tmpPath.toUri());
        LocalFileIO fileIO = LocalFileIO.create();
        fileIO.mkdirs(tableLocation);

        FormatTable formatTable =
                createFormatTableWithOptions(
                        tableLocation, FormatTable.Format.CSV, Collections.emptyMap());
        FormatTableScan scan = new FormatTableScan(formatTable, null, null);
        List<Split> splits = scan.plan().splits();

        assertThat(splits).isEmpty();
    }

    private void writeTestFile(LocalFileIO fileIO, Path filePath, long size) throws IOException {
        fileIO.mkdirs(filePath.getParent());
        try (OutputStream out = fileIO.newOutputStream(filePath, false)) {
            byte[] data = new byte[(int) size];
            Arrays.fill(data, (byte) 'a');
            out.write(data);
        }
    }

    private FormatTable createFormatTableWithOptions(
            Path tableLocation, FormatTable.Format format, Map<String, String> options) {
        RowType rowType =
                RowType.builder()
                        .field("id", DataTypes.INT())
                        .field("name", DataTypes.STRING())
                        .build();

        return FormatTable.builder()
                .fileIO(LocalFileIO.create())
                .identifier(Identifier.create("test_db", "test_table"))
                .rowType(rowType)
                .partitionKeys(Collections.emptyList())
                .location(tableLocation.toString())
                .format(format)
                .options(options)
                .build();
    }
}
