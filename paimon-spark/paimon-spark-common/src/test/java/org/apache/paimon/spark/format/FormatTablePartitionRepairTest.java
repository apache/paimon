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

package org.apache.paimon.spark.format;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.partition.Partition;
import org.apache.paimon.table.FormatTable;
import org.apache.paimon.table.format.FormatTablePartitionManager;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the catalog-managed partition repair engine of Format Tables. */
class FormatTablePartitionRepairTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    void applyDiffsAgainstTheWholeUnfilteredCatalogListing() {
        RecordingPartitionManager catalog = new RecordingPartitionManager();
        catalog.register(Arrays.asList(spec("dt", "20260701"), spec("dt", "20260702")));

        int applied =
                FormatTablePartitionRepair.apply(
                        catalog,
                        Arrays.asList(
                                spec("dt", "20260701"),
                                spec("dt", "20260702"),
                                spec("dt", "20260703")),
                        Collections.singletonList("dt"),
                        true,
                        false);

        // Only the partition missing from the catalog listing is registered.
        assertThat(applied).isEqualTo(1);
        assertThat(catalog.createdPartitions)
                .containsExactly(Collections.singletonList(spec("dt", "20260703")));
        // The diff needs every registered partition, so the listing is asked for all of them.
        assertThat(catalog.requestedPrefixes).containsExactly(Collections.emptyMap());
    }

    @Test
    void applyCreatesTheWholeDiffAsAnIdempotentBatch() {
        RecordingPartitionManager catalog = new RecordingPartitionManager();

        int applied =
                FormatTablePartitionRepair.apply(
                        catalog,
                        Arrays.asList(spec("dt", "20260701"), spec("dt", "20260702")),
                        Collections.singletonList("dt"),
                        true,
                        false);

        assertThat(applied).isEqualTo(2);
        assertThat(catalog.createdPartitions)
                .containsExactly(Arrays.asList(spec("dt", "20260701"), spec("dt", "20260702")));
        assertThat(catalog.createIgnoreFlags).containsExactly(true);
        assertThat(catalog.droppedPartitions).isEmpty();
    }

    @Test
    void applyPassesALargeAddDiffToTheCatalogInOneCall() {
        List<Map<String, String>> filesystemPartitions = new ArrayList<>();
        for (int index = 0; index < 1001; index++) {
            filesystemPartitions.add(spec("dt", String.format("%04d", index)));
        }

        RecordingPartitionManager catalog = new RecordingPartitionManager();

        int applied =
                FormatTablePartitionRepair.apply(
                        catalog,
                        filesystemPartitions,
                        Collections.singletonList("dt"),
                        true,
                        false);

        // Splitting a diff into per-request batches belongs to the partition catalog, so the
        // repair hands over the complete diff in a single call.
        assertThat(applied).isEqualTo(1001);
        assertThat(catalog.createdPartitions).hasSize(1);
        assertThat(catalog.createdPartitions.get(0)).isEqualTo(filesystemPartitions);
        assertThat(catalog.createIgnoreFlags).containsExactly(true);
    }

    @Test
    void applyPassesALargeDropDiffToTheCatalogInOneCall() {
        List<Map<String, String>> registered = new ArrayList<>();
        for (int index = 0; index < 1001; index++) {
            registered.add(spec("dt", String.format("%04d", index)));
        }

        RecordingPartitionManager catalog = new RecordingPartitionManager();
        catalog.register(registered);

        int applied =
                FormatTablePartitionRepair.apply(
                        catalog,
                        Collections.emptyList(),
                        Collections.singletonList("dt"),
                        false,
                        true);

        assertThat(applied).isEqualTo(1001);
        assertThat(catalog.droppedPartitions).hasSize(1);
        assertThat(catalog.droppedPartitions.get(0)).isEqualTo(registered);
        assertThat(catalog.createdPartitions).isEmpty();
    }

    @Test
    void dropOnlyUnregistersOnlyMissingDirectories() {
        RecordingPartitionManager catalog = new RecordingPartitionManager();
        catalog.register(Arrays.asList(spec("dt", "20260714"), spec("dt", "20260715")));

        // dt=20260715 exists on the filesystem, dt=20260714 does not: only the latter is dropped.
        int applied =
                FormatTablePartitionRepair.apply(
                        catalog,
                        Collections.singletonList(spec("dt", "20260715")),
                        Collections.singletonList("dt"),
                        false,
                        true);

        assertThat(applied).isEqualTo(1);
        assertThat(catalog.droppedPartitions)
                .containsExactly(Collections.singletonList(spec("dt", "20260714")));
        assertThat(catalog.createdPartitions).isEmpty();
    }

    @Test
    void addOnlyNeverDropsStaleCatalogPartitions() {
        RecordingPartitionManager catalog = new RecordingPartitionManager();
        catalog.register(Collections.singletonList(spec("dt", "20260714")));

        int applied =
                FormatTablePartitionRepair.apply(
                        catalog,
                        Collections.singletonList(spec("dt", "20260715")),
                        Collections.singletonList("dt"),
                        true,
                        false);

        assertThat(applied).isEqualTo(1);
        assertThat(catalog.createdPartitions)
                .containsExactly(Collections.singletonList(spec("dt", "20260715")));
        assertThat(catalog.droppedPartitions).isEmpty();
    }

    @Test
    void syncAddsAndDropsInOneCall() {
        RecordingPartitionManager catalog = new RecordingPartitionManager();
        catalog.register(Collections.singletonList(spec("dt", "20260714")));

        int applied =
                FormatTablePartitionRepair.apply(
                        catalog,
                        Collections.singletonList(spec("dt", "20260715")),
                        Collections.singletonList("dt"),
                        true,
                        true);

        assertThat(applied).isEqualTo(2);
        assertThat(catalog.createdPartitions)
                .containsExactly(Collections.singletonList(spec("dt", "20260715")));
        assertThat(catalog.droppedPartitions)
                .containsExactly(Collections.singletonList(spec("dt", "20260714")));
    }

    @Test
    void applyWithNoDiffDoesNotIssueAnEmptyMutation() {
        RecordingPartitionManager catalog = new RecordingPartitionManager();
        catalog.register(Collections.singletonList(spec("dt", "20260701")));

        int applied =
                FormatTablePartitionRepair.apply(
                        catalog,
                        Collections.singletonList(spec("dt", "20260701")),
                        Collections.singletonList("dt"),
                        true,
                        true);

        assertThat(applied).isZero();
        assertThat(catalog.createdPartitions).isEmpty();
        assertThat(catalog.droppedPartitions).isEmpty();
    }

    @Test
    void applyPropagatesFailureAfterAPartiallyAppliedMutation() {
        IllegalStateException failure =
                new IllegalStateException("injected partition drop failure");
        RecordingPartitionManager catalog =
                new RecordingPartitionManager() {
                    @Override
                    public void dropPartitions(List<Map<String, String>> partitions) {
                        throw failure;
                    }
                };
        catalog.register(Collections.singletonList(spec("dt", "20260714")));

        assertThatThrownBy(
                        () ->
                                FormatTablePartitionRepair.apply(
                                        catalog,
                                        Collections.singletonList(spec("dt", "20260715")),
                                        Collections.singletonList("dt"),
                                        true,
                                        true))
                .isSameAs(failure);
        // The ADD half is already committed; a rerun converges from there.
        assertThat(catalog.createdPartitions)
                .containsExactly(Collections.singletonList(spec("dt", "20260715")));
    }

    @Test
    void repairRegistersRawDirectoryValuesWithoutCastingThem() throws Exception {
        java.nio.file.Path partitionDirectory =
                Files.createDirectories(tempDir.resolve("dt=20260701/month=01"));
        Files.write(
                partitionDirectory.resolve("data.csv"),
                Collections.singletonList("1"),
                StandardCharsets.UTF_8);

        RecordingPartitionManager catalog = new RecordingPartitionManager();
        PaimonFormatTable sparkTable =
                new PaimonFormatTable(
                        monthPartitionedTable(
                                LocalFileIO.create(), tempDir.toUri().toString(), catalog));

        int applied = FormatTablePartitionRepair.repair(sparkTable, true, false);

        // The raw directory value must survive: a scan-based discovery would cast month=01 to 1
        // and register a spec that no longer round-trips to the real directory.
        Map<String, String> expected = new LinkedHashMap<>();
        expected.put("dt", "20260701");
        expected.put("month", "01");
        assertThat(applied).isEqualTo(1);
        assertThat(catalog.createdPartitions).containsExactly(Collections.singletonList(expected));
        assertThat(catalog.createIgnoreFlags).containsExactly(true);
    }

    @Test
    void repairRejectsUnsafeValueOnlyDirectoryBeforeCatalogMutation() throws Exception {
        Files.createDirectories(tempDir.resolve("%2E%2E"));

        RecordingPartitionManager catalog = new RecordingPartitionManager();
        PaimonFormatTable sparkTable =
                new PaimonFormatTable(formatTable(tempDir.toUri().toString(), true, catalog));

        assertThatThrownBy(() -> FormatTablePartitionRepair.repair(sparkTable, true, true))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("..");
        assertThat(catalog.requestedPrefixes).isEmpty();
        assertThat(catalog.createdPartitions).isEmpty();
        assertThat(catalog.droppedPartitions).isEmpty();
    }

    @Test
    void repairFailsClosedWhenTheCatalogListingFails() throws Exception {
        java.nio.file.Path partitionDirectory =
                Files.createDirectories(tempDir.resolve("dt=20260715"));
        Files.write(
                partitionDirectory.resolve("data.csv"),
                Collections.singletonList("15"),
                StandardCharsets.UTF_8);

        IllegalStateException listFailure =
                new IllegalStateException("injected catalog listing failure");
        AtomicInteger listCount = new AtomicInteger();
        RecordingPartitionManager catalog =
                new RecordingPartitionManager() {
                    @Override
                    public List<Partition> listPartitions(Map<String, String> prefix) {
                        listCount.incrementAndGet();
                        throw listFailure;
                    }
                };
        PaimonFormatTable sparkTable =
                new PaimonFormatTable(formatTable(tempDir.toUri().toString(), catalog));

        assertThatThrownBy(() -> FormatTablePartitionRepair.repair(sparkTable, true, true))
                .isSameAs(listFailure);
        assertThat(listCount).hasValue(1);
        assertThat(catalog.createdPartitions).isEmpty();
        assertThat(catalog.droppedPartitions).isEmpty();
        assertThat(partitionDirectory).exists();
    }

    @Test
    void repairFailsClosedWhenFilesystemDiscoveryFailsPartway() throws Exception {
        Files.createDirectories(tempDir.resolve("dt=20260715/month=01"));
        Files.createDirectories(tempDir.resolve("dt=20260716/month=01"));

        IOException listFailure = new IOException("injected nested filesystem LIST failure");
        AtomicInteger completedPartitionListings = new AtomicInteger();
        FileIO fileIO =
                new LocalFileIO() {
                    @Override
                    public FileStatus[] listStatus(Path path) throws IOException {
                        if ("dt=20260716".equals(path.getName())) {
                            throw listFailure;
                        }
                        FileStatus[] statuses = super.listStatus(path);
                        Arrays.sort(
                                statuses,
                                Comparator.comparing(status -> status.getPath().toString()));
                        if ("dt=20260715".equals(path.getName())) {
                            completedPartitionListings.incrementAndGet();
                        }
                        return statuses;
                    }
                };

        RecordingPartitionManager catalog = new RecordingPartitionManager();
        PaimonFormatTable sparkTable =
                new PaimonFormatTable(
                        monthPartitionedTable(fileIO, tempDir.toUri().toString(), catalog));

        assertThatThrownBy(() -> FormatTablePartitionRepair.repair(sparkTable, true, true))
                .isInstanceOf(RuntimeException.class)
                .hasCause(listFailure);
        // A truncated listing must never reach the diff: no catalog call at all.
        assertThat(completedPartitionListings).hasValue(1);
        assertThat(catalog.requestedPrefixes).isEmpty();
        assertThat(catalog.createdPartitions).isEmpty();
        assertThat(catalog.droppedPartitions).isEmpty();
    }

    private static Map<String, String> spec(String key, String value) {
        Map<String, String> spec = new LinkedHashMap<>();
        spec.put(key, value);
        return spec;
    }

    private static FormatTable formatTable(String location, FormatTablePartitionManager catalog) {
        return formatTable(location, false, catalog);
    }

    private static FormatTable formatTable(
            String location, boolean onlyValueInPath, FormatTablePartitionManager catalog) {
        RowType rowType =
                RowType.builder()
                        .field("id", DataTypes.INT())
                        .field("dt", DataTypes.STRING())
                        .build();
        return build(
                LocalFileIO.create(),
                location,
                rowType,
                Collections.singletonList("dt"),
                onlyValueInPath,
                catalog);
    }

    /** A table whose second partition key is an INT, so a cast would rewrite {@code 01} to 1. */
    private static FormatTable monthPartitionedTable(
            FileIO fileIO, String location, FormatTablePartitionManager catalog) {
        RowType rowType =
                RowType.builder()
                        .field("id", DataTypes.INT())
                        .field("dt", DataTypes.STRING())
                        .field("month", DataTypes.INT())
                        .build();
        return build(fileIO, location, rowType, Arrays.asList("dt", "month"), false, catalog);
    }

    private static FormatTable build(
            FileIO fileIO,
            String location,
            RowType rowType,
            List<String> partitionKeys,
            boolean onlyValueInPath,
            FormatTablePartitionManager catalog) {
        Map<String, String> options = new LinkedHashMap<>();
        options.put(CoreOptions.METASTORE_PARTITIONED_TABLE.key(), "true");
        options.put(
                CoreOptions.FORMAT_TABLE_PARTITION_ONLY_VALUE_IN_PATH.key(),
                Boolean.toString(onlyValueInPath));
        return FormatTable.builder()
                .fileIO(fileIO)
                .identifier(Identifier.create("db", "t"))
                .rowType(rowType)
                .partitionKeys(partitionKeys)
                .location(new Path(location).toString())
                .format(FormatTable.Format.CSV)
                .options(options)
                .partitionManager(catalog)
                .build();
    }

    private static class RecordingPartitionManager implements FormatTablePartitionManager {

        private static final long serialVersionUID = 1L;

        private final List<Map<String, String>> registered = new ArrayList<>();
        private final List<Map<String, String>> requestedPrefixes = new ArrayList<>();
        private final List<List<Map<String, String>>> createdPartitions = new ArrayList<>();
        private final List<Boolean> createIgnoreFlags = new ArrayList<>();
        private final List<List<Map<String, String>>> droppedPartitions = new ArrayList<>();

        private void register(List<Map<String, String>> partitions) {
            registered.addAll(partitions);
        }

        @Override
        public void createPartitions(List<Map<String, String>> partitions, boolean ignoreIfExists) {
            createdPartitions.add(new ArrayList<>(partitions));
            createIgnoreFlags.add(ignoreIfExists);
        }

        @Override
        public void dropPartitions(List<Map<String, String>> partitions) {
            droppedPartitions.add(new ArrayList<>(partitions));
        }

        @Override
        public List<Partition> listPartitionsByNames(List<Map<String, String>> partitions) {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<Partition> listPartitions(Map<String, String> prefix) {
            requestedPrefixes.add(prefix);
            List<Partition> partitions = new ArrayList<>(registered.size());
            for (Map<String, String> spec : registered) {
                partitions.add(new Partition(spec, 0L, 0L, 0L, 0L, 0, false));
            }
            return partitions;
        }
    }
}
