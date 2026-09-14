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
import org.apache.paimon.partition.Partition;
import org.apache.paimon.partition.PartitionStatistics;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.CoreOptions.PATH;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link FormatTablePartitionRegistryValidator}. */
class FormatTablePartitionRegistryValidatorTest {

    private static final List<String> PARTITION_KEYS = Arrays.asList("year", "month");

    @TempDir java.nio.file.Path tempDir;

    @Test
    void testPartitionsClaimingEachOtherAreRejectedOnlyWhenComparedToEachOther() {
        Path tablePath = new Path(new Path(tempDir.toUri()), "table");
        Path external = new Path(new Path(tempDir.toUri()), "external");
        List<Partition> registry =
                Arrays.asList(
                        partitionAt(partitionSpec("2026", "01"), external.toString()),
                        partitionAt(
                                partitionSpec("2026", "02"),
                                new Path(external, "child").toString()));

        assertThatThrownBy(() -> validateTogether(registry, tablePath))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("overlapping locations");

        // Each location is well formed on its own, so an operation that only needs to know where
        // every partition lives can go ahead - dropping one of the two is what repairs the pair.
        assertThatCode(() -> validateEach(registry, tablePath)).doesNotThrowAnyException();
    }

    @Test
    void testLocationInsideTheTableDirectoryIsRejectedEitherWay() {
        Path tablePath = new Path(new Path(tempDir.toUri()), "table");
        List<Partition> registry =
                Collections.singletonList(
                        partitionAt(
                                partitionSpec("2026", "01"),
                                new Path(tablePath, "year=2026/month=02").toString()));

        assertThatThrownBy(() -> validateTogether(registry, tablePath))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("invalid custom location");
        assertThatThrownBy(() -> validateEach(registry, tablePath))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("invalid custom location");
    }

    @Test
    void testIncompleteSpecIsRejectedEitherWay() {
        Path tablePath = new Path(new Path(tempDir.toUri()), "table");
        List<Partition> registry =
                Collections.singletonList(
                        partitionAt(Collections.singletonMap("year", "2026"), null));

        assertThatThrownBy(() -> validateTogether(registry, tablePath))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("incomplete partition spec");
        assertThatThrownBy(() -> validateEach(registry, tablePath))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("incomplete partition spec");
    }

    @Test
    void testTwoRowsForOneSpecAreRejectedOnlyWhenComparedToEachOther() {
        Path tablePath = new Path(new Path(tempDir.toUri()), "table");
        Path external = new Path(new Path(tempDir.toUri()), "external");
        List<Partition> registry =
                Arrays.asList(
                        partitionAt(partitionSpec("2026", "01"), external.toString()),
                        partitionAt(
                                partitionSpec("2026", "01"),
                                new Path(new Path(tempDir.toUri()), "other").toString()));

        assertThatThrownBy(() -> validateTogether(registry, tablePath))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("overlapping locations");
        assertThatCode(() -> validateEach(registry, tablePath)).doesNotThrowAnyException();
    }

    private static void validateTogether(List<Partition> registry, Path tablePath) {
        FormatTablePartitionRegistryValidator.validatePartitionLocations(
                registry, PARTITION_KEYS, tablePath, "db.table", false, null);
    }

    private static void validateEach(List<Partition> registry, Path tablePath) {
        FormatTablePartitionRegistryValidator.validateEachPartitionLocation(
                registry, PARTITION_KEYS, tablePath, "db.table", false, null);
    }

    private static Partition partitionAt(Map<String, String> spec, String location) {
        Map<String, String> options =
                location == null ? null : Collections.singletonMap(PATH.key(), location);
        return new Partition(
                spec,
                0,
                0,
                0,
                0,
                PartitionStatistics.UNKNOWN_TOTAL_BUCKETS,
                false,
                null,
                null,
                null,
                null,
                options);
    }

    private static Map<String, String> partitionSpec(String year, String month) {
        LinkedHashMap<String, String> spec = new LinkedHashMap<>();
        spec.put("year", year);
        spec.put("month", month);
        return spec;
    }
}
