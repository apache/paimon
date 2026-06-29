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

package org.apache.paimon.stats;

import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.PathFactory;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link StatisticsSidecarFile}. */
public class StatisticsSidecarFileTest {

    @TempDir java.nio.file.Path tempPath;

    @Test
    public void testWriteAndReadMultipleBlobs() {
        StatisticsSidecarFile sidecarFile =
                new StatisticsSidecarFile(
                        LocalFileIO.create(),
                        pathFactory(FileStorePathFactory.STATISTICS_SIDECAR_PREFIX));

        Map<String, String> firstProperties = new LinkedHashMap<>();
        firstProperties.put("name", "first");

        StatisticsBlob first =
                new StatisticsBlob(
                        "test-type-a",
                        Collections.singletonList(1),
                        10L,
                        20L,
                        firstProperties,
                        "first-payload".getBytes(StandardCharsets.UTF_8));
        StatisticsBlob second =
                new StatisticsBlob(
                        "test-type-b",
                        Arrays.asList(2, 3),
                        10L,
                        21L,
                        Collections.emptyMap(),
                        "second-payload".getBytes(StandardCharsets.UTF_8));

        List<StatisticsBlobMetadata> writtenMetadata =
                sidecarFile.write(Arrays.asList(first, second));

        assertThat(writtenMetadata).hasSize(2);
        assertThat(writtenMetadata.get(0).fileLocation())
                .startsWith(FileStorePathFactory.STATISTICS_SIDECAR_PREFIX);
        assertThat(writtenMetadata.get(0).fileLocation())
                .isEqualTo(writtenMetadata.get(1).fileLocation());
        assertThat(sidecarFile.exists(writtenMetadata.get(0).fileLocation())).isTrue();
        assertThat(writtenMetadata.get(0).offset()).hasValue(0L);
        assertThat(writtenMetadata.get(0).length()).hasValue((long) first.payload().length);
        assertThat(writtenMetadata.get(1).offset()).hasValue((long) first.payload().length);
        assertThat(writtenMetadata.get(1).length()).hasValue((long) second.payload().length);

        assertThat(sidecarFile.read(writtenMetadata.get(0))).containsExactly(first.payload());
        assertThat(sidecarFile.read(writtenMetadata.get(1))).containsExactly(second.payload());
        assertThat(sidecarFile.readMetadata(writtenMetadata.get(0).fileLocation()))
                .isEqualTo(writtenMetadata);

        sidecarFile.delete(writtenMetadata.get(0).fileLocation());
        assertThat(sidecarFile.exists(writtenMetadata.get(0).fileLocation())).isFalse();
    }

    @Test
    public void testStatsFileHandlerDeletesSidecarFiles() {
        PathFactory statsPathFactory = pathFactory("stat-");
        PathFactory sidecarPathFactory =
                pathFactory(FileStorePathFactory.STATISTICS_SIDECAR_PREFIX);
        LocalFileIO fileIO = LocalFileIO.create();
        StatsFile statsFile = new StatsFile(fileIO, statsPathFactory);
        StatisticsSidecarFile sidecarFile = new StatisticsSidecarFile(fileIO, sidecarPathFactory);
        StatsFileHandler statsFileHandler =
                new StatsFileHandler(null, null, statsFile, sidecarFile);

        List<StatisticsBlobMetadata> blobMetadata =
                statsFileHandler.writeSidecar(
                        Collections.singletonList(
                                new StatisticsBlob(
                                        "test-type",
                                        Collections.singletonList(1),
                                        10L,
                                        20L,
                                        Collections.emptyMap(),
                                        "payload".getBytes(StandardCharsets.UTF_8))));
        Statistics stats =
                new Statistics(10L, 0L, 100L, 1000L, Collections.emptyMap(), blobMetadata);
        String statsFileName = statsFile.write(stats);

        assertThat(statsFile.exists(statsFileName)).isTrue();
        assertThat(sidecarFile.exists(blobMetadata.get(0).fileLocation())).isTrue();
        assertThat(blobMetadata.get(0).fileLocation())
                .startsWith(FileStorePathFactory.STATISTICS_SIDECAR_PREFIX);
        assertThat(statsFileHandler.readSidecar(blobMetadata.get(0)))
                .containsExactly("payload".getBytes(StandardCharsets.UTF_8));

        statsFileHandler.deleteStats(statsFileName);

        assertThat(statsFile.exists(statsFileName)).isFalse();
        assertThat(sidecarFile.exists(blobMetadata.get(0).fileLocation())).isFalse();
    }

    @Test
    public void testRejectEmptyBlobs() {
        StatisticsSidecarFile sidecarFile =
                new StatisticsSidecarFile(
                        LocalFileIO.create(),
                        pathFactory(FileStorePathFactory.STATISTICS_SIDECAR_PREFIX));

        assertThatThrownBy(() -> sidecarFile.write(Collections.emptyList()))
                .hasMessageContaining("must contain at least one blob");
    }

    @Test
    public void testRejectOutOfRangeBlobMetadata() {
        StatisticsSidecarFile sidecarFile =
                new StatisticsSidecarFile(
                        LocalFileIO.create(),
                        pathFactory(FileStorePathFactory.STATISTICS_SIDECAR_PREFIX));
        StatisticsBlob blob =
                new StatisticsBlob(
                        "test-type",
                        Collections.singletonList(1),
                        10L,
                        20L,
                        Collections.emptyMap(),
                        "payload".getBytes(StandardCharsets.UTF_8));

        StatisticsBlobMetadata metadata = sidecarFile.write(Collections.singletonList(blob)).get(0);
        StatisticsBlobMetadata outOfRangeMetadata =
                new StatisticsBlobMetadata(
                        metadata.type(),
                        metadata.fieldIds(),
                        metadata.snapshotId().getAsLong(),
                        metadata.sequenceNumber().getAsLong(),
                        metadata.properties(),
                        metadata.fileLocation(),
                        metadata.offset().getAsLong(),
                        1024L);

        assertThatThrownBy(() -> sidecarFile.read(outOfRangeMetadata))
                .hasMessageContaining("range exceeds sidecar file length");
    }

    private PathFactory pathFactory(String prefix) {
        Path dir = new Path(tempPath.toUri());
        return new PathFactory() {
            @Override
            public Path newPath() {
                return new Path(dir, prefix + UUID.randomUUID());
            }

            @Override
            public Path toPath(String fileName) {
                return new Path(dir, fileName);
            }
        };
    }
}
