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

package org.apache.paimon.operation;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.CollectedDeletes;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.ManifestFileMetaTestBase;
import org.apache.paimon.stats.StatsTestUtils;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ManifestEntryRunMerge}. */
class ManifestEntryRunMergeTest extends ManifestFileMetaTestBase {

    private static final int ENTRY_COUNT = 25_001;

    @TempDir java.nio.file.Path tempDir;

    private final RowType partitionType = RowType.of(DataTypes.INT());
    private final BinaryRow partition = new BinaryRow(1);
    private ManifestFile manifestFile;

    @BeforeEach
    void beforeEach() {
        BinaryRowWriter writer = new BinaryRowWriter(partition);
        writer.writeInt(0, 0);
        writer.complete();
        manifestFile = createManifestFile(tempDir.toString());
    }

    @Test
    void testLargeFragmentedManifestsUseRunMerge() throws Exception {
        List<ManifestEntry> firstManifest = new ArrayList<>();
        List<ManifestEntry> secondManifest = new ArrayList<>();
        for (long firstRowId = ENTRY_COUNT - 1L; firstRowId >= 0; firstRowId--) {
            ManifestEntry entry = rowIdEntry("row-" + firstRowId, firstRowId);
            (firstRowId >= ENTRY_COUNT / 2 ? firstManifest : secondManifest).add(entry);
        }

        List<ManifestFileMeta> manifests = new ArrayList<>();
        manifests.add(makeManifest(firstManifest.toArray(new ManifestEntry[0])));
        manifests.add(makeManifest(secondManifest.toArray(new ManifestEntry[0])));
        ManifestFileSorter.RowIdEntrySortKey sortKey =
                (ManifestFileSorter.RowIdEntrySortKey)
                        ManifestFileSorter.createSortKey(true, manifests, null, partitionType);

        CollectedDeletes deletes = new CollectedDeletes(true);
        List<ManifestFileMeta> output;
        try {
            output =
                    ManifestEntryRunMerge.sortAndWriteFullEntries(
                            manifests,
                            sortKey,
                            partitionType,
                            manifestFile,
                            new ArrayList<>(),
                            deletes,
                            128,
                            1);
        } finally {
            deletes.release();
        }

        assertThat(output).isNotNull();
        assertThat(
                        output.stream()
                                .flatMap(
                                        meta ->
                                                manifestFile.read(meta.fileName(), meta.fileSize())
                                                        .stream())
                                .map(entry -> entry.file().nonNullFirstRowId())
                                .collect(Collectors.toList()))
                .containsExactlyElementsOf(
                        java.util.stream.LongStream.range(0, ENTRY_COUNT)
                                .boxed()
                                .collect(Collectors.toList()));
    }

    private ManifestEntry rowIdEntry(String fileName, long firstRowId) {
        return ManifestEntry.create(
                FileKind.ADD,
                partition,
                0,
                0,
                DataFileMeta.create(
                        fileName,
                        0,
                        1,
                        partition,
                        partition,
                        StatsTestUtils.newEmptySimpleStats(),
                        StatsTestUtils.newEmptySimpleStats(),
                        0,
                        0,
                        0,
                        0,
                        Collections.emptyList(),
                        Timestamp.fromEpochMillis(200000),
                        0L,
                        null,
                        FileSource.APPEND,
                        null,
                        null,
                        firstRowId,
                        Collections.singletonList("f0"),
                        null));
    }

    @Override
    protected ManifestFile getManifestFile() {
        return manifestFile;
    }

    @Override
    protected RowType getPartitionType() {
        return partitionType;
    }
}
