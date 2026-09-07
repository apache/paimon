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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.ManifestAvroReader;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestEntrySerializer;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.ManifestFileMetaTestBase;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ManifestFileMerger}. */
public class ManifestFileMergerTest extends ManifestFileMetaTestBase {

    private static final RowType NO_PARTITION_TYPE = RowType.of();

    @TempDir java.nio.file.Path tempDir;
    private ManifestFile manifestFile;

    @BeforeEach
    public void beforeEach() {
        manifestFile = createManifestFile(tempDir.toString());
    }

    @Test
    public void testManifestSortFallsBackToForcedLegacyMergeWithoutRowId() {
        List<ManifestFileMeta> input =
                Arrays.asList(
                        makeManifest(makeEntry(true, "base", null)),
                        makeManifest(
                                makeEntry(false, "base", null),
                                makeEntry(true, "replacement", null)));

        Options options = new Options();
        options.set(CoreOptions.MANIFEST_SORT_ENABLED, true);
        options.set(CoreOptions.DATA_EVOLUTION_ENABLED, true);
        options.set(CoreOptions.MANIFEST_MERGE_MIN_COUNT, 100);
        options.set(CoreOptions.MANIFEST_FULL_COMPACTION_FILE_SIZE.key(), Long.MAX_VALUE + "B");
        CoreOptions tableOptions = new CoreOptions(options);

        assertThat(ManifestFileMerger.canUseManifestSort(input, NO_PARTITION_TYPE, tableOptions))
                .isFalse();

        CoreOptions compactOptions =
                FileStoreCommitImpl.manifestCompactionOptions(
                        tableOptions, input, NO_PARTITION_TYPE);
        assertThat(compactOptions.manifestMergeMinCount()).isEqualTo(1);
        assertThat(compactOptions.manifestFullCompactionThresholdSize().getBytes()).isEqualTo(1);

        List<ManifestFileMeta> merged =
                ManifestFileMerger.merge(input, manifestFile, NO_PARTITION_TYPE, compactOptions);
        List<ManifestEntry> mergedEntries =
                merged.stream()
                        .flatMap(
                                meta ->
                                        manifestFile.read(meta.fileName(), meta.fileSize())
                                                .stream())
                        .collect(Collectors.toList());
        assertThat(mergedEntries).noneMatch(entry -> entry.kind() == FileKind.DELETE);
        assertThat(mergedEntries)
                .extracting(entry -> entry.file().fileName())
                .containsExactly("replacement");
    }

    @Test
    public void testFullCompactionWithStableLegacyManifestBlocks() throws Exception {
        List<ManifestEntry> baseEntries = new ArrayList<>();

        // Pending blocks contain many short records and therefore have a larger record count.
        for (int i = 0; i < 2_000; i++) {
            baseEntries.add(makeEntry(true, "short-" + i, null));
        }

        // Current blocks contain fewer long records while retaining a similar decompressed size.
        String longName = String.join("", Collections.nCopies(2_048, "x"));
        for (int i = 0; i < 200; i++) {
            baseEntries.add(makeEntry(true, "long-" + i + "-" + longName, null));
        }

        ManifestFileMeta base = makeLegacyManifest(baseEntries);

        List<Long> blockRecordCounts = new ArrayList<>();
        try (ManifestAvroReader reader =
                manifestFile.scanAvroBlocks(base.fileName(), base.fileSize())) {
            assertThat(reader.rawBlockCopySupported()).isFalse();

            while (reader.hasNext()) {
                blockRecordCounts.add(reader.next().recordCount());
            }
        }

        assertThat(blockRecordCounts.size()).isGreaterThan(2);

        // Find the first block whose record count decreases because it contains long file names.
        int changedBlock = -1;
        long changedBlockStart = 0;
        long blockStart = blockRecordCounts.get(0);

        for (int i = 1; i < blockRecordCounts.size() - 1; i++) {
            if (blockRecordCounts.get(i) < blockRecordCounts.get(i - 1)) {
                changedBlock = i;
                changedBlockStart = blockStart;
                break;
            }

            blockStart += blockRecordCounts.get(i);
        }

        assertThat(changedBlock).isGreaterThan(0);

        // Delete an entry from the middle of the selected block so that it must be rewritten.
        int deletedEntryIndex =
                Math.toIntExact(changedBlockStart + blockRecordCounts.get(changedBlock) / 2);

        String deletedFileName = baseEntries.get(deletedEntryIndex).fileName();
        ManifestFileMeta delta = makeManifest(makeEntry(false, deletedFileName, null));

        List<ManifestFileMeta> newFilesForAbort = new ArrayList<>();
        Optional<List<ManifestFileMeta>> compacted =
                ManifestFileBlockMerger.tryFullCompaction(
                        Arrays.asList(base, delta),
                        newFilesForAbort,
                        manifestFile,
                        1,
                        1,
                        NO_PARTITION_TYPE,
                        1);

        assertThat(compacted).isPresent();

        List<ManifestEntry> actual =
                compacted.get().stream()
                        .flatMap(
                                meta ->
                                        manifestFile.read(meta.fileName(), meta.fileSize())
                                                .stream())
                        .collect(Collectors.toList());

        assertThat(actual.size()).isEqualTo(baseEntries.size() - 1);
        assertThat(actual).allMatch(entry -> entry.kind() == FileKind.ADD);
        assertThat(actual)
                .extracting(entry -> entry.file().fileName())
                .doesNotContain(deletedFileName);
    }

    @Override
    public ManifestFile getManifestFile() {
        return manifestFile;
    }

    @Override
    public RowType getPartitionType() {
        return NO_PARTITION_TYPE;
    }

    private ManifestFileMeta makeLegacyManifest(List<ManifestEntry> entries) throws Exception {
        ManifestFileMeta current = makeManifest(entries.toArray(new ManifestEntry[0]));

        RowType legacyFileType =
                DataFileMeta.SCHEMA.project(
                        new int[] {
                            0, 1, 2, 3, 4, 5, 6, 7, 8,
                            9, 10, 11, 12, 13, 14, 15, 16, 17
                        });

        List<DataField> legacyManifestFields =
                ManifestEntry.MANIFEST_ROW_TYPE.getFields().stream()
                        .map(
                                field ->
                                        ManifestEntry.FILE.equals(field.name())
                                                ? field.newType(legacyFileType)
                                                : field)
                        .collect(Collectors.toList());

        RowType legacyManifestType = new RowType(false, legacyManifestFields);

        Path path = new Path(new Path(tempDir.toUri()), "manifest/" + current.fileName());

        FileIO fileIO = LocalFileIO.create();
        ManifestEntrySerializer serializer = new ManifestEntrySerializer();

        try (PositionOutputStream out = fileIO.newOutputStream(path, true);
                FormatWriter writer =
                        avro.createWriterFactory(legacyManifestType).create(out, "zstd")) {
            for (ManifestEntry entry : entries) {
                writer.addElement(serializer.toRow(entry));
            }
        }

        return new ManifestFileMeta(
                current.fileName(),
                fileIO.getFileStatus(path).getLen(),
                current.numAddedFiles(),
                current.numDeletedFiles(),
                current.partitionStats(),
                current.schemaId(),
                current.minBucket(),
                current.maxBucket(),
                current.minLevel(),
                current.maxLevel(),
                current.minRowId(),
                current.maxRowId());
    }
}
