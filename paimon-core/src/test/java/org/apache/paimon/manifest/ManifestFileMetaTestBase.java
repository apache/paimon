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

package org.apache.paimon.manifest;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileIOFinder;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.stats.StatsTestUtils;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileStorePathFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.paimon.io.DataFileTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/** base class for Test {@link ManifestFile}. */
public abstract class ManifestFileMetaTestBase {

    protected final FileFormat avro = FileFormat.fromIdentifier("avro", new Options());
    protected String manifestFileNameTemplate = "%d-%d";

    protected ManifestEntry makeEntry(boolean isAdd, String fileName) {
        return makeEntry(isAdd, fileName, 0);
    }

    protected ManifestEntry makeEntry(boolean isAdd, String fileName, Integer partition) {
        BinaryRow binaryRow;
        if (partition != null) {
            binaryRow = new BinaryRow(1);
            BinaryRowWriter writer = new BinaryRowWriter(binaryRow);
            writer.writeInt(0, partition);
            writer.complete();
        } else {
            binaryRow = BinaryRow.EMPTY_ROW;
        }

        return new ManifestEntry(
                isAdd ? FileKind.ADD : FileKind.DELETE,
                binaryRow,
                0, // not used
                0, // not used
                new DataFileMeta(
                        fileName,
                        0, // not used
                        0, // not used
                        binaryRow, // not used
                        binaryRow, // not used
                        StatsTestUtils.newEmptySimpleStats(), // not used
                        StatsTestUtils.newEmptySimpleStats(), // not used
                        0, // not used
                        0, // not used
                        0, // not used
                        0, // not used
                        Collections.emptyList(),
                        Timestamp.fromEpochMillis(200000),
                        0L, // not used
                        null, // not used
                        FileSource.APPEND));
    }

    protected ManifestFileMeta makeManifest(ManifestEntry... entries) {
        return getManifestFile().write(Arrays.asList(entries)).get(0);
    }

    abstract ManifestFile getManifestFile();

    abstract RowType getPartitionType();

    protected void assertEquivalentEntries(
            List<ManifestFileMeta> input, List<ManifestFileMeta> merged) {
        List<ManifestEntry> inputEntry =
                input.stream()
                        .flatMap(f -> getManifestFile().read(f.fileName(), f.fileSize()).stream())
                        .collect(Collectors.toList());
        List<String> entryBeforeMerge =
                FileEntry.mergeEntries(inputEntry).stream()
                        .filter(entry -> entry.kind() == FileKind.ADD)
                        .map(entry -> entry.kind() + "-" + entry.file().fileName())
                        .collect(Collectors.toList());

        List<String> entryAfterMerge = new ArrayList<>();
        for (ManifestFileMeta manifestFileMeta : merged) {
            List<ManifestEntry> entries =
                    getManifestFile()
                            .read(manifestFileMeta.fileName(), manifestFileMeta.fileSize());
            for (ManifestEntry entry : entries) {
                entryAfterMerge.add(entry.kind() + "-" + entry.file().fileName());
            }
        }

        assertThat(entryBeforeMerge).hasSameElementsAs(entryAfterMerge);
    }

    protected ManifestFile createManifestFile(String pathStr) {
        Path path = new Path(pathStr);
        FileIO fileIO = FileIOFinder.find(path);
        return new ManifestFile.Factory(
                        fileIO,
                        new SchemaManager(fileIO, path),
                        getPartitionType(),
                        avro,
                        "zstd",
                        new FileStorePathFactory(
                                path,
                                getPartitionType(),
                                "default",
                                CoreOptions.FILE_FORMAT.defaultValue().toString()),
                        Long.MAX_VALUE,
                        null)
                .create();
    }

    protected void containSameEntryFile(
            List<ManifestFileMeta> mergedMainfest, List<String> expecteded) {
        List<String> actual =
                mergedMainfest.stream()
                        .flatMap(
                                file ->
                                        getManifestFile().read(file.fileName(), file.fileSize())
                                                .stream())
                        .map(f -> f.kind() + "-" + f.file().fileName())
                        .collect(Collectors.toList());
        assertThat(actual).hasSameElementsAs(expecteded);
    }

    protected void assertSameContent(
            ManifestFileMeta expected, ManifestFileMeta actual, ManifestFile manifestFile) {
        // check meta
        assertThat(actual.numAddedFiles()).isEqualTo(expected.numAddedFiles());
        assertThat(actual.numDeletedFiles()).isEqualTo(expected.numDeletedFiles());
        assertThat(actual.partitionStats()).isEqualTo(expected.partitionStats());

        // check content
        assertThat(manifestFile.read(actual.fileName(), actual.fileSize()))
                .isEqualTo(manifestFile.read(expected.fileName(), expected.fileSize()));
    }

    protected List<ManifestFileMeta> createBaseManifestFileMetas(boolean hasPartition) {
        List<ManifestFileMeta> input = new ArrayList<>();
        // base with 3 partition ,16 entry each parition
        for (int j = 0; j < 3; j++) {
            List<ManifestEntry> entrys = new ArrayList<>();
            for (int i = 0; i < 16; i++) {
                Integer partition;
                if (hasPartition) {
                    partition = j;
                } else {
                    partition = null;
                }
                entrys.add(
                        makeEntry(true, String.format(manifestFileNameTemplate, j, i), partition));
            }
            input.add(makeManifest(entrys.toArray(new ManifestEntry[0])));
        }
        return input;
    }

    protected void addDeltaManifests(List<ManifestFileMeta> input, boolean hasPartition) {
        Integer parittion1 = null;
        if (hasPartition) {
            parittion1 = 1;
        }
        input.add(
                makeManifest(
                        makeEntry(false, "1-15", parittion1),
                        makeEntry(false, "1-14", parittion1),
                        makeEntry(true, "A", parittion1),
                        makeEntry(true, "B", parittion1),
                        makeEntry(true, "C", parittion1)));
        input.add(makeManifest(makeEntry(true, "D", parittion1)));
        input.add(
                makeManifest(
                        makeEntry(false, "A", parittion1),
                        makeEntry(false, "B", parittion1),
                        makeEntry(true, "F", parittion1)));
        input.add(
                makeManifest(
                        makeEntry(false, "C", parittion1),
                        makeEntry(false, "D", parittion1),
                        makeEntry(false, "F", parittion1),
                        makeEntry(true, "G", parittion1)));
        Integer partition2 = null;
        if (hasPartition) {
            partition2 = 2;
        }
        input.add(
                makeManifest(
                        makeEntry(false, "2-15", partition2),
                        makeEntry(false, "2-14", partition2),
                        makeEntry(true, "A2", partition2),
                        makeEntry(true, "B2", partition2),
                        makeEntry(true, "C2", partition2)));
        input.add(
                makeManifest(
                        makeEntry(false, "A2", partition2),
                        makeEntry(false, "B2", partition2),
                        makeEntry(true, "D2", partition2)));
    }

    public static ManifestEntry makeEntry(
            FileKind fileKind, int partition, int bucket, long rowCount) {
        return new ManifestEntry(
                fileKind,
                row(partition),
                bucket,
                0, // not used
                new DataFileMeta(
                        "", // not used
                        0, // not used
                        rowCount,
                        null, // not used
                        null, // not used
                        StatsTestUtils.newEmptySimpleStats(), // not used
                        StatsTestUtils.newEmptySimpleStats(), // not used
                        0, // not used
                        0, // not used
                        0, // not used
                        0, // not used
                        0L,
                        null,
                        FileSource.APPEND));
    }
}
