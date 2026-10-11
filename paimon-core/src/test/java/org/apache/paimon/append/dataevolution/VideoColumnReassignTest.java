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

package org.apache.paimon.append.dataevolution;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Blob;
import org.apache.paimon.data.BlobData;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.VideoFrameDescriptor;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.UriReader;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Deterministic interleavings of video-column backfill and row-ID reassignment. */
public class VideoColumnReassignTest extends TableTestBase {

    private final byte[] payload = new byte[] {11, 22, 33, 44};

    @ParameterizedTest
    @ValueSource(strings = {"a", "b"})
    void testReassignBeforeVideoBackfillCommit(String reassignedPartition) throws Exception {
        FileStoreTable table = createImagesAndAddVideo();
        String originalFile = normalFileAt(table, 0).fileName();
        List<CommitMessage> pending = prepareVideo(table, 0);
        DataEvolutionRowIdReassigner.Result result =
                new DataEvolutionRowIdReassigner(table, partition(table, reassignedPartition))
                        .reassign();
        assertThat(result.newSnapshotId).isGreaterThan(result.previousSnapshotId);
        long afterReassign = table.latestSnapshot().get().id();
        if (reassignedPartition.equals("a")) {
            assertThatThrownBy(() -> commitVideo(table, pending))
                    .hasMessageContaining("Row ID existence conflict");
            assertThat(table.latestSnapshot().get().id()).isEqualTo(afterReassign);
            assertRows(table, false);
            // Re-read the mapping and rebuild the update, rather than retry stale messages.
            long newRowId =
                    files(table).stream()
                            .filter(file -> file.fileName().equals(originalFile))
                            .findFirst()
                            .get()
                            .nonNullFirstRowId();
            assertThat(newRowId).isGreaterThanOrEqualTo(8);
            commitVideo(table, prepareVideo(table, newRowId));
        } else {
            // Reassignment of another partition must not invalidate this update.
            commitVideo(table, pending);
        }
        assertRows(table, true);
    }

    @Test
    void testVideoBackfillCommitsAfterReassignPlanning() throws Exception {
        FileStoreTable table = createImagesAndAddVideo();
        List<CommitMessage> pending = prepareVideo(table, 0);
        AtomicBoolean injected = new AtomicBoolean();
        DataEvolutionRowIdReassigner.Result result =
                new DataEvolutionRowIdReassigner(
                                table,
                                partition(table, "a"),
                                () -> {
                                    if (injected.compareAndSet(false, true)) {
                                        try {
                                            commitVideo(table, pending);
                                        } catch (Exception e) {
                                            throw new RuntimeException(e);
                                        }
                                    }
                                })
                        .reassign();
        assertThat(injected).isTrue();
        assertThat(result.newSnapshotId).isGreaterThan(result.previousSnapshotId);
        assertThat(files(table).stream().filter(f -> f.fileName().endsWith(".video")))
                .allMatch(f -> f.nonNullFirstRowId() >= 8);
        assertRows(table, true);
    }

    private FileStoreTable createImagesAndAddVideo() throws Exception {
        catalog.createTable(
                identifier(),
                Schema.newBuilder()
                        .column("pt", DataTypes.STRING())
                        .column("id", DataTypes.INT())
                        .column("image", DataTypes.BLOB())
                        .partitionKeys("pt")
                        .option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true")
                        .option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true")
                        .build(),
                false);
        FileStoreTable table = getTableDefault();
        for (int group = 0; group < 4; group++) {
            BatchWriteBuilder builder = table.newBatchWriteBuilder();
            try (BatchTableWrite write = builder.newWrite();
                    BatchTableCommit commit = builder.newCommit()) {
                for (int offset = 0; offset < 2; offset++) {
                    int id = group * 2 + offset;
                    write.write(
                            GenericRow.of(
                                    BinaryString.fromString(group % 2 == 0 ? "a" : "b"),
                                    id,
                                    new BlobData(new byte[] {(byte) id})));
                }
                commit.commit(write.prepareCommit());
            }
        }
        catalog.alterTable(
                identifier(),
                Arrays.asList(
                        SchemaChange.addColumn("video", DataTypes.BLOB()),
                        SchemaChange.setOption("video-frame-field", "video")),
                false);
        return getTableDefault();
    }

    private List<CommitMessage> prepareVideo(FileStoreTable table, long firstRowId)
            throws Exception {
        long snapshot = table.latestSnapshot().get().id();
        java.nio.file.Path source = tempPath.resolve("source.mp4");
        java.nio.file.Files.write(source, payload);
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite write =
                builder.newWrite()
                        .withWriteType(table.rowType().project(Arrays.asList("pt", "video")))) {
            for (int i = 0; i < 2; i++) {
                write.write(
                        GenericRow.of(
                                BinaryString.fromString("a"),
                                Blob.fromDescriptor(
                                        UriReader.fromFile(LocalFileIO.create()),
                                        new VideoFrameDescriptor(
                                                new Path(source.toUri()).toString(),
                                                0,
                                                payload.length,
                                                7 + i,
                                                -1,
                                                0))));
            }
            List<CommitMessage> result = new ArrayList<>();
            for (CommitMessage message : write.prepareCommit()) {
                CommitMessageImpl impl = (CommitMessageImpl) message;
                List<DataFileMeta> files = new ArrayList<>(impl.newFilesIncrement().newFiles());
                impl.newFilesIncrement().newFiles().clear();
                for (DataFileMeta file : files) {
                    impl.newFilesIncrement().newFiles().add(file.assignFirstRowId(firstRowId));
                }
                // Python update-by-row-id sends this base snapshot with its commit messages.
                result.add(impl.withCheckFromSnapshot(snapshot));
            }
            assertThat(result).isNotEmpty();
            return result;
        }
    }

    private void commitVideo(FileStoreTable table, List<CommitMessage> messages) throws Exception {
        try (BatchTableCommit commit = table.newBatchWriteBuilder().newCommit()) {
            commit.commit(messages);
        }
    }

    private void assertRows(FileStoreTable table, boolean backfilled) throws Exception {
        List<InternalRow> rows = read(table, Pair.of(CoreOptions.BLOB_AS_DESCRIPTOR, "true"));
        rows.sort((a, b) -> Integer.compare(a.getInt(1), b.getInt(1)));
        assertThat(rows).hasSize(8);
        for (int id = 0; id < 8; id++) {
            InternalRow row = rows.get(id);
            assertThat(row.getInt(1)).isEqualTo(id);
            assertThat(row.getBlob(2).toData()).containsExactly((byte) id);
            if (backfilled && id < 2) {
                VideoFrameDescriptor descriptor =
                        (VideoFrameDescriptor) row.getBlob(3).toDescriptor();
                assertThat(descriptor.frameIndex()).isEqualTo(7 + id);
                assertThat(descriptor.uri()).endsWith(".video");
                assertThat(row.getBlob(3).toData()).isEqualTo(payload);
            } else {
                assertThat(row.isNullAt(3)).isTrue();
            }
        }
    }

    private List<DataFileMeta> files(FileStoreTable table) {
        return table.store().newScan().plan().files().stream()
                .map(ManifestEntry::file)
                .collect(Collectors.toList());
    }

    private DataFileMeta normalFileAt(FileStoreTable table, long rowId) {
        return files(table).stream()
                .filter(f -> f.fileName().endsWith(".parquet") && f.nonNullFirstRowId() == rowId)
                .findFirst()
                .get();
    }

    private PartitionPredicate partition(FileStoreTable table, String value) {
        return PartitionPredicate.fromMaps(
                table.schema().logicalPartitionType(),
                Collections.singletonList(Collections.singletonMap("pt", value)),
                table.coreOptions().partitionDefaultName());
    }
}
