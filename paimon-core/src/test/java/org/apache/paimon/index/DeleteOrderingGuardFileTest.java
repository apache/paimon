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

package org.apache.paimon.index;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.KeyValue;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.cache.CacheManager;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

/** Tests immutable guard SST segments, including duplicate keys, corruption, and abort cleanup. */
class DeleteOrderingGuardFileTest {

    @TempDir java.nio.file.Path tempDir;

    private final CacheManager cacheManager =
            new CacheManager(MemorySize.ofBytes(1024 * 1024), 0.2);

    @AfterEach
    void closeCache() throws Exception {
        cacheManager.close();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testMultipleKeysAndRetractsAcrossBlocks(boolean externalPath) throws Exception {
        DeleteOrderingGuardFile file = file(LocalFileIO.create(), serializer(7), externalPath);
        List<DeleteOrderingGuard> input = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            input.add(
                    guard(
                            i / 10 - 50,
                            Long.MAX_VALUE - i,
                            i % 2 == 0 ? RowKind.DELETE : RowKind.UPDATE_BEFORE,
                            i % 3 == 0 ? null : i));
        }
        IndexFileMeta meta = write(file, input);
        assertThat(meta.indexType()).isEqualTo(DeleteOrderingGuardFile.DELETE_ORDERING_GUARD);
        assertThat(meta.rowCount()).isEqualTo(input.size());
        assertThat(meta.fileSize()).isEqualTo(file.fileSize(meta));
        assertThat(meta.externalPath()).isEqualTo(externalPath ? file.path(meta).toString() : null);

        List<DeleteOrderingGuard> result = read(file, meta);
        assertThat(result).hasSize(input.size());
        for (int i = 0; i < result.size(); i++) {
            DeleteOrderingGuard actual = result.get(i);
            DeleteOrderingGuard expected = input.get(i);
            assertThat(actual.key().getInt(0)).isEqualTo(expected.key().getInt(0));
            assertThat(actual.ordering().isNullAt(0)).isEqualTo(expected.ordering().isNullAt(0));
            if (!actual.ordering().isNullAt(0)) {
                assertThat(actual.ordering().getInt(0)).isEqualTo(expected.ordering().getInt(0));
            }
            assertThat(actual.sequenceNumber()).isEqualTo(expected.sequenceNumber());
            assertThat(actual.valueKind()).isEqualTo(expected.valueKind());
            assertThat(actual.level()).isEqualTo(expected.level());
        }
    }

    @Test
    void testNewSegmentLeavesPreviousSegmentUnchanged() throws Exception {
        DeleteOrderingGuardFile file = file(LocalFileIO.create(), serializer(7), false);
        IndexFileMeta first =
                write(file, Collections.singletonList(guard(1, 4, RowKind.DELETE, 4)));
        byte[] original = Files.readAllBytes(Paths.get(file.path(first).toUri()));
        IndexFileMeta second =
                write(file, Collections.singletonList(guard(1, 7, RowKind.DELETE, 7)));

        assertThat(second.fileName()).isNotEqualTo(first.fileName());
        assertThat(Files.readAllBytes(Paths.get(file.path(first).toUri()))).isEqualTo(original);
        assertThat(read(file, first).get(0).ordering().getInt(0)).isEqualTo(4);
        assertThat(read(file, second).get(0).ordering().getInt(0)).isEqualTo(7);
    }

    @Test
    void testPayloadDoesNotReachSegment() throws Exception {
        DeleteOrderingGuardSerializer serializer = serializer(7);
        DeleteOrderingGuardFile file = file(LocalFileIO.create(), serializer, false);
        String payload = "this-private-delete-payload-must-never-be-stored-in-guard-files";
        DeleteOrderingGuard guard =
                serializer.extract(
                        new KeyValue()
                                .replace(
                                        GenericRow.of(1),
                                        4,
                                        RowKind.DELETE,
                                        GenericRow.of(4, BinaryString.fromString(payload))));
        IndexFileMeta meta = write(file, Collections.singletonList(guard));

        assertThat(
                        new String(
                                Files.readAllBytes(Paths.get(file.path(meta).toUri())),
                                StandardCharsets.ISO_8859_1))
                .doesNotContain(payload);
        assertThat(read(file, meta).get(0).ordering().getFieldCount()).isEqualTo(1);
    }

    @Test
    void testCompositeKeyAndGroupMetadataRoundTrip() throws Exception {
        RowType keyType = RowType.of(DataTypes.BYTES(), RowType.of(DataTypes.INT()));
        RowType valueType =
                RowType.builder()
                        .field("a", DataTypes.INT())
                        .field("b", RowType.of(DataTypes.INT(), DataTypes.BIGINT()))
                        .field("payload", DataTypes.BLOB())
                        .build();
        Options options = new Options();
        options.set(CoreOptions.MERGE_ENGINE, CoreOptions.MergeEngine.PARTIAL_UPDATE);
        options.set("fields.a,b.sequence-group", "payload");
        options.set("fields.a,b.sequence-group.sort-order", "descending");
        DeleteOrderingGuardSerializer serializer =
                new DeleteOrderingGuardSerializer(7, keyType, valueType, new CoreOptions(options));
        DeleteOrderingGuardFile file = file(LocalFileIO.create(), serializer, false);
        List<DeleteOrderingGuard> input = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            input.add(
                    serializer.extract(
                            new KeyValue()
                                    .replace(
                                            GenericRow.of(new byte[] {1, 2}, GenericRow.of(i)),
                                            i,
                                            RowKind.DELETE,
                                            GenericRow.of(null, GenericRow.of(4, 7L), null))));
        }
        List<DeleteOrderingGuard> result = read(file, write(file, input));

        assertThat(result).hasSize(20);
        for (int i = 0; i < result.size(); i++) {
            assertThat(result.get(i).key().getBinary(0)).containsExactly(1, 2);
            assertThat(result.get(i).key().getRow(1, 1).getInt(0)).isEqualTo(i);
            assertThat(result.get(i).ordering().isNullAt(0)).isTrue();
            assertThat(result.get(i).ordering().getRow(1, 2).getInt(0)).isEqualTo(4);
            assertThat(result.get(i).ordering().getRow(1, 2).getLong(1)).isEqualTo(7L);
        }
    }

    @Test
    void testEmptyCompactionCreatesNoFile() throws Exception {
        DeleteOrderingGuardFile file = file(LocalFileIO.create(), serializer(7), false);
        assertThat(write(file, Collections.emptyList())).isNull();
        assertThat(tempDir).isEmptyDirectory();
    }

    @Test
    void testUnfinishedCloseAbortsSegment() throws Exception {
        DeleteOrderingGuardFile file = file(LocalFileIO.create(), serializer(7), false);
        try (DeleteOrderingGuardFile.Writer writer = file.createWriter()) {
            writer.write(guard(1, 4, RowKind.DELETE, 4));
            assertThat(tempDir).isDirectoryContaining(path -> true);
        }
        assertThat(tempDir).isEmptyDirectory();
    }

    @Test
    void testAbortAfterFinishPreservesOtherSegments() throws Exception {
        DeleteOrderingGuardFile file = file(LocalFileIO.create(), serializer(7), false);
        IndexFileMeta previous =
                write(file, Collections.singletonList(guard(1, 4, RowKind.DELETE, 4)));
        try (DeleteOrderingGuardFile.Writer writer = file.createWriter()) {
            writer.write(guard(2, 7, RowKind.DELETE, 7));
            IndexFileMeta aborted = writer.finish();
            assertThat(file.exists(aborted)).isTrue();
            writer.abort();
            writer.abort();
            assertThat(file.exists(aborted)).isFalse();
            assertThatThrownBy(writer::finish).hasMessageContaining("aborted");
        }
        assertThat(read(file, previous)).hasSize(1);
    }

    @Test
    void testUnsortedInputAbortsSegment() throws Exception {
        DeleteOrderingGuardFile file = file(LocalFileIO.create(), serializer(7), false);
        assertThatThrownBy(
                        () ->
                                write(
                                        file,
                                        Arrays.asList(
                                                guard(2, 4, RowKind.DELETE, 4),
                                                guard(1, 7, RowKind.DELETE, 7))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("sorted by primary key");
        assertThat(tempDir).isEmptyDirectory();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testWriteAndFinishFailuresAbortSegment(boolean failDuringWrite) throws Exception {
        AtomicBoolean fail = new AtomicBoolean(!failDuringWrite);
        LocalFileIO fileIO =
                new LocalFileIO() {
                    @Override
                    public PositionOutputStream newOutputStream(Path path, boolean overwrite)
                            throws IOException {
                        PositionOutputStream output = spy(super.newOutputStream(path, overwrite));
                        if (failDuringWrite) {
                            doAnswer(
                                            invocation -> {
                                                if (fail.get()) {
                                                    throw new IOException("injected write failure");
                                                }
                                                return invocation.callRealMethod();
                                            })
                                    .when(output)
                                    .write(any(byte[].class), anyInt(), anyInt());
                        } else {
                            doAnswer(
                                            invocation -> {
                                                invocation.callRealMethod();
                                                if (fail.getAndSet(false)) {
                                                    throw new IOException("injected close failure");
                                                }
                                                return null;
                                            })
                                    .when(output)
                                    .close();
                        }
                        return output;
                    }
                };
        DeleteOrderingGuardFile file = file(fileIO, serializer(7), false);
        try (DeleteOrderingGuardFile.Writer writer = file.createWriter()) {
            writer.write(guard(1, 4, RowKind.DELETE, 4));
            if (failDuringWrite) {
                fail.set(true);
                assertThatThrownBy(() -> writer.write(guard(2, 7, RowKind.DELETE, 7)))
                        .isInstanceOf(IOException.class)
                        .hasMessage("injected write failure");
            } else {
                assertThatThrownBy(writer::finish)
                        .isInstanceOf(IOException.class)
                        .hasMessage("injected close failure");
            }
            assertThatThrownBy(writer::finish).hasMessageContaining("aborted");
        }
        assertThat(tempDir).isEmptyDirectory();
    }

    @Test
    void testSchemaMismatchRejected() throws Exception {
        DeleteOrderingGuardFile file = file(LocalFileIO.create(), serializer(7), false);
        IndexFileMeta meta = write(file, Collections.singletonList(guard(1, 4, RowKind.DELETE, 4)));
        DeleteOrderingGuardFile wrongSchema = file(LocalFileIO.create(), serializer(8), false);
        assertThatThrownBy(() -> wrongSchema.read(meta))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("schema mismatch: expected 8, found 7");
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 4})
    void testInvalidHeaderRejected(int offset) throws Exception {
        DeleteOrderingGuardFile file = file(LocalFileIO.create(), serializer(7), false);
        IndexFileMeta meta = write(file, Collections.singletonList(guard(1, 4, RowKind.DELETE, 4)));
        try (RandomAccessFile output =
                new RandomAccessFile(Paths.get(file.path(meta).toUri()).toFile(), "rw")) {
            output.seek(offset);
            output.writeInt(-1);
        }
        assertThatThrownBy(() -> file.read(meta))
                .isInstanceOf(IOException.class)
                .hasMessageContaining(offset == 0 ? "magic" : "version");
    }

    @Test
    void testCorruptDataBlockFailsChecksum() throws Exception {
        DeleteOrderingGuardFile file = file(LocalFileIO.create(), serializer(7), false);
        IndexFileMeta meta = write(file, Collections.singletonList(guard(1, 4, RowKind.DELETE, 4)));
        try (RandomAccessFile output =
                new RandomAccessFile(Paths.get(file.path(meta).toUri()).toFile(), "rw")) {
            output.seek(20);
            int original = output.read();
            output.seek(20);
            output.write(original ^ 1);
        }
        assertThatThrownBy(() -> read(file, meta))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("CRC32C");
    }

    private DeleteOrderingGuardFile file(
            FileIO fileIO, DeleteOrderingGuardSerializer serializer, boolean externalPath) {
        Path dir = new Path(tempDir.toUri());
        return new DeleteOrderingGuardFile(
                fileIO,
                new IndexPathFactory() {
                    @Override
                    public Path toPath(String fileName) {
                        return new Path(dir, fileName);
                    }

                    @Override
                    public Path newPath() {
                        return new Path(dir, UUID.randomUUID().toString());
                    }

                    @Override
                    public boolean isExternalPath() {
                        return externalPath;
                    }
                },
                serializer,
                cacheManager,
                128);
    }

    private static DeleteOrderingGuardSerializer serializer(long schemaId) {
        Options options = new Options();
        options.set(CoreOptions.SEQUENCE_FIELD, "f0");
        return new DeleteOrderingGuardSerializer(
                schemaId,
                RowType.of(DataTypes.INT()),
                RowType.of(DataTypes.INT(), DataTypes.STRING()),
                new CoreOptions(options));
    }

    private static DeleteOrderingGuard guard(int key, long sequence, RowKind kind, Integer source) {
        return new DeleteOrderingGuard(
                GenericRow.of(key), GenericRow.of(source), sequence, kind, 3);
    }

    private static IndexFileMeta write(
            DeleteOrderingGuardFile file, List<DeleteOrderingGuard> guards) throws IOException {
        try (DeleteOrderingGuardFile.Writer writer = file.createWriter()) {
            for (DeleteOrderingGuard guard : guards) {
                writer.write(guard);
            }
            return writer.finish();
        }
    }

    private static List<DeleteOrderingGuard> read(DeleteOrderingGuardFile file, IndexFileMeta meta)
            throws IOException {
        List<DeleteOrderingGuard> guards = new ArrayList<>();
        RecordReader<DeleteOrderingGuard> reader = file.read(meta);
        reader.forEachRemaining(guards::add);
        return guards;
    }
}
