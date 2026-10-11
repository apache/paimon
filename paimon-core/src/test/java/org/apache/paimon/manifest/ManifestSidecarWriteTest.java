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
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.PositionOutputStreamWrapper;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.FileSystemSchemaManager;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RowRangeIndex;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiPredicate;
import java.util.stream.Stream;

import static org.apache.paimon.TestKeyValueGenerator.DEFAULT_PART_TYPE;
import static org.apache.paimon.utils.VarLengthIntUtils.decodeInt;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests automatic generation and ownership of manifest sidecars. */
class ManifestSidecarWriteTest {

    @TempDir java.nio.file.Path temp;
    private final ManifestTestDataGenerator gen = ManifestTestDataGenerator.builder().build();

    @Test
    void sidecarOptionControlsWriteIO() {
        for (Boolean sort : new Boolean[] {null, false, true}) {
            for (Boolean configured : new Boolean[] {null, false, true}) {
                Options options = new Options();
                if (sort != null) {
                    options.set(CoreOptions.MANIFEST_SORT_ENABLED, sort);
                }
                if (configured != null) {
                    options.set(CoreOptions.MANIFEST_SIDECAR_ENABLED, configured);
                }
                AtomicInteger reads = new AtomicInteger();
                FileIO io =
                        new LocalFileIO() {

                            @Override
                            public SeekableInputStream newInputStream(Path path)
                                    throws IOException {
                                reads.incrementAndGet();
                                return super.newInputStream(path);
                            }
                        };
                Path root = root(sort + "-" + configured);
                ManifestFile manifests =
                        manifests(root, io, DEFAULT_PART_TYPE, Long.MAX_VALUE, options);
                ManifestFileMeta meta =
                        manifests.write(Collections.singletonList(gen.next())).get(0);
                boolean enabled = configured == null ? Boolean.TRUE.equals(sort) : configured;
                assertThat(reads.get()).isEqualTo(enabled ? 1 : 0);
                assertThat(ManifestSidecar.fileName(meta) != null).isEqualTo(enabled);
            }
        }
    }

    @Test
    void rollingAndRawRewritesGenerateTheirOwnSidecars() throws Exception {
        Options options = enabledOptions();
        options.set(CoreOptions.MANIFEST_SORT_ENABLED, true);
        FileIO io = LocalFileIO.create();
        Path root = root("rolling");
        ManifestFile manifests = manifests(root, io, DEFAULT_PART_TYPE, 1, options);
        List<ManifestFileMeta> metas = manifests.write(entries(2200));
        assertThat(metas.size()).isGreaterThan(1);
        for (ManifestFileMeta meta : metas) {
            assertThat(ManifestSidecar.fileName(meta))
                    .isEqualTo(meta.fileName() + ManifestSidecar.SUFFIX);
            List<ManifestEntry> actual = manifests.read(meta.fileName());
            for (ManifestEntry entry :
                    Arrays.asList(actual.get(0), actual.get(actual.size() - 1))) {
                long row = entry.file().firstRowId();
                assertThat(select(root, io, meta, DEFAULT_PART_TYPE, point(row), null).blocks())
                        .isNotEmpty();
            }
            long gap = actual.get(0).file().firstRowId() + actual.get(0).file().rowCount();
            assertThat(select(root, io, meta, DEFAULT_PART_TYPE, point(gap), null).blocks())
                    .isEmpty();
        }

        ManifestFileMeta source = metas.get(0);
        Path rewrittenPath = new Path(new Path(root, "manifest"), "explicit-rewrite");
        ManifestAvroWriter writer = manifests.createAvroWriter(rewrittenPath);
        try (ManifestAvroReader reader =
                manifests.scanAvroBlocks(source.fileName(), source.fileSize())) {
            writer.writeEncodedManifest(reader, source);
        }
        assertThatThrownBy(writer::result).isInstanceOf(IllegalStateException.class);
        writer.close();
        ManifestFileMeta rewritten = writer.result().get(0);
        assertThat(ManifestSidecar.fileName(rewritten))
                .isEqualTo("explicit-rewrite" + ManifestSidecar.SUFFIX);
        assertThat(manifests.read(rewritten.fileName()))
                .isEqualTo(manifests.read(source.fileName()));
        long outside = metas.get(metas.size() - 1).maxRowId();
        assertThat(select(root, io, rewritten, DEFAULT_PART_TYPE, point(outside), null).blocks())
                .isEmpty();
        writer.abort();
        assertThat(io.exists(rewrittenPath)).isFalse();
        assertThat(io.exists(ManifestSidecar.path(rewrittenPath))).isFalse();
        assertThat(io.exists(manifestPath(root, source))).isTrue();
        for (ManifestFileMeta meta : metas) {
            manifests.delete(meta);
            assertThat(io.exists(ManifestSidecar.path(manifestPath(root, meta)))).isFalse();
        }
    }

    @Test
    void payloadsFollowTableMetadata() throws Exception {
        for (boolean partitioned : new boolean[] {false, true}) {
            for (boolean evolution : new boolean[] {false, true}) {
                for (int bucket : new int[] {-2, -1, 4}) {
                    Options options = enabledOptions();
                    options.set(CoreOptions.DATA_EVOLUTION_ENABLED, evolution);
                    options.set(CoreOptions.BUCKET, bucket);
                    RowType partitionType = partitioned ? DEFAULT_PART_TYPE : RowType.of();
                    Path root = root(partitioned + "-" + evolution + "-" + bucket);
                    FileIO io = LocalFileIO.create();
                    ManifestFile manifests =
                            manifests(root, io, partitionType, Long.MAX_VALUE, options);
                    ManifestEntry source = gen.next();
                    ManifestEntry entry =
                            ManifestEntry.create(
                                    FileKind.ADD,
                                    partitioned ? source.partition() : BinaryRow.EMPTY_ROW,
                                    1,
                                    4,
                                    source.file().newFirstRowId(100L));
                    ManifestFileMeta meta =
                            manifests.write(Collections.singletonList(entry)).get(0);
                    byte[] bytes =
                            Files.readAllBytes(
                                    java.nio.file.Paths.get(
                                            ManifestSidecar.path(manifestPath(root, meta))
                                                    .toString()));
                    ByteBuffer in = ByteBuffer.wrap(bytes);
                    in.getInt();
                    decodeInt(in);
                    int headerLength = decodeInt(in);
                    in.position(in.position() + headerLength);
                    assertThat(decodeInt(in)).isEqualTo(1);
                    assertThat(select(root, io, meta, partitionType, point(99), null).blocks())
                            .hasSize(evolution ? 0 : 1);
                    assertThat(
                                    select(root, io, meta, partitionType, null, (b, t) -> b == 99)
                                            .blocks())
                            .hasSize(bucket == -1 ? 1 : 0);
                    assertThat(manifests.read(meta.fileName())).containsExactly(entry);
                }
            }
        }
    }

    @Test
    void invalidRowIdRangeKeepsCoverageUnavailable() throws Exception {
        FileIO io = LocalFileIO.create();
        Path root = root("unknown");
        ManifestFile manifests =
                manifests(root, io, DEFAULT_PART_TYPE, Long.MAX_VALUE, enabledOptions());
        ManifestEntry source = gen.next();
        ManifestEntry entry =
                ManifestEntry.create(
                        FileKind.ADD, source.partition(), 1, 4, source.file().newFirstRowId(-1L));
        ManifestFileMeta meta = manifests.write(Collections.singletonList(entry)).get(0);
        assertThat(meta.minRowId()).isNull();
        assertThat(meta.maxRowId()).isNull();
        assertThat(select(root, io, meta, DEFAULT_PART_TYPE, point(123), null).blocks()).hasSize(1);
    }

    @ParameterizedTest
    @ValueSource(strings = {"open", "write", "close"})
    void sidecarFailureCleansAllRollingOutputs(String phase) throws Exception {
        AtomicInteger sidecars = new AtomicInteger();
        FileIO io =
                new LocalFileIO() {

                    @Override
                    public PositionOutputStream newOutputStream(Path path, boolean overwrite)
                            throws IOException {
                        if (!path.getName().endsWith(ManifestSidecar.SUFFIX)
                                || sidecars.incrementAndGet() != 2) {
                            return super.newOutputStream(path, overwrite);
                        }
                        if (phase.equals("open")) {
                            throw new IOException("sidecar " + phase + " failed");
                        }
                        return new PositionOutputStreamWrapper(
                                super.newOutputStream(path, overwrite)) {

                            @Override
                            public void write(byte[] bytes) throws IOException {
                                if (phase.equals("write")) {
                                    throw new IOException("sidecar " + phase + " failed");
                                }
                                super.write(bytes);
                            }

                            @Override
                            public void close() throws IOException {
                                super.close();
                                if (phase.equals("close")) {
                                    throw new IOException("sidecar " + phase + " failed");
                                }
                            }
                        };
                    }
                };
        Path root = root("failure-" + phase);
        ManifestFile manifests = manifests(root, io, DEFAULT_PART_TYPE, 1, enabledOptions());
        assertThatThrownBy(() -> manifests.write(entries(2200)))
                .hasRootCauseMessage("sidecar " + phase + " failed");
        try (Stream<java.nio.file.Path> files =
                Files.list(temp.resolve("failure-" + phase).resolve("manifest"))) {
            assertThat(files).isEmpty();
        }
    }

    private Options enabledOptions() {
        Options options = new Options();
        options.set(CoreOptions.MANIFEST_SIDECAR_ENABLED, true);
        options.set(CoreOptions.DATA_EVOLUTION_ENABLED, true);
        return options;
    }

    private List<ManifestEntry> entries(int count) {
        List<ManifestEntry> result = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            ManifestEntry entry = gen.next();
            result.add(
                    ManifestEntry.create(
                            i % 2 == 0 ? FileKind.ADD : FileKind.DELETE,
                            entry.partition(),
                            entry.bucket(),
                            entry.totalBuckets(),
                            entry.file().newFirstRowId(i * 100000000L)));
        }
        return result;
    }

    private Path root(String name) {
        return new Path(temp.resolve(name).toString());
    }

    private static RowRangeIndex point(long rowId) {
        return RowRangeIndex.create(Collections.singletonList(new Range(rowId, rowId)));
    }

    private static Path manifestPath(Path root, ManifestFileMeta meta) {
        return new Path(new Path(root, "manifest"), meta.fileName());
    }

    private ManifestSidecar.Selection select(
            Path root,
            FileIO io,
            ManifestFileMeta meta,
            RowType partitionType,
            @Nullable RowRangeIndex rows,
            @Nullable BiPredicate<Integer, Integer> buckets) {
        ManifestSidecar.Selection result =
                ManifestSidecar.read(
                        io,
                        manifestPath(root, meta),
                        meta,
                        rows,
                        null,
                        partitionType,
                        buckets,
                        null);
        assertThat(result).isNotNull();
        return result;
    }

    private ManifestFile manifests(
            Path root, FileIO io, RowType partitionType, long targetSize, Options options) {
        FileStorePathFactory paths =
                new FileStorePathFactory(
                        root,
                        partitionType,
                        "default",
                        CoreOptions.FILE_FORMAT.defaultValue(),
                        CoreOptions.DATA_FILE_PREFIX.defaultValue(),
                        CoreOptions.CHANGELOG_FILE_PREFIX.defaultValue(),
                        CoreOptions.PARTITION_GENERATE_LEGACY_NAME.defaultValue(),
                        CoreOptions.FILE_SUFFIX_INCLUDE_COMPRESSION.defaultValue(),
                        CoreOptions.FILE_COMPRESSION.defaultValue(),
                        null,
                        null,
                        CoreOptions.ExternalPathStrategy.NONE,
                        null,
                        false,
                        null);
        return new ManifestFile.Factory(
                        io,
                        new FileSystemSchemaManager(io, root),
                        partitionType,
                        FileFormat.fromIdentifier("avro", new Options()),
                        "zstd",
                        paths,
                        targetSize,
                        null,
                        null,
                        new CoreOptions(options))
                .create();
    }
}
