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

package org.apache.paimon.globalindex;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.index.DataEvolutionIndexSourceMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.IndexPathFactory;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.PojoDataFileMeta;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.options.Options;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.VarCharType;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RowRangeIndex;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.apache.paimon.utils.FileStorePathFactoryTest.createNonPartFactory;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link GlobalIndexBuilderUtils}. */
class GlobalIndexBuilderUtilsTest {

    @TempDir java.nio.file.Path tempDir;

    private FileIO fileIO;
    private IndexPathFactory indexPathFactory;
    private CoreOptions coreOptions;

    @ParameterizedTest
    @ValueSource(strings = {"internal", "single", "multiple"})
    void testFileLocationsWithDifferentReaderOptions(String placement) throws Exception {
        Path root = new Path(tempDir.toUri());
        boolean external = !placement.equals("internal");
        List<Path> roots;
        if (placement.equals("single")) {
            roots = Collections.singletonList(new Path(root, "index,archive"));
            coreOptions
                    .toConfiguration()
                    .set(CoreOptions.GLOBAL_INDEX_EXTERNAL_PATH, roots.get(0).toString());
        } else {
            roots = Arrays.asList(new Path(root, "a"), new Path(root, "b"));
        }
        if (placement.equals("multiple")) {
            coreOptions
                    .toConfiguration()
                    .set(
                            CoreOptions.GLOBAL_INDEX_EXTERNAL_PATHS,
                            roots.get(0) + "," + roots.get(1));
        }
        IndexPathFactory writerPaths =
                createNonPartFactory(root, coreOptions.globalIndexExternalPaths())
                        .globalIndexFileFactory();
        GlobalIndexFileReadWrite writer = new GlobalIndexFileReadWrite(fileIO, writerPaths);
        List<ResultEntry> entries = new ArrayList<>();
        List<Path> parents = new ArrayList<>();
        for (int i = 0; i < 6; i++) {
            String file = writer.newFilePath("test");
            assertThat(new Path(file).toUri().getScheme()).isEqualTo("file");
            assertThat(new Path(file).getName())
                    .startsWith("test-global-index-")
                    .endsWith(".index");
            try (PositionOutputStream output = writer.newOutputStream(file)) {
                output.write(i);
            }
            assertThat(writer.fileSize(file)).isEqualTo(1);
            parents.add(new Path(file).getParent());
            entries.add(new ResultEntry(file, 1, null));
        }
        if (external) {
            for (Path externalRoot : roots) {
                assertThat(Collections.frequency(parents, externalRoot))
                        .isEqualTo(6 / roots.size());
            }
        } else {
            assertThat(parents).containsOnly(new Path(root, "index"));
        }

        List<IndexFileMeta> metas =
                GlobalIndexBuilderUtils.toIndexFileMetas(
                        fileIO, coreOptions, new Range(0, 5), 0, "test", entries);

        // Reading committed files must not depend on the current external path configuration.
        IndexPathFactory readerPaths =
                createNonPartFactory(root, external ? Collections.emptyList() : roots)
                        .globalIndexFileFactory();
        GlobalIndexFileReadWrite reader = new GlobalIndexFileReadWrite(fileIO, readerPaths);
        for (int i = 0; i < metas.size(); i++) {
            IndexFileMeta meta = metas.get(i);
            Path path = new Path(entries.get(i).filePath());
            assertThat(meta.fileName()).isEqualTo(path.getName());
            assertThat(meta.externalPath()).isEqualTo(external ? path.toString() : null);
            assertThat(readerPaths.toPath(meta)).isEqualTo(path);
            try (SeekableInputStream input =
                    reader.getInputStream(
                            new GlobalIndexIOMeta(
                                    readerPaths.toPath(meta), meta.fileSize(), null))) {
                assertThat(input.read()).isEqualTo(i);
            }
            fileIO.delete(readerPaths.toPath(meta), false);
            assertThat(fileIO.exists(path)).isFalse();
        }
    }

    @BeforeEach
    void setUp() {
        fileIO = new LocalFileIO();
        Path dir = new Path(tempDir.toString());
        indexPathFactory =
                new IndexPathFactory() {
                    @Override
                    public Path newPathFromName(String fileName) {
                        return new Path(dir, fileName);
                    }

                    @Override
                    public Path newPath() {
                        return new Path(dir, UUID.randomUUID().toString());
                    }

                    @Override
                    public boolean isExternalPath() {
                        return false;
                    }

                    @Override
                    public Path toPath(String fileName, @Nullable String externalPath) {
                        return externalPath == null
                                ? newPathFromName(fileName)
                                : new Path(externalPath);
                    }
                };
        coreOptions = new CoreOptions(new Options().toMap());
    }

    // Test: 2 columns (title + vec), primary column title is indexFieldId, rest in extraFieldIds
    @Test
    void testToIndexFileMetasMultiColumn() throws IOException {
        DataField titleField = new DataField(1, "title", new VarCharType(Integer.MAX_VALUE));
        DataField vecField = new DataField(2, "vec", new ArrayType(new FloatType()));
        List<DataField> fields = Arrays.asList(titleField, vecField);

        List<ResultEntry> entries = createDummyResultEntries();
        Range range = new Range(0, 99);

        List<IndexFileMeta> metas =
                GlobalIndexBuilderUtils.toIndexFileMetas(
                        fileIO, coreOptions, range, fields, "test-type", entries, null);

        assertThat(metas).hasSize(1);
        assertThat(metas.get(0).globalIndexMeta().indexFieldId()).isEqualTo(1);
        assertThat(metas.get(0).globalIndexMeta().extraFieldIds()).isEqualTo(new int[] {2});
        assertThat(metas.get(0).globalIndexMeta().rowRangeStart()).isEqualTo(0);
        assertThat(metas.get(0).globalIndexMeta().rowRangeEnd()).isEqualTo(99);
    }

    // Test: single column, extraFieldIds should be null (backward compatible with single-column
    // path)
    @Test
    void testToIndexFileMetasSingleColumn() throws IOException {
        DataField titleField = new DataField(1, "title", new VarCharType(Integer.MAX_VALUE));
        List<DataField> fields = Collections.singletonList(titleField);

        List<ResultEntry> entries = createDummyResultEntries();
        Range range = new Range(0, 49);

        List<IndexFileMeta> metas =
                GlobalIndexBuilderUtils.toIndexFileMetas(
                        fileIO, coreOptions, range, fields, "test-type", entries, null);

        assertThat(metas).hasSize(1);
        assertThat(metas.get(0).globalIndexMeta().indexFieldId()).isEqualTo(1);
        assertThat(metas.get(0).globalIndexMeta().extraFieldIds()).isNull();
    }

    @Test
    void testRowCountPerIndexFile() throws IOException {
        List<ResultEntry> entries =
                Arrays.asList(
                        new ResultEntry(
                                indexPathFactory.newPathFromName("first").toString(), 2, null),
                        new ResultEntry(
                                indexPathFactory.newPathFromName("second").toString(), 1, null));
        for (ResultEntry entry : entries) {
            fileIO.newOutputStream(new Path(entry.filePath()), false).close();
        }
        List<IndexFileMeta> metas =
                GlobalIndexBuilderUtils.toIndexFileMetas(
                        fileIO, coreOptions, new Range(100, 102), 1, "test-type", entries);
        assertThat(metas).extracting(IndexFileMeta::rowCount).containsExactly(2L, 1L);
        assertThat(metas)
                .extracting(file -> file.globalIndexMeta().rowRange())
                .containsOnly(new Range(100, 102));
    }

    @Test
    void testToIndexFileMetasWithSourceMeta() throws IOException {
        DataField field = new DataField(1, "vec", new ArrayType(new FloatType()));
        byte[] sourceMeta = new DataEvolutionIndexSourceMeta(7L).serialize();

        List<IndexFileMeta> metas =
                GlobalIndexBuilderUtils.toIndexFileMetas(
                        fileIO,
                        coreOptions,
                        new Range(0, 9),
                        Collections.singletonList(field),
                        "lumina",
                        createDummyResultEntries(),
                        sourceMeta);

        assertThat(metas.get(0).globalIndexMeta().sourceMeta()).containsExactly(sourceMeta);
    }

    // Test: 3 columns (title + vec + id), primary column title is indexFieldId, rest in
    // extraFieldIds
    @Test
    void testToIndexFileMetasThreeColumns() throws IOException {
        DataField titleField = new DataField(1, "title", new VarCharType(Integer.MAX_VALUE));
        DataField vecField = new DataField(2, "vec", new ArrayType(new FloatType()));
        DataField idField = new DataField(3, "id", new IntType());
        List<DataField> fields = Arrays.asList(titleField, vecField, idField);

        List<ResultEntry> entries = createDummyResultEntries();
        Range range = new Range(0, 199);

        List<IndexFileMeta> metas =
                GlobalIndexBuilderUtils.toIndexFileMetas(
                        fileIO, coreOptions, range, fields, "test-type", entries, null);

        assertThat(metas).hasSize(1);
        assertThat(metas.get(0).globalIndexMeta().indexFieldId()).isEqualTo(1);
        assertThat(metas.get(0).globalIndexMeta().extraFieldIds()).isEqualTo(new int[] {2, 3});
    }

    @Test
    void testCreateShardIndexedSplitsUsesUnindexedRanges() {
        List<ManifestEntry> entries = Arrays.asList(createEntry(0L, 100), createEntry(100L, 100));

        List<IndexedSplit> splits =
                GlobalIndexBuilderUtils.createShardIndexedSplits(
                        entries,
                        100,
                        (partition, bucket) -> "/bucket-" + bucket,
                        Collections.singletonList(new Range(0, 99)));

        assertThat(splits).hasSize(1);
        assertThat(splits.get(0).rowRanges()).containsExactly(new Range(0, 99));
        assertThat(splits.get(0).dataSplit().dataFiles()).containsExactly(entries.get(0).file());
    }

    @Test
    void testCreateShardIndexedSplitsCanSplitOneShardIntoMultipleRanges() {
        List<ManifestEntry> entries = Collections.singletonList(createEntry(0L, 100));

        List<IndexedSplit> splits =
                GlobalIndexBuilderUtils.createShardIndexedSplits(
                        entries,
                        100,
                        (partition, bucket) -> "/bucket-" + bucket,
                        Arrays.asList(new Range(0, 9), new Range(90, 99)));

        assertThat(splits).hasSize(2);
        assertThat(splits.get(0).rowRanges()).containsExactly(new Range(0, 9));
        assertThat(splits.get(1).rowRanges()).containsExactly(new Range(90, 99));
        assertThat(splits.get(0).dataSplit()).isEqualTo(splits.get(1).dataSplit());
    }

    @Test
    void testSplitByContiguousRowRangeFromDataFiles() {
        DataFileMeta file1 = createDataFileMeta(0L, 100L);
        DataFileMeta file2 = createDataFileMeta(300L, 100L);
        DataFileMeta file3 = createDataFileMeta(100L, 100L);
        DataSplit split =
                DataSplit.builder()
                        .withSnapshot(1L)
                        .withPartition(BinaryRow.EMPTY_ROW)
                        .withBucket(0)
                        .withBucketPath("bucket-0")
                        .withDataFiles(Arrays.asList(file1, file2, file3))
                        .isStreaming(false)
                        .rawConvertible(false)
                        .build();

        List<DataSplit> rebuilt =
                GlobalIndexBuilderUtils.splitByContiguousRowRange(Collections.singletonList(split));

        assertThat(rebuilt).hasSize(2);
        assertThat(rebuilt.get(0).dataFiles()).containsExactly(file1, file3);
        assertThat(rebuilt.get(1).dataFiles()).containsExactly(file2);
        assertThat(GlobalIndexBuilderUtils.calcRowRange(rebuilt.get(0)))
                .isEqualTo(new Range(0, 199));
        assertThat(GlobalIndexBuilderUtils.calcRowRange(rebuilt.get(1)))
                .isEqualTo(new Range(300, 399));
    }

    @Test
    void testGroupSplitsByDiscontiguousRowRangeIndex() {
        DataFileMeta file1 = createDataFileMeta(4750L, 151L);
        DataFileMeta file2 = createDataFileMeta(4901L, 1037L);
        DataFileMeta file3 = createDataFileMeta(5938L, 1662L);
        DataSplit split =
                DataSplit.builder()
                        .withSnapshot(1L)
                        .withPartition(BinaryRow.EMPTY_ROW)
                        .withBucket(0)
                        .withBucketPath("bucket-0")
                        .withDataFiles(Arrays.asList(file1, file2, file3))
                        .isStreaming(false)
                        .rawConvertible(false)
                        .build();

        Map<BinaryRow, Map<Range, List<Split>>> result =
                GlobalIndexBuilderUtils.groupSplitsByRange(
                        RowRangeIndex.create(
                                Arrays.asList(new Range(4750, 4900), new Range(5938, 7599))),
                        Collections.singletonList(split));

        assertThat(result).containsOnlyKeys(BinaryRow.EMPTY_ROW);
        Map<Range, List<Split>> ranges = result.get(BinaryRow.EMPTY_ROW);
        assertThat(ranges).containsOnlyKeys(new Range(4750, 4900), new Range(5938, 7599));
        assertIndexedSplitRowRanges(ranges.get(new Range(4750, 4900)), new Range(4750, 4900));
        assertIndexedSplitRowRanges(ranges.get(new Range(5938, 7599)), new Range(5938, 7599));
    }

    @Test
    void testShardSplitsUsesStableGlobalRowIdWindows() {
        DataSplit split =
                DataSplit.builder()
                        .withPartition(BinaryRow.EMPTY_ROW)
                        .withBucket(0)
                        .withBucketPath("bucket-0")
                        .withDataFiles(Collections.singletonList(createDataFileMeta(5L, 10L)))
                        .rawConvertible(false)
                        .build();
        Map<Range, List<Split>> input = new LinkedHashMap<>();
        input.put(new Range(5, 14), Collections.singletonList(split));

        Map<Range, List<Split>> shards = GlobalIndexBuilderUtils.shardSplitsByRowRange(input, 6);

        assertThat(shards.keySet())
                .containsExactly(new Range(5, 5), new Range(6, 11), new Range(12, 14));
        for (Map.Entry<Range, List<Split>> shard : shards.entrySet()) {
            assertIndexedSplitRowRanges(shard.getValue(), shard.getKey());
        }
    }

    private List<ResultEntry> createDummyResultEntries() throws IOException {
        String fileName = "test-index-" + UUID.randomUUID();
        Path filePath = indexPathFactory.newPathFromName(fileName);
        fileIO.newOutputStream(filePath, false).close();
        return Collections.singletonList(new ResultEntry(filePath.toString(), 100, null));
    }

    private ManifestEntry createEntry(Long firstRowId, long rowCount) {
        PojoDataFileMeta file =
                new PojoDataFileMeta(
                        "test-file-" + UUID.randomUUID(),
                        1024L,
                        rowCount,
                        BinaryRow.EMPTY_ROW,
                        BinaryRow.EMPTY_ROW,
                        SimpleStats.EMPTY_STATS,
                        SimpleStats.EMPTY_STATS,
                        0L,
                        0L,
                        0L,
                        0,
                        Collections.emptyList(),
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        firstRowId,
                        null,
                        null);
        return ManifestEntry.create(FileKind.ADD, BinaryRow.EMPTY_ROW, 0, 1, file);
    }

    private static void assertIndexedSplitRowRanges(List<Split> splits, Range rowRange) {
        assertThat(splits).hasSize(1);
        assertThat(splits.get(0)).isInstanceOf(IndexedSplit.class);
        assertThat(((IndexedSplit) splits.get(0)).rowRanges()).containsExactly(rowRange);
    }

    private static DataFileMeta createDataFileMeta(long firstRowId, long rowCount) {
        return new PojoDataFileMeta(
                "test-file-" + UUID.randomUUID(),
                1024L,
                rowCount,
                BinaryRow.EMPTY_ROW,
                BinaryRow.EMPTY_ROW,
                SimpleStats.EMPTY_STATS,
                SimpleStats.EMPTY_STATS,
                0L,
                0L,
                0L,
                0,
                Collections.emptyList(),
                null,
                null,
                null,
                null,
                null,
                null,
                firstRowId,
                null,
                null);
    }
}
