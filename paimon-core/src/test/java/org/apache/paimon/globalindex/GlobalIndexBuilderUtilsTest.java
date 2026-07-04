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
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.IndexPathFactory;
import org.apache.paimon.io.PojoDataFileMeta;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.options.Options;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.VarCharType;
import org.apache.paimon.utils.Range;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link GlobalIndexBuilderUtils}. */
class GlobalIndexBuilderUtilsTest {

    @TempDir java.nio.file.Path tempDir;

    private FileIO fileIO;
    private IndexPathFactory indexPathFactory;
    private CoreOptions coreOptions;

    @BeforeEach
    void setUp() {
        fileIO = new LocalFileIO();
        Path dir = new Path(tempDir.toString());
        indexPathFactory =
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
                        return false;
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
                        fileIO, indexPathFactory, coreOptions, range, fields, "test-type", entries);

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
                        fileIO, indexPathFactory, coreOptions, range, fields, "test-type", entries);

        assertThat(metas).hasSize(1);
        assertThat(metas.get(0).globalIndexMeta().indexFieldId()).isEqualTo(1);
        assertThat(metas.get(0).globalIndexMeta().extraFieldIds()).isNull();
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
                        fileIO, indexPathFactory, coreOptions, range, fields, "test-type", entries);

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

    private List<ResultEntry> createDummyResultEntries() throws IOException {
        String fileName = "test-index-" + UUID.randomUUID();
        Path filePath = indexPathFactory.toPath(fileName);
        fileIO.newOutputStream(filePath, false).close();
        return Collections.singletonList(new ResultEntry(fileName, 100, null));
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
                        null);
        return ManifestEntry.create(FileKind.ADD, BinaryRow.EMPTY_ROW, 0, 1, file);
    }
}
