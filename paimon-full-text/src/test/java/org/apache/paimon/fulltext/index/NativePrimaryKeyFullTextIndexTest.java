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

package org.apache.paimon.fulltext.index;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.deletionvectors.BitmapDeletionVector;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.GlobalIndexSingleColumnWriter;
import org.apache.paimon.globalindex.GlobalIndexer;
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.globalindex.io.GlobalIndexFileReader;
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.pk.PrimaryKeyIndexSourceFile;
import org.apache.paimon.index.pk.PrimaryKeyIndexSourceMeta;
import org.apache.paimon.index.pkfulltext.PrimaryKeyFullTextBucketSearch;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.options.Options;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.PrimaryKeyFullTextSearchSplit;
import org.apache.paimon.table.source.PrimaryKeySearchPosition;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.JsonSerdeUtil;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.type.TypeReference;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.apache.paimon.shade.guava30.com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** Native SPI tests for file-aligned primary-key full-text archives. */
class NativePrimaryKeyFullTextIndexTest {

    private static final DataField TEXT_FIELD = new DataField(7, "content", DataTypes.STRING());

    @TempDir java.nio.file.Path tempDir;

    private FileIO fileIO;
    private Path indexPath;

    @BeforeEach
    void before() {
        assumeTrue(isNativeAvailable(), "Native full-text library not available, skipping tests");
        fileIO = LocalFileIO.create();
        indexPath = new Path(tempDir.toUri());
    }

    @AfterEach
    void after() throws IOException {
        if (fileIO != null) {
            fileIO.delete(indexPath, true);
        }
    }

    @Test
    void testPreservesNullOrdinalFiltersDeletedRowsAndOrdersByNativeScore() throws Exception {
        Options options = new Options();
        IndexFileMeta archive =
                buildArchive(
                        Arrays.asList(
                                BinaryString.fromString("paimon vector"),
                                null,
                                BinaryString.fromString("paimon lake"),
                                BinaryString.fromString("paimon storage")),
                        options,
                        "native-definition");

        assertThat(archive.indexType()).isEqualTo("full-text");
        assertThat(archive.rowCount()).isEqualTo(4);
        assertThat(archive.globalIndexMeta().rowRangeStart()).isZero();
        assertThat(archive.globalIndexMeta().rowRangeEnd()).isEqualTo(3);
        assertThat(archive.globalIndexMeta().indexFieldId()).isEqualTo(TEXT_FIELD.id());
        PrimaryKeyIndexSourceMeta sourceMeta = PrimaryKeyIndexSourceMeta.fromIndexFile(archive);
        assertThat(sourceMeta.sourceFile().fileName()).isEqualTo("data-1");
        assertThat(sourceMeta.sourceFile().rowCount()).isEqualTo(4);
        assertThat(sourceMeta.definitionFingerprint()).contains("native-definition");

        DataFileMeta dataFile = dataFile(4);
        PrimaryKeyFullTextSearchSplit split =
                new PrimaryKeyFullTextSearchSplit(
                        DataSplit.builder()
                                .withSnapshot(1)
                                .withPartition(BinaryRow.EMPTY_ROW)
                                .withBucket(0)
                                .withBucketPath(indexPath.toString())
                                .withTotalBuckets(1)
                                .withDataFiles(Collections.singletonList(dataFile))
                                .build(),
                        Collections.singletonList(archive),
                        Collections.emptyList());
        BitmapDeletionVector deletionVector = new BitmapDeletionVector();
        deletionVector.delete(2);
        Map<String, DeletionVector> deletionVectors =
                Collections.singletonMap(dataFile.fileName(), deletionVector);
        GlobalIndexer indexer = GlobalIndexer.create("full-text", TEXT_FIELD, options);
        PrimaryKeyFullTextBucketSearch search =
                new PrimaryKeyFullTextBucketSearch(
                        payload ->
                                indexer.createReader(
                                        fileReader(),
                                        Collections.singletonList(toIOMeta(payload)),
                                        newDirectExecutorService()));

        List<List<PrimaryKeySearchPosition>> rankings =
                search.searchRankings(
                        split, deletionVectors, "content", boostQuery("paimon", "vector", 0.1F), 2);

        assertThat(rankings).hasSize(1);
        assertThat(rankings.get(0))
                .extracting(
                        PrimaryKeySearchPosition::rowPosition,
                        PrimaryKeySearchPosition::dataFileName)
                .containsExactly(tuple(3L, "data-1"), tuple(0L, "data-1"));
        assertThat(rankings.get(0).get(0).score()).isGreaterThan(rankings.get(0).get(1).score());
    }

    @Test
    void testPersistsTokenizerOptionsInPkArchiveMetadata() throws Exception {
        Options options = new Options();
        options.set("full-text.tokenizer", "ngram");
        options.set("full-text.ngram.min-gram", "2");
        options.set("full-text.ngram.max-gram", "2");

        IndexFileMeta archive =
                buildArchive(
                        Collections.singletonList(BinaryString.fromString("中文全文检索")),
                        options,
                        "ngram-definition");

        assertThat(serializedOptions(archive.globalIndexMeta().indexMeta()))
                .containsEntry("tokenizer", "ngram")
                .containsEntry("ngram.min-gram", "2")
                .containsEntry("ngram.max-gram", "2");
    }

    private IndexFileMeta buildArchive(
            List<BinaryString> texts, Options options, String definitionFingerprint)
            throws Exception {
        GlobalIndexer indexer = GlobalIndexer.create("full-text", TEXT_FIELD, options);
        GlobalIndexSingleColumnWriter writer =
                (GlobalIndexSingleColumnWriter) indexer.createWriter(fileWriter());
        for (int i = 0; i < texts.size(); i++) {
            writer.write(texts.get(i), i);
        }
        List<ResultEntry> results = writer.finish();
        assertThat(results).hasSize(1);
        ResultEntry result = results.get(0);
        Path path = new Path(indexPath, result.fileName());
        return new IndexFileMeta(
                "full-text",
                result.fileName(),
                fileIO.getFileSize(path),
                result.rowCount(),
                new GlobalIndexMeta(
                        0,
                        texts.size() - 1,
                        TEXT_FIELD.id(),
                        null,
                        result.meta(),
                        new PrimaryKeyIndexSourceMeta(
                                        new PrimaryKeyIndexSourceFile("data-1", texts.size()),
                                        definitionFingerprint)
                                .serialize()),
                null);
    }

    private GlobalIndexFileWriter fileWriter() {
        return new GlobalIndexFileWriter() {
            @Override
            public String newFileName(String prefix) {
                return prefix + "-" + UUID.randomUUID();
            }

            @Override
            public PositionOutputStream newOutputStream(String fileName) throws IOException {
                return fileIO.newOutputStream(new Path(indexPath, fileName), false);
            }
        };
    }

    private GlobalIndexFileReader fileReader() {
        return meta -> fileIO.newInputStream(meta.filePath());
    }

    private GlobalIndexIOMeta toIOMeta(IndexFileMeta archive) {
        return new GlobalIndexIOMeta(
                new Path(indexPath, archive.fileName()),
                archive.fileSize(),
                archive.globalIndexMeta().indexMeta());
    }

    private static DataFileMeta dataFile(long rowCount) {
        return DataFileMeta.forAppend(
                "data-1",
                100,
                rowCount,
                SimpleStats.EMPTY_STATS,
                0,
                1,
                1,
                Collections.emptyList(),
                null,
                FileSource.COMPACT,
                null,
                null,
                null,
                null);
    }

    private static Map<String, String> serializedOptions(byte[] metadata) {
        return JsonSerdeUtil.fromJson(
                new String(metadata, StandardCharsets.UTF_8),
                new TypeReference<Map<String, String>>() {});
    }

    private static String boostQuery(String positive, String negative, float negativeBoost) {
        return "{\"boost\":{\"positive\":"
                + matchQuery(positive)
                + ",\"negative\":"
                + matchQuery(negative)
                + ",\"negative_boost\":"
                + negativeBoost
                + "}}";
    }

    private static String matchQuery(String terms) {
        return "{\"match\":{\"query\":\"" + terms + "\"}}";
    }

    private static boolean isNativeAvailable() {
        String path = System.getenv("PAIMON_FTINDEX_JNI_LIB_PATH");
        return path != null && !path.isEmpty() && new File(path).isFile();
    }
}
