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

package org.apache.paimon.table.source;

import org.apache.paimon.catalog.TableQueryAuthResult;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.globalindex.IndexedSplit;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFileTestUtils;
import org.apache.paimon.io.DataInputDeserializer;
import org.apache.paimon.io.DataOutputViewStreamWrapper;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.table.FallbackReadFileStoreTable;
import org.apache.paimon.utils.CompatibilityUtils;
import org.apache.paimon.utils.IOUtils;
import org.apache.paimon.utils.InstantiationUtil;
import org.apache.paimon.utils.Range;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link SplitSerializer}. */
public class SplitSerializerTest {

    private static final String GENERATE_GOLDEN_FILES_PROPERTY = "generateSplitGoldenFiles";
    private static final String RESOURCE_PREFIX = "compatibility/";

    @Test
    public void testRoundTrip() throws IOException {
        for (GoldenCase goldenCase : goldenCases(2)) {
            Split actual = SplitSerializer.deserialize(SplitSerializer.serialize(goldenCase.split));
            assertSplitEquals(goldenCase.split, actual);
        }
    }

    @Test
    public void testVersion1GoldenFiles() throws IOException {
        for (GoldenCase goldenCase : goldenCases(1)) {
            assertSplitEquals(
                    goldenCase.split,
                    SplitSerializer.deserialize(readGoldenFile(goldenCase.fileName)));
        }
    }

    @Test
    public void testVersion2GoldenFiles() throws IOException {
        boolean generateGoldenFiles =
                Boolean.parseBoolean(
                        System.getProperties().getProperty(GENERATE_GOLDEN_FILES_PROPERTY));
        for (GoldenCase goldenCase : goldenCases(2)) {
            byte[] current = SplitSerializer.serialize(goldenCase.split);
            byte[] serialized;
            if (generateGoldenFiles) {
                CompatibilityUtils.writeCompatibilityFile(goldenCase.fileName, current);
                serialized = current;
            } else {
                serialized = readGoldenFile(goldenCase.fileName);
                assertThat(current).isEqualTo(serialized);
            }
            assertSplitEquals(goldenCase.split, SplitSerializer.deserialize(serialized));
        }
    }

    @Test
    public void testUnsupportedVersion() throws IOException {
        byte[] serialized = SplitSerializer.serialize(dataSplit(true));
        ByteBuffer.wrap(serialized, Long.BYTES, Integer.BYTES).putInt(3);

        assertThatThrownBy(() -> SplitSerializer.deserialize(serialized))
                .isInstanceOf(IOException.class)
                .hasMessage("Unsupported split serializer version: 3");
    }

    @Test
    public void testFallbackSplitImplSerializeAndDeserialize() throws IOException {
        FallbackReadFileStoreTable.FallbackSplitImpl split = fallbackSplit(true);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        split.serialize(new DataOutputViewStreamWrapper(out));
        FallbackReadFileStoreTable.FallbackSplitImpl deserialized =
                FallbackReadFileStoreTable.FallbackSplitImpl.deserialize(
                        new DataInputDeserializer(out.toByteArray()));

        assertFallbackSplitEquals(split, deserialized);
    }

    @Test
    public void testFallbackSplitImplJavaSerializeAndDeserialize() throws Exception {
        FallbackReadFileStoreTable.FallbackSplitImpl split = fallbackSplit(true);

        byte[] bytes = InstantiationUtil.serializeObject(split);
        FallbackReadFileStoreTable.FallbackSplitImpl deserialized =
                InstantiationUtil.deserializeObject(bytes, getClass().getClassLoader());

        assertFallbackSplitEquals(split, deserialized);
    }

    private static byte[] readGoldenFile(String fileName) throws IOException {
        try (InputStream in =
                SplitSerializerTest.class
                        .getClassLoader()
                        .getResourceAsStream(RESOURCE_PREFIX + fileName)) {
            return IOUtils.readFully(in, false);
        }
    }

    private static List<GoldenCase> goldenCases(int version) {
        boolean withColumnSequences = version >= 2;
        List<GoldenCase> cases = new ArrayList<>();
        DataSplit dataSplit = dataSplit(withColumnSequences);
        IncrementalSplit incrementalSplit = incrementalSplit(withColumnSequences);
        IndexedSplit indexedSplit = indexedSplit(dataSplit);
        ChainSplit chainSplit = chainSplit(withColumnSequences);
        QueryAuthSplit queryAuthSplit = queryAuthSplit(dataSplit);
        FallbackReadFileStoreTable.FallbackDataSplit fallbackDataSplit =
                (FallbackReadFileStoreTable.FallbackDataSplit)
                        FallbackReadFileStoreTable.toFallbackSplit(dataSplit, true);
        FallbackReadFileStoreTable.FallbackSplitImpl fallbackSplit =
                fallbackSplit(withColumnSequences);

        String prefix = "split-v" + version + "-";
        cases.add(new GoldenCase(prefix + "data", dataSplit));
        cases.add(new GoldenCase(prefix + "incremental", incrementalSplit));
        cases.add(new GoldenCase(prefix + "indexed", indexedSplit));
        cases.add(new GoldenCase(prefix + "chain", chainSplit));
        cases.add(new GoldenCase(prefix + "query-auth", queryAuthSplit));
        cases.add(new GoldenCase(prefix + "fallback-data", fallbackDataSplit));
        cases.add(new GoldenCase(prefix + "fallback", fallbackSplit));
        return cases;
    }

    private static DataSplit dataSplit(boolean withColumnSequences) {
        List<DataFileMeta> files =
                Arrays.asList(
                        dataFile("file-a", 0, 1, 10, 100L, withColumnSequences),
                        dataFile("file-b", 1, 11, 20, 200L, withColumnSequences));
        List<DeletionFile> deletionFiles =
                Arrays.asList(null, new DeletionFile("dv/file-b", 2L, 10L, 3L));
        return DataSplit.builder()
                .withSnapshot(42L)
                .withPartition(DataFileTestUtils.row(2026, 7))
                .withBucket(3)
                .withTotalBuckets(8)
                .withBucketPath("dt=20260706/bucket-3")
                .withDataFiles(files)
                .withDataDeletionFiles(deletionFiles)
                .rawConvertible(true)
                .build();
    }

    private static IncrementalSplit incrementalSplit(boolean withColumnSequences) {
        List<DataFileMeta> before =
                Collections.singletonList(
                        dataFile("before-file", 0, 1, 5, 10L, withColumnSequences));
        List<DataFileMeta> after =
                Collections.singletonList(
                        dataFile("after-file", 0, 6, 12, 20L, withColumnSequences));
        return new IncrementalSplit(
                43L,
                DataFileTestUtils.row(2026, 8),
                4,
                8,
                before,
                Collections.singletonList(new DeletionFile("dv/before", 0L, 7L, null)),
                after,
                Collections.singletonList(new DeletionFile("dv/after", 1L, 9L, 2L)),
                true);
    }

    private static IndexedSplit indexedSplit(DataSplit dataSplit) {
        return new IndexedSplit(
                dataSplit,
                Arrays.asList(new Range(1L, 4L), new Range(11L, 13L)),
                new float[] {0.5f, 0.25f, 0.125f});
    }

    private static ChainSplit chainSplit(boolean withColumnSequences) {
        DataSplit left = dataSplit(withColumnSequences);
        DataSplit right =
                DataSplit.builder()
                        .withSnapshot(44L)
                        .withPartition(DataFileTestUtils.row(2026, 9))
                        .withBucket(5)
                        .withTotalBuckets(8)
                        .withBucketPath("dt=20260707/bucket-5")
                        .withDataFiles(
                                Collections.singletonList(
                                        dataFile(
                                                "chain-file",
                                                0,
                                                21,
                                                30,
                                                300L,
                                                withColumnSequences)))
                        .withDataDeletionFiles(
                                Collections.singletonList(
                                        new DeletionFile("deletion_file", 100, 22, null)))
                        .rawConvertible(false)
                        .build();
        Map<String, String> fileBucketPathMapping = new LinkedHashMap<>();
        Map<String, String> fileBranchMapping = new LinkedHashMap<>();
        List<DeletionFile> deletionFiles = new ArrayList<>();
        List<DataFileMeta> files = new ArrayList<>();
        for (DataSplit split : Arrays.asList(left, right)) {
            Optional<List<DeletionFile>> deletionFilesOpt = split.deletionFiles();
            for (int i = 0; i < split.dataFiles().size(); i++) {
                DataFileMeta file = split.dataFiles().get(i);
                DeletionFile deletionFile =
                        deletionFilesOpt.isPresent() ? deletionFilesOpt.get().get(i) : null;
                files.add(file);
                deletionFiles.add(deletionFile);
                fileBucketPathMapping.put(file.fileName(), split.bucketPath());
                fileBranchMapping.put(file.fileName(), split == left ? "snapshot" : "delta");
            }
        }
        return new ChainSplit(
                DataFileTestUtils.row(2026),
                files,
                fileBranchMapping,
                fileBucketPathMapping,
                deletionFiles);
    }

    private static QueryAuthSplit queryAuthSplit(Split split) {
        Map<String, String> columnMasking = new LinkedHashMap<>();
        columnMasking.put("name", "mask-name-json");
        columnMasking.put("phone", "mask-phone-json");
        return new QueryAuthSplit(
                split,
                new TableQueryAuthResult(
                        Arrays.asList("filter-json-1", "filter-json-2"), columnMasking));
    }

    private static FallbackReadFileStoreTable.FallbackSplitImpl fallbackSplit(
            boolean withColumnSequences) {
        return new FallbackReadFileStoreTable.FallbackSplitImpl(
                chainSplit(withColumnSequences), true);
    }

    private static DataFileMeta dataFile(
            String name,
            int level,
            int minKey,
            int maxKey,
            long maxSequence,
            boolean withColumnSequences) {
        DataFileMeta file =
                DataFileMeta.create(
                        name,
                        maxKey - minKey + 1,
                        maxKey - minKey + 1,
                        DataFileTestUtils.row(minKey),
                        DataFileTestUtils.row(maxKey),
                        SimpleStats.EMPTY_STATS,
                        SimpleStats.EMPTY_STATS,
                        0L,
                        maxSequence,
                        0L,
                        level,
                        Collections.emptyList(),
                        Timestamp.fromEpochMillis(100),
                        0L,
                        null,
                        FileSource.APPEND,
                        null,
                        null,
                        null,
                        null,
                        null);
        return withColumnSequences
                ? file.withColumnMaxSequenceNumbers(new long[] {maxSequence})
                : file;
    }

    private static void assertSplitEquals(Split expected, Split actual) {
        assertThat(actual.getClass()).isEqualTo(expected.getClass());
        if (expected instanceof QueryAuthSplit) {
            assertQueryAuthSplitEquals((QueryAuthSplit) expected, (QueryAuthSplit) actual);
        } else if (expected instanceof ChainSplit) {
            assertChainSplitEquals((ChainSplit) expected, (ChainSplit) actual);
        } else if (expected instanceof FallbackReadFileStoreTable.FallbackSplitImpl) {
            assertFallbackSplitEquals(
                    (FallbackReadFileStoreTable.FallbackSplitImpl) expected,
                    (FallbackReadFileStoreTable.FallbackSplitImpl) actual);
        } else {
            assertThat(actual).isEqualTo(expected);
        }
    }

    private static void assertQueryAuthSplitEquals(QueryAuthSplit expected, QueryAuthSplit actual) {
        assertSplitEquals(expected.split(), actual.split());
        assertThat(actual.authResult()).isNotNull();
        assertThat(actual.authResult().filter()).isEqualTo(expected.authResult().filter());
        assertThat(actual.authResult().columnMasking())
                .isEqualTo(expected.authResult().columnMasking());
    }

    private static void assertChainSplitEquals(ChainSplit expected, ChainSplit actual) {
        assertThat(actual).isEqualTo(expected);
        assertThat(actual.fileBucketPathMapping()).isEqualTo(expected.fileBucketPathMapping());
        assertThat(actual.fileBranchMapping()).isEqualTo(expected.fileBranchMapping());
        assertThat(actual.deletionFiles()).isEqualTo(expected.deletionFiles());
    }

    private static void assertFallbackSplitEquals(
            FallbackReadFileStoreTable.FallbackSplitImpl expected,
            FallbackReadFileStoreTable.FallbackSplitImpl actual) {
        assertThat(actual.isFallback()).isEqualTo(expected.isFallback());
        assertSplitEquals(expected.wrapped(), actual.wrapped());
    }

    private static class GoldenCase {

        private final String fileName;
        private final Split split;

        private GoldenCase(String fileName, Split split) {
            this.fileName = fileName;
            this.split = split;
        }
    }
}
