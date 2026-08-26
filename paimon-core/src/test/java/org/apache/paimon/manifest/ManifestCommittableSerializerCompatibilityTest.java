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

import org.apache.paimon.data.Timestamp;
import org.apache.paimon.index.DeletionVectorMeta;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.utils.CompatibilityUtils;
import org.apache.paimon.utils.IOUtils;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;

import static org.apache.paimon.data.BinaryArray.fromLongArray;
import static org.apache.paimon.data.BinaryRow.singleColumn;
import static org.assertj.core.api.Assertions.assertThat;

/** Compatibility Test for {@link ManifestCommittableSerializer}. */
public class ManifestCommittableSerializerCompatibilityTest {

    private static final String GENERATE_GOLDEN_FILES_PROPERTY =
            "generateManifestCommittableGoldenFiles";

    @Test
    public void testCompatibilityToV5CommitV14() throws IOException {
        ManifestCommittable committable =
                createCurrentCommitCommittable(new GlobalIndexMeta(0, 9, 7, null, null, null, 11L));

        ManifestCommittableSerializer serializer = new ManifestCommittableSerializer();
        byte[] current = serializer.serialize(committable);
        byte[] serialized;
        if (Boolean.parseBoolean(
                System.getProperties().getProperty(GENERATE_GOLDEN_FILES_PROPERTY))) {
            CompatibilityUtils.writeCompatibilityFile("manifest-committable-v14-v5", current);
            serialized = current;
        } else {
            serialized =
                    IOUtils.readFully(
                            ManifestCommittableSerializerCompatibilityTest.class
                                    .getClassLoader()
                                    .getResourceAsStream(
                                            "compatibility/manifest-committable-v14-v5"),
                            true);
        }

        assertThat(current).isEqualTo(serialized);
        assertThat(serializer.deserialize(5, serialized)).isEqualTo(committable);
    }

    @Test
    public void testCompatibilityToV5CommitV13() throws IOException {
        byte[] serialized =
                IOUtils.readFully(
                        ManifestCommittableSerializerCompatibilityTest.class
                                .getClassLoader()
                                .getResourceAsStream("compatibility/manifest-committable-v13-v5"),
                        true);

        assertThat(new ManifestCommittableSerializer().deserialize(5, serialized))
                .isEqualTo(createCurrentCommitCommittable(null));
    }

    @Test
    public void testCompatibilityToV5CommitV13WithGlobalIndex() throws IOException {
        byte[] serialized =
                IOUtils.readFully(
                        ManifestCommittableSerializerCompatibilityTest.class
                                .getClassLoader()
                                .getResourceAsStream(
                                        "compatibility/manifest-committable-v13-global-index-v5"),
                        true);
        GlobalIndexMeta expectedGlobalIndex =
                new GlobalIndexMeta(
                        0L,
                        9L,
                        7,
                        new int[] {8, 9},
                        new byte[] {0x12, 0x34},
                        new byte[] {0x56, 0x78},
                        null);

        ManifestCommittable deserialized =
                new ManifestCommittableSerializer().deserialize(5, serialized);
        assertThat(deserialized).isEqualTo(createCurrentCommitCommittable(expectedGlobalIndex));
        GlobalIndexMeta actualGlobalIndex =
                ((CommitMessageImpl) deserialized.fileCommittables().get(0))
                        .newFilesIncrement()
                        .newIndexFiles()
                        .get(0)
                        .globalIndexMeta();
        assertThat(actualGlobalIndex).isEqualTo(expectedGlobalIndex);
        assertThat(actualGlobalIndex.sourceMeta()).containsExactly(0x56, 0x78);
        assertThat(actualGlobalIndex.buildSchemaId()).isNull();
    }

    private static ManifestCommittable createCurrentCommitCommittable(
            GlobalIndexMeta globalIndexMeta) {
        DataFileMeta dataFile =
                DataFileMeta.create(
                                "column-sequence-file",
                                1024L,
                                10L,
                                singleColumn("min_key"),
                                singleColumn("max_key"),
                                SimpleStats.EMPTY_STATS,
                                SimpleStats.EMPTY_STATS,
                                1L,
                                5L,
                                1L,
                                0,
                                Collections.emptyList(),
                                Timestamp.fromLocalDateTime(
                                        LocalDateTime.parse("2026-08-07T00:00:00")),
                                0L,
                                null,
                                FileSource.COMPACT,
                                null,
                                null,
                                1L,
                                Arrays.asList("a", "b"),
                                null)
                        .withColumnMaxSequenceNumbers(new long[] {3L, 5L});
        IndexFileMeta indexFile =
                new IndexFileMeta("index-type", "index-file", 100L, 10L, globalIndexMeta, null);
        ManifestCommittable committable =
                createManifestCommittable(
                        Collections.singletonList(dataFile), indexFile, indexFile);
        return committable;
    }

    @Test
    public void testCompatibilityToV5CommitV11() throws IOException {
        String fileName = "manifest-committable-v11-v5";

        SimpleStats keyStats =
                new SimpleStats(
                        singleColumn("min_key"),
                        singleColumn("max_key"),
                        fromLongArray(new Long[] {0L}));
        SimpleStats valueStats =
                new SimpleStats(
                        singleColumn("min_value"),
                        singleColumn("max_value"),
                        fromLongArray(new Long[] {0L}));
        DataFileMeta dataFile =
                DataFileMeta.create(
                        "my_file",
                        1024 * 1024,
                        1024,
                        singleColumn("min_key"),
                        singleColumn("max_key"),
                        keyStats,
                        valueStats,
                        15,
                        200,
                        5,
                        3,
                        Arrays.asList("extra1", "extra2"),
                        Timestamp.fromLocalDateTime(LocalDateTime.parse("2022-03-02T20:20:12")),
                        11L,
                        new byte[] {1, 2, 4},
                        FileSource.COMPACT,
                        Arrays.asList("field1", "field2", "field3"),
                        "hdfs://localhost:9000/path/to/file",
                        1L,
                        Arrays.asList("asdf", "qwer", "zxcv"),
                        null);
        List<DataFileMeta> dataFiles = Collections.singletonList(dataFile);
        GlobalIndexMeta globalIndexMeta =
                new GlobalIndexMeta(
                        1L, 2L, 3, new int[] {5, 6, 7}, new byte[] {0x23, 0x45}, new byte[] {0x67});
        IndexFileMeta hashIndexFile =
                new IndexFileMeta(
                        "my_index_type",
                        "my_index_file",
                        1024 * 100,
                        1002,
                        null,
                        null,
                        globalIndexMeta);

        LinkedHashMap<String, DeletionVectorMeta> dvRanges = new LinkedHashMap<>();
        dvRanges.put("dv_key1", new DeletionVectorMeta("dv_key1", 1, 2, 3L));
        dvRanges.put("dv_key2", new DeletionVectorMeta("dv_key2", 3, 4, 5L));
        IndexFileMeta devIndexFile =
                new IndexFileMeta(
                        "my_index_type",
                        "my_index_file",
                        1024 * 100,
                        1002,
                        dvRanges,
                        "external_path");

        ManifestCommittable manifestCommittable =
                createManifestCommittable(dataFiles, hashIndexFile, devIndexFile);

        ManifestCommittableSerializer serializer = new ManifestCommittableSerializer();
        byte[] bytes = serializer.serialize(manifestCommittable);
        ManifestCommittable deserialized = serializer.deserialize(serializer.getVersion(), bytes);
        assertThat(deserialized).isEqualTo(manifestCommittable);
        GlobalIndexMeta deserializedGlobalIndexMeta =
                ((CommitMessageImpl) deserialized.fileCommittables().get(0))
                        .newFilesIncrement()
                        .newIndexFiles()
                        .get(0)
                        .globalIndexMeta();
        assertThat(deserializedGlobalIndexMeta.sourceMeta()).containsExactly(0x67);

        byte[] oldBytes =
                IOUtils.readFully(
                        ManifestCommittableSerializerCompatibilityTest.class
                                .getClassLoader()
                                .getResourceAsStream("compatibility/" + fileName),
                        true);
        deserialized = serializer.deserialize(5, oldBytes);
        GlobalIndexMeta legacyGlobalIndexMeta =
                new GlobalIndexMeta(1L, 2L, 3, new int[] {5, 6, 7}, new byte[] {0x23, 0x45});
        IndexFileMeta legacyHashIndexFile =
                new IndexFileMeta(
                        "my_index_type",
                        "my_index_file",
                        1024 * 100,
                        1002,
                        null,
                        null,
                        legacyGlobalIndexMeta);
        assertThat(deserialized)
                .isEqualTo(createManifestCommittable(dataFiles, legacyHashIndexFile, devIndexFile));
        deserializedGlobalIndexMeta =
                ((CommitMessageImpl) deserialized.fileCommittables().get(0))
                        .newFilesIncrement()
                        .newIndexFiles()
                        .get(0)
                        .globalIndexMeta();
        assertThat(deserializedGlobalIndexMeta.sourceMeta()).isNull();
    }

    private static ManifestCommittable createManifestCommittable(
            List<DataFileMeta> dataFiles, IndexFileMeta hashIndexFile, IndexFileMeta devIndexFile) {
        CommitMessageImpl commitMessage =
                new CommitMessageImpl(
                        singleColumn("my_partition"),
                        11,
                        16,
                        new DataIncrement(
                                dataFiles,
                                dataFiles,
                                dataFiles,
                                Collections.singletonList(hashIndexFile),
                                Collections.singletonList(hashIndexFile)),
                        new CompactIncrement(
                                dataFiles,
                                dataFiles,
                                dataFiles,
                                Collections.singletonList(devIndexFile),
                                Collections.emptyList()));
        ManifestCommittable manifestCommittable =
                new ManifestCommittable(5, 202020L, Collections.singletonList(commitMessage));
        manifestCommittable.addProperty("k1", "v1");
        manifestCommittable.addProperty("k2", "v2");
        return manifestCommittable;
    }

    @Test
    public void testCompatibilityToV4CommitV11() throws IOException {
        String fileName = "manifest-committable-v11";

        SimpleStats keyStats =
                new SimpleStats(
                        singleColumn("min_key"),
                        singleColumn("max_key"),
                        fromLongArray(new Long[] {0L}));
        SimpleStats valueStats =
                new SimpleStats(
                        singleColumn("min_value"),
                        singleColumn("max_value"),
                        fromLongArray(new Long[] {0L}));
        DataFileMeta dataFile =
                DataFileMeta.create(
                        "my_file",
                        1024 * 1024,
                        1024,
                        singleColumn("min_key"),
                        singleColumn("max_key"),
                        keyStats,
                        valueStats,
                        15,
                        200,
                        5,
                        3,
                        Arrays.asList("extra1", "extra2"),
                        Timestamp.fromLocalDateTime(LocalDateTime.parse("2022-03-02T20:20:12")),
                        11L,
                        new byte[] {1, 2, 4},
                        FileSource.COMPACT,
                        Arrays.asList("field1", "field2", "field3"),
                        "hdfs://localhost:9000/path/to/file",
                        1L,
                        Arrays.asList("asdf", "qwer", "zxcv"),
                        null);
        List<DataFileMeta> dataFiles = Collections.singletonList(dataFile);
        GlobalIndexMeta globalIndexMeta =
                new GlobalIndexMeta(1L, 2L, 3, new int[] {5, 6, 7}, new byte[] {0x23, 0x45});
        IndexFileMeta hashIndexFile =
                new IndexFileMeta(
                        "my_index_type",
                        "my_index_file",
                        1024 * 100,
                        1002,
                        null,
                        null,
                        globalIndexMeta);

        LinkedHashMap<String, DeletionVectorMeta> dvRanges = new LinkedHashMap<>();
        dvRanges.put("dv_key1", new DeletionVectorMeta("dv_key1", 1, 2, 3L));
        dvRanges.put("dv_key2", new DeletionVectorMeta("dv_key2", 3, 4, 5L));
        IndexFileMeta devIndexFile =
                new IndexFileMeta(
                        "my_index_type",
                        "my_index_file",
                        1024 * 100,
                        1002,
                        dvRanges,
                        "external_path");

        CommitMessageImpl commitMessage =
                new CommitMessageImpl(
                        singleColumn("my_partition"),
                        11,
                        16,
                        new DataIncrement(
                                dataFiles,
                                dataFiles,
                                dataFiles,
                                Collections.singletonList(hashIndexFile),
                                Collections.singletonList(hashIndexFile)),
                        new CompactIncrement(
                                dataFiles,
                                dataFiles,
                                dataFiles,
                                Collections.singletonList(devIndexFile),
                                Collections.emptyList()));

        ManifestCommittable manifestCommittable =
                new ManifestCommittable(5, 202020L, Collections.singletonList(commitMessage));
        manifestCommittable.addProperty("k1", "v1");
        manifestCommittable.addProperty("k2", "v2");

        ManifestCommittableSerializer serializer = new ManifestCommittableSerializer();
        byte[] bytes = serializer.serialize(manifestCommittable);
        ManifestCommittable deserialized = serializer.deserialize(serializer.getVersion(), bytes);
        assertThat(deserialized).isEqualTo(manifestCommittable);

        byte[] oldBytes =
                IOUtils.readFully(
                        ManifestCommittableSerializerCompatibilityTest.class
                                .getClassLoader()
                                .getResourceAsStream("compatibility/" + fileName),
                        true);
        deserialized = serializer.deserialize(4, oldBytes);
        assertThat(deserialized).isEqualTo(manifestCommittable);
    }

    @Test
    public void testCompatibilityToV4CommitV10() throws IOException {
        String fileName = "manifest-committable-v10";

        SimpleStats keyStats =
                new SimpleStats(
                        singleColumn("min_key"),
                        singleColumn("max_key"),
                        fromLongArray(new Long[] {0L}));
        SimpleStats valueStats =
                new SimpleStats(
                        singleColumn("min_value"),
                        singleColumn("max_value"),
                        fromLongArray(new Long[] {0L}));
        DataFileMeta dataFile =
                DataFileMeta.create(
                        "my_file",
                        1024 * 1024,
                        1024,
                        singleColumn("min_key"),
                        singleColumn("max_key"),
                        keyStats,
                        valueStats,
                        15,
                        200,
                        5,
                        3,
                        Arrays.asList("extra1", "extra2"),
                        Timestamp.fromLocalDateTime(LocalDateTime.parse("2022-03-02T20:20:12")),
                        11L,
                        new byte[] {1, 2, 4},
                        FileSource.COMPACT,
                        Arrays.asList("field1", "field2", "field3"),
                        "hdfs://localhost:9000/path/to/file",
                        1L,
                        Arrays.asList("asdf", "qwer", "zxcv"),
                        null);
        List<DataFileMeta> dataFiles = Collections.singletonList(dataFile);
        IndexFileMeta hashIndexFile =
                new IndexFileMeta(
                        "my_index_type", "my_index_file", 1024 * 100, 1002, null, null, null);

        LinkedHashMap<String, DeletionVectorMeta> dvRanges = new LinkedHashMap<>();
        dvRanges.put("dv_key1", new DeletionVectorMeta("dv_key1", 1, 2, 3L));
        dvRanges.put("dv_key2", new DeletionVectorMeta("dv_key2", 3, 4, 5L));
        IndexFileMeta devIndexFile =
                new IndexFileMeta(
                        "my_index_type",
                        "my_index_file",
                        1024 * 100,
                        1002,
                        dvRanges,
                        "external_path");

        CommitMessageImpl commitMessage =
                new CommitMessageImpl(
                        singleColumn("my_partition"),
                        11,
                        16,
                        new DataIncrement(
                                dataFiles,
                                dataFiles,
                                dataFiles,
                                Collections.singletonList(hashIndexFile),
                                Collections.singletonList(hashIndexFile)),
                        new CompactIncrement(
                                dataFiles,
                                dataFiles,
                                dataFiles,
                                Collections.singletonList(devIndexFile),
                                Collections.emptyList()));

        ManifestCommittable manifestCommittable =
                new ManifestCommittable(5, 202020L, Collections.singletonList(commitMessage));
        manifestCommittable.addProperty("k1", "v1");
        manifestCommittable.addProperty("k2", "v2");

        ManifestCommittableSerializer serializer = new ManifestCommittableSerializer();
        byte[] bytes = serializer.serialize(manifestCommittable);
        ManifestCommittable deserialized = serializer.deserialize(serializer.getVersion(), bytes);
        assertThat(deserialized).isEqualTo(manifestCommittable);

        byte[] oldBytes =
                IOUtils.readFully(
                        ManifestCommittableSerializerCompatibilityTest.class
                                .getClassLoader()
                                .getResourceAsStream("compatibility/" + fileName),
                        true);
        deserialized = serializer.deserialize(4, oldBytes);
        assertThat(deserialized).isEqualTo(manifestCommittable);
    }

    @Test
    public void testCompatibilityToV4CommitV9() throws IOException {
        SimpleStats keyStats =
                new SimpleStats(
                        singleColumn("min_key"),
                        singleColumn("max_key"),
                        fromLongArray(new Long[] {0L}));
        SimpleStats valueStats =
                new SimpleStats(
                        singleColumn("min_value"),
                        singleColumn("max_value"),
                        fromLongArray(new Long[] {0L}));
        DataFileMeta dataFile =
                DataFileMeta.create(
                        "my_file",
                        1024 * 1024,
                        1024,
                        singleColumn("min_key"),
                        singleColumn("max_key"),
                        keyStats,
                        valueStats,
                        15,
                        200,
                        5,
                        3,
                        Arrays.asList("extra1", "extra2"),
                        Timestamp.fromLocalDateTime(LocalDateTime.parse("2022-03-02T20:20:12")),
                        11L,
                        new byte[] {1, 2, 4},
                        FileSource.COMPACT,
                        Arrays.asList("field1", "field2", "field3"),
                        "hdfs://localhost:9000/path/to/file",
                        1L,
                        Arrays.asList("asdf", "qwer", "zxcv"),
                        null);
        List<DataFileMeta> dataFiles = Collections.singletonList(dataFile);

        LinkedHashMap<String, DeletionVectorMeta> dvRanges = new LinkedHashMap<>();
        dvRanges.put("dv_key1", new DeletionVectorMeta("dv_key1", 1, 2, 3L));
        dvRanges.put("dv_key2", new DeletionVectorMeta("dv_key2", 3, 4, 5L));
        IndexFileMeta indexFile =
                new IndexFileMeta(
                        "my_index_type",
                        "my_index_file",
                        1024 * 100,
                        1002,
                        dvRanges,
                        "external_path");
        List<IndexFileMeta> indexFiles = Collections.singletonList(indexFile);

        CommitMessageImpl commitMessage =
                new CommitMessageImpl(
                        singleColumn("my_partition"),
                        11,
                        16,
                        new DataIncrement(dataFiles, dataFiles, dataFiles),
                        new CompactIncrement(
                                dataFiles,
                                dataFiles,
                                dataFiles,
                                indexFiles,
                                Collections.emptyList()));

        ManifestCommittable manifestCommittable =
                new ManifestCommittable(5, 202020L, Collections.singletonList(commitMessage));

        manifestCommittable.addProperty("k1", "v1");
        manifestCommittable.addProperty("k2", "v2");

        ManifestCommittableSerializer serializer = new ManifestCommittableSerializer();
        byte[] bytes = serializer.serialize(manifestCommittable);

        ManifestCommittable deserialized = serializer.deserialize(serializer.getVersion(), bytes);
        assertThat(deserialized).isEqualTo(manifestCommittable);

        byte[] oldBytes =
                IOUtils.readFully(
                        ManifestCommittableSerializerCompatibilityTest.class
                                .getClassLoader()
                                .getResourceAsStream("compatibility/manifest-committable-v9"),
                        true);
        deserialized = serializer.deserialize(4, oldBytes);
        assertThat(deserialized).isEqualTo(manifestCommittable);
    }

    @Test
    public void testCompatibilityToV4CommitV8() throws IOException {
        SimpleStats keyStats =
                new SimpleStats(
                        singleColumn("min_key"),
                        singleColumn("max_key"),
                        fromLongArray(new Long[] {0L}));
        SimpleStats valueStats =
                new SimpleStats(
                        singleColumn("min_value"),
                        singleColumn("max_value"),
                        fromLongArray(new Long[] {0L}));
        DataFileMeta dataFile =
                DataFileMeta.create(
                        "my_file",
                        1024 * 1024,
                        1024,
                        singleColumn("min_key"),
                        singleColumn("max_key"),
                        keyStats,
                        valueStats,
                        15,
                        200,
                        5,
                        3,
                        Arrays.asList("extra1", "extra2"),
                        Timestamp.fromLocalDateTime(LocalDateTime.parse("2022-03-02T20:20:12")),
                        11L,
                        new byte[] {1, 2, 4},
                        FileSource.COMPACT,
                        Arrays.asList("field1", "field2", "field3"),
                        "hdfs://localhost:9000/path/to/file",
                        1L,
                        null,
                        null);
        List<DataFileMeta> dataFiles = Collections.singletonList(dataFile);

        LinkedHashMap<String, DeletionVectorMeta> dvMetas = new LinkedHashMap<>();
        dvMetas.put("dv_key1", new DeletionVectorMeta("dv_key1", 1, 2, 3L));
        dvMetas.put("dv_key2", new DeletionVectorMeta("dv_key2", 3, 4, 5L));
        IndexFileMeta indexFile =
                new IndexFileMeta(
                        "my_index_type", "my_index_file", 1024 * 100, 1002, dvMetas, null);
        List<IndexFileMeta> indexFiles = Collections.singletonList(indexFile);

        CommitMessageImpl commitMessage =
                new CommitMessageImpl(
                        singleColumn("my_partition"),
                        11,
                        16,
                        new DataIncrement(dataFiles, dataFiles, dataFiles),
                        new CompactIncrement(
                                dataFiles,
                                dataFiles,
                                dataFiles,
                                indexFiles,
                                Collections.emptyList()));

        ManifestCommittable manifestCommittable =
                new ManifestCommittable(5, 202020L, Collections.singletonList(commitMessage));

        manifestCommittable.addProperty("k1", "v1");
        manifestCommittable.addProperty("k2", "v2");

        ManifestCommittableSerializer serializer = new ManifestCommittableSerializer();
        byte[] bytes = serializer.serialize(manifestCommittable);

        ManifestCommittable deserialized = serializer.deserialize(serializer.getVersion(), bytes);
        assertThat(deserialized).isEqualTo(manifestCommittable);

        byte[] oldBytes =
                IOUtils.readFully(
                        ManifestCommittableSerializerCompatibilityTest.class
                                .getClassLoader()
                                .getResourceAsStream("compatibility/manifest-committable-v8"),
                        true);
        deserialized = serializer.deserialize(4, oldBytes);
        assertThat(deserialized).isEqualTo(manifestCommittable);
    }

    @Test
    public void testCompatibilityToV4CommitV7() throws IOException {
        SimpleStats keyStats =
                new SimpleStats(
                        singleColumn("min_key"),
                        singleColumn("max_key"),
                        fromLongArray(new Long[] {0L}));
        SimpleStats valueStats =
                new SimpleStats(
                        singleColumn("min_value"),
                        singleColumn("max_value"),
                        fromLongArray(new Long[] {0L}));
        DataFileMeta dataFile =
                DataFileMeta.create(
                        "my_file",
                        1024 * 1024,
                        1024,
                        singleColumn("min_key"),
                        singleColumn("max_key"),
                        keyStats,
                        valueStats,
                        15,
                        200,
                        5,
                        3,
                        Arrays.asList("extra1", "extra2"),
                        Timestamp.fromLocalDateTime(LocalDateTime.parse("2022-03-02T20:20:12")),
                        11L,
                        new byte[] {1, 2, 4},
                        FileSource.COMPACT,
                        Arrays.asList("field1", "field2", "field3"),
                        "hdfs://localhost:9000/path/to/file",
                        null,
                        null,
                        null);
        List<DataFileMeta> dataFiles = Collections.singletonList(dataFile);

        LinkedHashMap<String, DeletionVectorMeta> dvRanges = new LinkedHashMap<>();
        dvRanges.put("dv_key1", new DeletionVectorMeta("dv_key1", 1, 2, 3L));
        dvRanges.put("dv_key2", new DeletionVectorMeta("dv_key2", 3, 4, 5L));
        IndexFileMeta indexFile =
                new IndexFileMeta(
                        "my_index_type", "my_index_file", 1024 * 100, 1002, dvRanges, null);
        List<IndexFileMeta> indexFiles = Collections.singletonList(indexFile);

        CommitMessageImpl commitMessage =
                new CommitMessageImpl(
                        singleColumn("my_partition"),
                        11,
                        16,
                        new DataIncrement(dataFiles, dataFiles, dataFiles),
                        new CompactIncrement(
                                dataFiles,
                                dataFiles,
                                dataFiles,
                                indexFiles,
                                Collections.emptyList()));

        ManifestCommittable manifestCommittable =
                new ManifestCommittable(5, 202020L, Collections.singletonList(commitMessage));

        manifestCommittable.addProperty("k1", "v1");
        manifestCommittable.addProperty("k2", "v2");

        ManifestCommittableSerializer serializer = new ManifestCommittableSerializer();
        byte[] bytes = serializer.serialize(manifestCommittable);
        ManifestCommittable deserialized = serializer.deserialize(serializer.getVersion(), bytes);
        assertThat(deserialized).isEqualTo(manifestCommittable);

        byte[] oldBytes =
                IOUtils.readFully(
                        ManifestCommittableSerializerCompatibilityTest.class
                                .getClassLoader()
                                .getResourceAsStream("compatibility/manifest-committable-v4-v7"),
                        true);
        deserialized = serializer.deserialize(4, oldBytes);
        assertThat(deserialized).isEqualTo(manifestCommittable);
    }

    @Test
    public void testCompatibilityToV3CommitV7() throws IOException {
        SimpleStats keyStats =
                new SimpleStats(
                        singleColumn("min_key"),
                        singleColumn("max_key"),
                        fromLongArray(new Long[] {0L}));
        SimpleStats valueStats =
                new SimpleStats(
                        singleColumn("min_value"),
                        singleColumn("max_value"),
                        fromLongArray(new Long[] {0L}));
        DataFileMeta dataFile =
                DataFileMeta.create(
                        "my_file",
                        1024 * 1024,
                        1024,
                        singleColumn("min_key"),
                        singleColumn("max_key"),
                        keyStats,
                        valueStats,
                        15,
                        200,
                        5,
                        3,
                        Arrays.asList("extra1", "extra2"),
                        Timestamp.fromLocalDateTime(LocalDateTime.parse("2022-03-02T20:20:12")),
                        11L,
                        new byte[] {1, 2, 4},
                        FileSource.COMPACT,
                        Arrays.asList("field1", "field2", "field3"),
                        "hdfs://localhost:9000/path/to/file",
                        null,
                        null,
                        null);
        List<DataFileMeta> dataFiles = Collections.singletonList(dataFile);

        LinkedHashMap<String, DeletionVectorMeta> dvRanges = new LinkedHashMap<>();
        dvRanges.put("dv_key1", new DeletionVectorMeta("dv_key1", 1, 2, 3L));
        dvRanges.put("dv_key2", new DeletionVectorMeta("dv_key2", 3, 4, 5L));
        IndexFileMeta indexFile =
                new IndexFileMeta(
                        "my_index_type", "my_index_file", 1024 * 100, 1002, dvRanges, null);
        List<IndexFileMeta> indexFiles = Collections.singletonList(indexFile);

        CommitMessageImpl commitMessage =
                new CommitMessageImpl(
                        singleColumn("my_partition"),
                        11,
                        16,
                        new DataIncrement(dataFiles, dataFiles, dataFiles),
                        new CompactIncrement(
                                dataFiles,
                                dataFiles,
                                dataFiles,
                                indexFiles,
                                Collections.emptyList()));

        ManifestCommittable manifestCommittable =
                new ManifestCommittable(5, 202020L, Collections.singletonList(commitMessage));

        ManifestCommittableSerializer serializer = new ManifestCommittableSerializer();
        byte[] bytes = serializer.serialize(manifestCommittable);
        ManifestCommittable deserialized = serializer.deserialize(serializer.getVersion(), bytes);
        assertThat(deserialized).isEqualTo(manifestCommittable);

        byte[] oldBytes =
                IOUtils.readFully(
                        ManifestCommittableSerializerCompatibilityTest.class
                                .getClassLoader()
                                .getResourceAsStream("compatibility/manifest-committable-v7"),
                        true);
        deserialized = serializer.deserialize(3, oldBytes);
        assertThat(deserialized).isEqualTo(manifestCommittable);
    }

    @Test
    public void testCompatibilityToV3CommitV6() throws IOException {
        SimpleStats keyStats =
                new SimpleStats(
                        singleColumn("min_key"),
                        singleColumn("max_key"),
                        fromLongArray(new Long[] {0L}));
        SimpleStats valueStats =
                new SimpleStats(
                        singleColumn("min_value"),
                        singleColumn("max_value"),
                        fromLongArray(new Long[] {0L}));
        DataFileMeta dataFile =
                DataFileMeta.create(
                        "my_file",
                        1024 * 1024,
                        1024,
                        singleColumn("min_key"),
                        singleColumn("max_key"),
                        keyStats,
                        valueStats,
                        15,
                        200,
                        5,
                        3,
                        Arrays.asList("extra1", "extra2"),
                        Timestamp.fromLocalDateTime(LocalDateTime.parse("2022-03-02T20:20:12")),
                        11L,
                        new byte[] {1, 2, 4},
                        FileSource.COMPACT,
                        Arrays.asList("field1", "field2", "field3"),
                        "hdfs://localhost:9000/path/to/file",
                        null,
                        null,
                        null);
        List<DataFileMeta> dataFiles = Collections.singletonList(dataFile);

        LinkedHashMap<String, DeletionVectorMeta> dvRanges = new LinkedHashMap<>();
        dvRanges.put("dv_key1", new DeletionVectorMeta("dv_key1", 1, 2, 3L));
        dvRanges.put("dv_key2", new DeletionVectorMeta("dv_key2", 3, 4, 5L));
        IndexFileMeta indexFile =
                new IndexFileMeta(
                        "my_index_type", "my_index_file", 1024 * 100, 1002, dvRanges, null);
        List<IndexFileMeta> indexFiles = Collections.singletonList(indexFile);

        CommitMessageImpl commitMessage =
                new CommitMessageImpl(
                        singleColumn("my_partition"),
                        11,
                        null,
                        new DataIncrement(dataFiles, dataFiles, dataFiles),
                        new CompactIncrement(
                                dataFiles,
                                dataFiles,
                                dataFiles,
                                indexFiles,
                                Collections.emptyList()));

        ManifestCommittable manifestCommittable =
                new ManifestCommittable(5, 202020L, Collections.singletonList(commitMessage));

        ManifestCommittableSerializer serializer = new ManifestCommittableSerializer();
        byte[] bytes = serializer.serialize(manifestCommittable);
        ManifestCommittable deserialized = serializer.deserialize(serializer.getVersion(), bytes);
        assertThat(deserialized).isEqualTo(manifestCommittable);

        byte[] oldBytes =
                IOUtils.readFully(
                        ManifestCommittableSerializerCompatibilityTest.class
                                .getClassLoader()
                                .getResourceAsStream("compatibility/manifest-committable-v6"),
                        true);
        deserialized = serializer.deserialize(3, oldBytes);
        assertThat(deserialized).isEqualTo(manifestCommittable);
    }

    @Test
    public void testCompatibilityToV3CommitV5() throws IOException {
        SimpleStats keyStats =
                new SimpleStats(
                        singleColumn("min_key"),
                        singleColumn("max_key"),
                        fromLongArray(new Long[] {0L}));
        SimpleStats valueStats =
                new SimpleStats(
                        singleColumn("min_value"),
                        singleColumn("max_value"),
                        fromLongArray(new Long[] {0L}));
        DataFileMeta dataFile =
                DataFileMeta.create(
                        "my_file",
                        1024 * 1024,
                        1024,
                        singleColumn("min_key"),
                        singleColumn("max_key"),
                        keyStats,
                        valueStats,
                        15,
                        200,
                        5,
                        3,
                        Arrays.asList("extra1", "extra2"),
                        Timestamp.fromLocalDateTime(LocalDateTime.parse("2022-03-02T20:20:12")),
                        11L,
                        new byte[] {1, 2, 4},
                        FileSource.COMPACT,
                        Arrays.asList("field1", "field2", "field3"),
                        null,
                        null,
                        null,
                        null);
        List<DataFileMeta> dataFiles = Collections.singletonList(dataFile);

        LinkedHashMap<String, DeletionVectorMeta> dvRanges = new LinkedHashMap<>();
        dvRanges.put("dv_key1", new DeletionVectorMeta("dv_key1", 1, 2, 3L));
        dvRanges.put("dv_key2", new DeletionVectorMeta("dv_key2", 3, 4, 5L));
        IndexFileMeta indexFile =
                new IndexFileMeta(
                        "my_index_type", "my_index_file", 1024 * 100, 1002, dvRanges, null);
        List<IndexFileMeta> indexFiles = Collections.singletonList(indexFile);

        CommitMessageImpl commitMessage =
                new CommitMessageImpl(
                        singleColumn("my_partition"),
                        11,
                        null,
                        new DataIncrement(dataFiles, dataFiles, dataFiles),
                        new CompactIncrement(
                                dataFiles,
                                dataFiles,
                                dataFiles,
                                indexFiles,
                                Collections.emptyList()));

        ManifestCommittable manifestCommittable =
                new ManifestCommittable(5, 202020L, Collections.singletonList(commitMessage));

        ManifestCommittableSerializer serializer = new ManifestCommittableSerializer();
        byte[] bytes = serializer.serialize(manifestCommittable);
        ManifestCommittable deserialized = serializer.deserialize(serializer.getVersion(), bytes);
        assertThat(deserialized).isEqualTo(manifestCommittable);
        byte[] oldBytes =
                IOUtils.readFully(
                        ManifestCommittableSerializerCompatibilityTest.class
                                .getClassLoader()
                                .getResourceAsStream("compatibility/manifest-committable-v5"),
                        true);
        deserialized = serializer.deserialize(3, oldBytes);
        assertThat(deserialized).isEqualTo(manifestCommittable);
    }

    @Test
    public void testCompatibilityToV3CommitV4() throws IOException {
        SimpleStats keyStats =
                new SimpleStats(
                        singleColumn("min_key"),
                        singleColumn("max_key"),
                        fromLongArray(new Long[] {0L}));
        SimpleStats valueStats =
                new SimpleStats(
                        singleColumn("min_value"),
                        singleColumn("max_value"),
                        fromLongArray(new Long[] {0L}));
        DataFileMeta dataFile =
                DataFileMeta.create(
                        "my_file",
                        1024 * 1024,
                        1024,
                        singleColumn("min_key"),
                        singleColumn("max_key"),
                        keyStats,
                        valueStats,
                        15,
                        200,
                        5,
                        3,
                        Arrays.asList("extra1", "extra2"),
                        Timestamp.fromLocalDateTime(LocalDateTime.parse("2022-03-02T20:20:12")),
                        11L,
                        new byte[] {1, 2, 4},
                        FileSource.COMPACT,
                        Arrays.asList("field1", "field2", "field3"),
                        null,
                        null,
                        null,
                        null);
        List<DataFileMeta> dataFiles = Collections.singletonList(dataFile);

        LinkedHashMap<String, DeletionVectorMeta> dvRanges = new LinkedHashMap<>();
        dvRanges.put("dv_key1", new DeletionVectorMeta("dv_key1", 1, 2, null));
        dvRanges.put("dv_key2", new DeletionVectorMeta("dv_key2", 3, 4, null));
        IndexFileMeta indexFile =
                new IndexFileMeta(
                        "my_index_type", "my_index_file", 1024 * 100, 1002, dvRanges, null);
        List<IndexFileMeta> indexFiles = Collections.singletonList(indexFile);

        CommitMessageImpl commitMessage =
                new CommitMessageImpl(
                        singleColumn("my_partition"),
                        11,
                        null,
                        new DataIncrement(dataFiles, dataFiles, dataFiles),
                        new CompactIncrement(
                                dataFiles,
                                dataFiles,
                                dataFiles,
                                indexFiles,
                                Collections.emptyList()));

        ManifestCommittable manifestCommittable =
                new ManifestCommittable(5, 202020L, Collections.singletonList(commitMessage));

        ManifestCommittableSerializer serializer = new ManifestCommittableSerializer();
        byte[] bytes = serializer.serialize(manifestCommittable);
        ManifestCommittable deserialized = serializer.deserialize(serializer.getVersion(), bytes);
        assertThat(deserialized).isEqualTo(manifestCommittable);

        byte[] v2Bytes =
                IOUtils.readFully(
                        ManifestCommittableSerializerCompatibilityTest.class
                                .getClassLoader()
                                .getResourceAsStream("compatibility/manifest-committable-v4"),
                        true);
        deserialized = serializer.deserialize(3, v2Bytes);
        assertThat(deserialized).isEqualTo(manifestCommittable);
    }

    @Test
    public void testCompatibilityToV3CommitV3() throws IOException {
        SimpleStats keyStats =
                new SimpleStats(
                        singleColumn("min_key"),
                        singleColumn("max_key"),
                        fromLongArray(new Long[] {0L}));
        SimpleStats valueStats =
                new SimpleStats(
                        singleColumn("min_value"),
                        singleColumn("max_value"),
                        fromLongArray(new Long[] {0L}));
        DataFileMeta dataFile =
                DataFileMeta.create(
                        "my_file",
                        1024 * 1024,
                        1024,
                        singleColumn("min_key"),
                        singleColumn("max_key"),
                        keyStats,
                        valueStats,
                        15,
                        200,
                        5,
                        3,
                        Arrays.asList("extra1", "extra2"),
                        Timestamp.fromLocalDateTime(LocalDateTime.parse("2022-03-02T20:20:12")),
                        11L,
                        new byte[] {1, 2, 4},
                        FileSource.COMPACT,
                        null,
                        null,
                        null,
                        null,
                        null);
        List<DataFileMeta> dataFiles = Collections.singletonList(dataFile);

        LinkedHashMap<String, DeletionVectorMeta> dvRanges = new LinkedHashMap<>();
        dvRanges.put("dv_key1", new DeletionVectorMeta("dv_key1", 1, 2, null));
        dvRanges.put("dv_key2", new DeletionVectorMeta("dv_key2", 3, 4, null));
        IndexFileMeta indexFile =
                new IndexFileMeta(
                        "my_index_type", "my_index_file", 1024 * 100, 1002, dvRanges, null);
        List<IndexFileMeta> indexFiles = Collections.singletonList(indexFile);

        CommitMessageImpl commitMessage =
                new CommitMessageImpl(
                        singleColumn("my_partition"),
                        11,
                        null,
                        new DataIncrement(dataFiles, dataFiles, dataFiles),
                        new CompactIncrement(
                                dataFiles,
                                dataFiles,
                                dataFiles,
                                indexFiles,
                                Collections.emptyList()));

        ManifestCommittable manifestCommittable =
                new ManifestCommittable(5, 202020L, Collections.singletonList(commitMessage));

        ManifestCommittableSerializer serializer = new ManifestCommittableSerializer();
        byte[] bytes = serializer.serialize(manifestCommittable);
        ManifestCommittable deserialized = serializer.deserialize(serializer.getVersion(), bytes);
        assertThat(deserialized).isEqualTo(manifestCommittable);

        byte[] oldBytes =
                IOUtils.readFully(
                        ManifestCommittableSerializerCompatibilityTest.class
                                .getClassLoader()
                                .getResourceAsStream("compatibility/manifest-committable-v3"),
                        true);
        deserialized = serializer.deserialize(3, oldBytes);
        assertThat(deserialized).isEqualTo(manifestCommittable);
    }

    @Test
    public void testCompatibilityToV2CommitV2() throws IOException {
        SimpleStats keyStats =
                new SimpleStats(
                        singleColumn("min_key"),
                        singleColumn("max_key"),
                        fromLongArray(new Long[] {0L}));
        SimpleStats valueStats =
                new SimpleStats(
                        singleColumn("min_value"),
                        singleColumn("max_value"),
                        fromLongArray(new Long[] {0L}));
        DataFileMeta dataFile =
                DataFileMeta.create(
                        "my_file",
                        1024 * 1024,
                        1024,
                        singleColumn("min_key"),
                        singleColumn("max_key"),
                        keyStats,
                        valueStats,
                        15,
                        200,
                        5,
                        3,
                        Arrays.asList("extra1", "extra2"),
                        Timestamp.fromLocalDateTime(LocalDateTime.parse("2022-03-02T20:20:12")),
                        11L,
                        new byte[] {1, 2, 4},
                        null,
                        null,
                        null,
                        null,
                        null,
                        null);
        List<DataFileMeta> dataFiles = Collections.singletonList(dataFile);

        LinkedHashMap<String, DeletionVectorMeta> dvRanges = new LinkedHashMap<>();
        dvRanges.put("dv_key1", new DeletionVectorMeta("dv_key1", 1, 2, null));
        dvRanges.put("dv_key2", new DeletionVectorMeta("dv_key2", 3, 4, null));
        IndexFileMeta indexFile =
                new IndexFileMeta(
                        "my_index_type", "my_index_file", 1024 * 100, 1002, dvRanges, null);
        List<IndexFileMeta> indexFiles = Collections.singletonList(indexFile);

        CommitMessageImpl commitMessage =
                new CommitMessageImpl(
                        singleColumn("my_partition"),
                        11,
                        null,
                        new DataIncrement(dataFiles, dataFiles, dataFiles),
                        new CompactIncrement(
                                dataFiles,
                                dataFiles,
                                dataFiles,
                                indexFiles,
                                Collections.emptyList()));

        ManifestCommittable manifestCommittable =
                new ManifestCommittable(5, 202020L, Collections.singletonList(commitMessage));

        ManifestCommittableSerializer serializer = new ManifestCommittableSerializer();
        byte[] bytes = serializer.serialize(manifestCommittable);
        ManifestCommittable deserialized = serializer.deserialize(serializer.getVersion(), bytes);
        assertThat(deserialized).isEqualTo(manifestCommittable);

        byte[] v2Bytes =
                IOUtils.readFully(
                        ManifestCommittableSerializerCompatibilityTest.class
                                .getClassLoader()
                                .getResourceAsStream("compatibility/manifest-committable-v2"),
                        true);
        deserialized = serializer.deserialize(2, v2Bytes);
        assertThat(deserialized).isEqualTo(manifestCommittable);
    }

    @Test
    public void testCompatibilityToVersion2PaimonV07() throws IOException {
        SimpleStats keyStats =
                new SimpleStats(
                        singleColumn("min_key"),
                        singleColumn("max_key"),
                        fromLongArray(new Long[] {0L}));
        SimpleStats valueStats =
                new SimpleStats(
                        singleColumn("min_value"),
                        singleColumn("max_value"),
                        fromLongArray(new Long[] {0L}));
        DataFileMeta dataFile =
                DataFileMeta.create(
                        "my_file",
                        1024 * 1024,
                        1024,
                        singleColumn("min_key"),
                        singleColumn("max_key"),
                        keyStats,
                        valueStats,
                        15,
                        200,
                        5,
                        3,
                        Arrays.asList("extra1", "extra2"),
                        Timestamp.fromLocalDateTime(LocalDateTime.parse("2022-03-02T20:20:12")),
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null);
        List<DataFileMeta> dataFiles = Collections.singletonList(dataFile);

        IndexFileMeta indexFile =
                new IndexFileMeta(
                        "my_index_type", "my_index_file", 1024 * 100, 1002, null, null, null);
        List<IndexFileMeta> indexFiles = Collections.singletonList(indexFile);

        CommitMessageImpl commitMessage =
                new CommitMessageImpl(
                        singleColumn("my_partition"),
                        11,
                        null,
                        new DataIncrement(dataFiles, Collections.emptyList(), dataFiles),
                        new CompactIncrement(
                                dataFiles,
                                dataFiles,
                                dataFiles,
                                indexFiles,
                                Collections.emptyList()));

        ManifestCommittable manifestCommittable =
                new ManifestCommittable(5, 202020L, Collections.singletonList(commitMessage));

        ManifestCommittableSerializer serializer = new ManifestCommittableSerializer();
        byte[] bytes = serializer.serialize(manifestCommittable);
        ManifestCommittable deserialized = serializer.deserialize(serializer.getVersion(), bytes);
        assertThat(deserialized).isEqualTo(manifestCommittable);

        byte[] v2Bytes =
                IOUtils.readFully(
                        ManifestCommittableSerializerCompatibilityTest.class
                                .getClassLoader()
                                .getResourceAsStream("compatibility/manifest-committable-v2-0.7"),
                        true);
        deserialized = serializer.deserialize(2, v2Bytes);
        assertThat(deserialized).isEqualTo(manifestCommittable);
    }
}
