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

package org.apache.paimon.vector.index;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.globalindex.CentroidVectorGlobalIndexer.TrainingBatch;
import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.GlobalIndexReader;
import org.apache.paimon.globalindex.GlobalIndexSingleColumnWriter;
import org.apache.paimon.globalindex.OffsetGlobalIndexReader;
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.globalindex.ScoredGlobalIndexResult;
import org.apache.paimon.globalindex.VectorGlobalModelTrainer;
import org.apache.paimon.globalindex.VectorTrainingModel;
import org.apache.paimon.globalindex.io.GlobalIndexFileReader;
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;
import org.apache.paimon.index.vector.CentroidRouter;
import org.apache.paimon.index.vector.VectorIndexReader;
import org.apache.paimon.index.vector.VectorIndexTrainer;
import org.apache.paimon.index.vector.VectorIndexTraining;
import org.apache.paimon.predicate.BatchVectorSearch;
import org.apache.paimon.predicate.VectorSearch;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RoaringNavigableMap64;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real native routing, file pruning, absolute row-ID filters and batch dispatch. */
class CentroidVectorGlobalIndexTest {
    @TempDir java.nio.file.Path temp;
    private static final long BASE = 1L << 35;

    @Test
    void distributedSampleBudgetUsesCorpusSizeAndConfiguredRatio() {
        Map<String, String> options = new HashMap<>();
        options.put("index.type", "ivf_flat");
        options.put("metric", "l2");
        options.put("dimension", "2");
        NativeCentroidVectorGlobalIndexer indexer =
                new NativeCentroidVectorGlobalIndexer(
                        new ArrayType(new FloatType()), options, "ivf-flat", 0.25);
        assertThat(indexer.planGlobalTrainingSample(5)).containsExactly(0L, 2L);
        assertThat(indexer.planGlobalTrainingSample(1000000)).hasSize(65536);
        assertThatThrownBy(() -> indexer.planGlobalTrainingSample(0))
                .hasMessageContaining("without non-null vectors");
        // Explicit expected size still controls automatic nlist on a sampled build.
        options.put("expected-vector-count", "16000000");
        assertThat(indexer.planGlobalTrainingSample(2000000)).hasSize(4096 * 64);
        options.put("nlist", "2048");
        assertThat(indexer.planGlobalTrainingSample(1000000)).hasSize(2048 * 64);
    }

    @Test
    void diskAnnLocalSettingsDoNotChangeTheOuterSampleOrCenters() throws Exception {
        Map<String, String> coarse = new HashMap<>();
        coarse.put("index.type", "ivf_flat");
        coarse.put("dimension", "128");
        coarse.put("metric", "l2");
        coarse.put("nlist", "64");
        long[] expected;
        try (VectorIndexTrainer trainer = VectorIndexTrainer.create(coarse)) {
            expected = trainer.planTrainingSample(100000);
        }
        assertThat(expected).hasSize(65536);
        float[] vectors = new float[256 * 128];
        for (int i = 0; i < vectors.length; i++) {
            vectors[i] = (i * 19) % 401;
        }
        Map<String, String> disk = new HashMap<>(coarse);
        disk.put("index.type", "diskann");
        disk.put("nlist", "auto");
        try (VectorIndexTraining training = VectorIndexTrainer.train(coarse, vectors, 256);
                CentroidRouter reference = CentroidRouter.fromTraining(training)) {
            for (int seed : new int[] {42, 93}) {
                disk.put("diskann.seed", Integer.toString(seed));
                disk.put("diskann.memory-budget-bytes", seed == 42 ? "8388608" : "4194304");
                NativeCentroidVectorGlobalIndexer indexer =
                        new NativeCentroidVectorGlobalIndexer(
                                new ArrayType(new FloatType()), disk, "diskann", 1.0);
                assertThat(indexer.planGlobalTrainingSample(100000)).isEqualTo(expected);
                try (VectorTrainingModel model =
                        indexer.trainGlobalModel(
                                100000,
                                Collections.singletonList(new TrainingBatch(vectors, 256))
                                        .iterator())) {
                    assertThat(model.serialize()).isEqualTo(reference.serialize());
                }
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"ivf-flat", "ivf-pq", "ivf-sq", "ivf-rq", "diskann"})
    void plannedRowsReproduceStreamingModel(String type) throws Exception {
        int rows = 142003;
        Map<String, String> options = new HashMap<>();
        options.put("index.type", type.replace('-', '_'));
        options.put("dimension", "2");
        options.put("nlist", "4");
        options.put("metric", "l2");
        if (type.equals("diskann") || type.equals("ivf-pq")) {
            options.put("pq.m", "1");
        }
        if (type.equals("diskann")) {
            options.put("diskann.seed", "93");
        }
        NativeCentroidVectorGlobalIndexer indexer =
                new NativeCentroidVectorGlobalIndexer(
                        new ArrayType(new FloatType()), options, type, 0.5);
        long[] positions = indexer.planGlobalTrainingSample(rows);
        assertThat(positions.length).isLessThan((rows + 1) / 2);
        float[] selected = new float[2 * positions.length];
        for (int i = 0; i < positions.length; i++) {
            System.arraycopy(testVector((int) positions[i]), 0, selected, 2 * i, 2);
        }
        try (VectorGlobalModelTrainer streaming = indexer.createGlobalModelTrainer()) {
            for (int i = 0; i < rows; i++) {
                streaming.write(testVector(i), BASE + i);
            }
            try (VectorTrainingModel expected = streaming.finishTraining();
                    VectorTrainingModel actual =
                            indexer.trainGlobalModel(
                                    rows,
                                    Collections.singletonList(
                                                    new TrainingBatch(selected, positions.length))
                                            .iterator())) {
                assertThat(actual.serialize()).isEqualTo(expected.serialize());
                try (CentroidRouter routing = CentroidRouter.deserialize(actual.serialize())) {
                    assertThat(routing.nlist()).isEqualTo(4);
                }
            }
        }
    }

    private static float[] testVector(int i) {
        return new float[] {(i * 47) % 4001, (i * 31) % 1471};
    }

    @Test
    void distributedBatchesTrainOnceUsingTheFullCorpusShape() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put("index.type", "ivf_flat");
        options.put("metric", "l2");
        options.put("dimension", "2");
        NativeCentroidVectorGlobalIndexer indexer =
                new NativeCentroidVectorGlobalIndexer(
                        new ArrayType(new FloatType()), options, "ivf-flat", 0.0625);
        float[] samples = new float[128];
        for (int i = 0; i < samples.length; i++) {
            samples[i] = (i * 19) % 73;
        }
        Map<String, String> reference = new HashMap<>(options);
        reference.put("expected-vector-count", "1024");
        List<TrainingBatch> batches =
                Arrays.asList(
                        new TrainingBatch(Arrays.copyOfRange(samples, 0, 50), 25),
                        new TrainingBatch(Arrays.copyOfRange(samples, 50, samples.length), 39));
        try (VectorTrainingModel actual = indexer.trainGlobalModel(1024, batches.iterator());
                VectorIndexTraining coarse = VectorIndexTrainer.train(reference, samples, 64);
                CentroidRouter expected = CentroidRouter.fromTraining(coarse)) {
            assertThat(actual.nlist()).isEqualTo(16);
            assertThat(actual.serialize()).isEqualTo(expected.serialize());
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"ivf-flat", "ivf-pq", "ivf-sq", "ivf-rq", "diskann"})
    void testRoutingPrunesFilesAndAppliesRangeOffset(String type) throws Exception {
        FileIO io = new LocalFileIO();
        Path directory = new Path(temp.toString());
        boolean diskann = "diskann".equals(type);
        Map<String, String> options = new HashMap<>();
        options.put("dimension", "2");
        options.put("index.type", type.replace('-', '_'));
        options.put("metric", "l2");
        options.put("nlist", "4");
        if (!diskann) {
            options.put("centroid.local-nlist", "8");
        }
        if ("ivf-pq".equals(type) || diskann) {
            options.put("pq.m", "1");
        }
        NativeCentroidVectorGlobalIndexer indexer =
                new NativeCentroidVectorGlobalIndexer(
                        new ArrayType(new FloatType()), options, type, 1.0);
        GlobalIndexFileWriter files = fileWriter(io, directory);
        float[][] vectors = new float[256][];
        for (int i = 0; i < vectors.length; i++) {
            vectors[i] = new float[] {(i / 64) * 100f + (i % 64) * 0.1f, i % 11};
        }
        List<GlobalIndexIOMeta> metas = new ArrayList<>();
        int[] centroidByRow = new int[256];
        GlobalIndexIOMeta emptyMeta;
        try (VectorGlobalModelTrainer trainer = indexer.createGlobalModelTrainer()) {
            for (int i = 0; i < vectors.length; i++) {
                trainer.write(vectors[i], BASE + 101L * i);
            }
            try (VectorTrainingModel model = trainer.finishTraining()) {
                assertThatThrownBy(() -> trainer.write(vectors[0], BASE))
                        .isInstanceOf(IllegalStateException.class)
                        .hasMessageContaining("finished");
                assertThatThrownBy(trainer::finishTraining)
                        .isInstanceOf(IllegalStateException.class)
                        .hasMessageContaining("finished");
                String id = "test-model";
                String name = files.newFileName("routing");
                try (PositionOutputStream out = files.newOutputStream(name)) {
                    VectorTrainingModelIO.write(out, model.serialize(), id);
                }
                Path modelPath = new Path(directory, name);
                metas.add(
                        new GlobalIndexIOMeta(
                                modelPath,
                                io.getFileSize(modelPath),
                                0,
                                VectorIndexShardMeta.routing(id, model.nlist()).serialize()));
                GlobalIndexSingleColumnWriter empty = model.createCentroidWriter(files, 0, id);
                try {
                    ResultEntry result = empty.finish().get(0);
                    Path path = new Path(directory, result.fileName());
                    emptyMeta = new GlobalIndexIOMeta(path, io.getFileSize(path), 0, result.meta());
                    assertThat(io.getFileSize(path)).isEqualTo(8);
                } finally {
                    ((Closeable) empty).close();
                }
                for (int c = 0; c < model.nlist(); c++) {
                    GlobalIndexSingleColumnWriter writer = model.createCentroidWriter(files, c, id);
                    try {
                        assertThatThrownBy(() -> writer.write(vectors[0], -1L))
                                .isInstanceOf(IllegalArgumentException.class);
                        for (int i = 0; i < vectors.length; i++) {
                            centroidByRow[i] = model.assignCentroid(vectors[i]);
                            if (centroidByRow[i] == c) {
                                writer.write(vectors[i], 101L * i);
                            }
                        }
                        ResultEntry result = writer.finish().get(0);
                        Path path = new Path(directory, result.fileName());
                        if (result.rowCount() > 0) {
                            VectorIndexShardMeta descriptor =
                                    VectorIndexShardMeta.deserialize(result.meta());
                            assertThat(descriptor.centroid()).isEqualTo(c);
                            assertThat(descriptor.payloadMeta())
                                    .isEqualTo(new VectorIndexMeta().serialize());
                            try (SeekableInputStream in = io.newInputStream(path);
                                    VectorIndexReader local =
                                            new VectorIndexReader(
                                                    new NativeVectorGlobalIndexReader
                                                            .SeekableStreamVectorIndexInput(in))) {
                                assertThat(local.metadata().indexType())
                                        .isEqualTo(type.replace('-', '_'));
                                if (!diskann) {
                                    assertThat(local.metadata().nlist()).isEqualTo(8);
                                }
                            }
                        }
                        metas.add(
                                new GlobalIndexIOMeta(
                                        path,
                                        io.getFileSize(path),
                                        result.rowCount(),
                                        result.meta()));
                    } finally {
                        ((Closeable) writer).close();
                    }
                }
            }
        }
        // A single worker also guards against waiting for child futures on the same executor.
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Map<Path, AtomicInteger> opened = new ConcurrentHashMap<>();
        GlobalIndexFileReader fileReader =
                meta -> {
                    opened.computeIfAbsent(meta.filePath(), p -> new AtomicInteger())
                            .incrementAndGet();
                    return io.newInputStream(meta.filePath());
                };
        long rowCount = 101L * (vectors.length - 1) + 1;
        try (GlobalIndexReader reader =
                new OffsetGlobalIndexReader(
                        indexer.createReader(fileReader, metas, rowCount, null, executor),
                        BASE,
                        BASE + rowCount - 1)) {
            try (GlobalIndexReader empty =
                    new NativeVectorGlobalIndexReader(
                            fileReader,
                            Collections.singletonList(emptyMeta),
                            new ArrayType(new FloatType()),
                            executor,
                            type)) {
                assertThat(empty.visitVectorSearch(new VectorSearch(vectors[0], 1, "v")).get())
                        .isEmpty();
                assertThat(
                                empty.visitBatchVectorSearch(
                                                new BatchVectorSearch(
                                                        new float[][] {vectors[0], vectors[1]},
                                                        1,
                                                        "v"))
                                        .get())
                        .allSatisfy(result -> assertThat(result).isEmpty());
                assertThat(opened).isEmpty();
            }
            Map<String, String> search = new HashMap<>();
            search.put("centroid.nprobe", "1");
            if (diskann) {
                search.put("diskann.l_search", "256");
            } else {
                search.put("ivf.nprobe", "2");
            }
            List<Optional<ScoredGlobalIndexResult>> empty =
                    reader.visitBatchVectorSearch(
                                    new BatchVectorSearch(
                                                    new float[][] {vectors[0], vectors[200]},
                                                    1,
                                                    "v",
                                                    search)
                                            .withIncludeRowIds(new RoaringNavigableMap64()))
                            .get(30, TimeUnit.SECONDS);
            assertThat(empty).allSatisfy(result -> assertThat(result).isEmpty());
            assertThat(opened).isEmpty();
            ScoredGlobalIndexResult one =
                    reader.visitVectorSearch(new VectorSearch(vectors[0], 1, "v", search))
                            .get(30, TimeUnit.SECONDS)
                            .get();
            assertThat(one.results().contains(BASE)).isTrue();
            assertThat(opened).hasSize(2); // routing model and the selected centroid only
            for (GlobalIndexIOMeta file : metas.subList(1, metas.size())) {
                boolean selected =
                        VectorIndexShardMeta.deserialize(file.metadata()).centroid()
                                == centroidByRow[0];
                assertThat(opened.containsKey(file.filePath())).isEqualTo(selected);
            }
            float[][] queries = {vectors[0], vectors[200], vectors[0]};
            List<Optional<ScoredGlobalIndexResult>> batch =
                    reader.visitBatchVectorSearch(new BatchVectorSearch(queries, 1, "v", search))
                            .get(30, TimeUnit.SECONDS);
            assertThat(batch).hasSize(3);
            assertThat(batch.get(0).get().results().contains(BASE)).isTrue();
            assertThat(batch.get(1).get().results().contains(BASE + 101L * 200)).isTrue();
            assertThat(batch.get(2).get().results().contains(BASE)).isTrue();
            assertThat(opened.values()).allSatisfy(count -> assertThat(count.get()).isEqualTo(1));

            RoaringNavigableMap64 include = new RoaringNavigableMap64();
            include.add(BASE - 1);
            include.add(BASE + 101L * 200);
            include.add(BASE + rowCount);
            ScoredGlobalIndexResult filtered =
                    reader.visitVectorSearch(
                                    new VectorSearch(vectors[0], 1, "v").withIncludeRowIds(include))
                            .get(30, TimeUnit.SECONDS)
                            .get();
            assertThat(filtered.results().getLongCardinality()).isEqualTo(1);
            assertThat(filtered.results().contains(BASE + 101L * 200)).isTrue();
            List<Optional<ScoredGlobalIndexResult>> filteredBatch =
                    reader.visitBatchVectorSearch(
                                    new BatchVectorSearch(queries, 1, "v")
                                            .withIncludeRowIds(include))
                            .get(30, TimeUnit.SECONDS);
            assertThat(filteredBatch).hasSize(queries.length);
            for (Optional<ScoredGlobalIndexResult> result : filteredBatch) {
                assertThat(result.get().results().toArray(1)).containsExactly(BASE + 101L * 200);
            }

            assertThat(
                            reader.visitVectorSearch(
                                            new VectorSearch(vectors[0], 1, "v", search)
                                                    .withIncludeRowIds(new RoaringNavigableMap64()))
                                    .get(30, TimeUnit.SECONDS))
                    .isEmpty();
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void joinProbesUsingTheNearestShardLayoutsForSingleAndBatchSearch() throws Exception {
        FileIO io = new LocalFileIO();
        NativeCentroidVectorGlobalIndexer indexer = flatIndexer(64);
        float[][] landmarks = new float[64][8];
        List<GlobalIndexIOMeta> metas = buildMixedShards(io, indexer, landmarks);
        Set<Path> opened = ConcurrentHashMap.newKeySet();
        GlobalIndexFileReader input =
                meta -> {
                    opened.add(meta.filePath());
                    return io.newInputStream(meta.filePath());
                };
        // No dependency on a spare executor thread to open a shard or complete a planned search.
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            int k = 10;
            Map<String, String> fixed = new HashMap<>();
            fixed.put("centroid.nprobe", "4");
            fixed.put("ivf.nprobe", "16");
            VectorSearch near = new VectorSearch(landmarks[59], k, "v");
            VectorSearch far = new VectorSearch(landmarks[63], k, "v");
            ScoredGlobalIndexResult joint;
            try (GlobalIndexReader reader =
                    indexer.createReader(input, metas, 32768, null, executor)) {
                joint = reader.visitVectorSearch(near).get(30, TimeUnit.SECONDS).get();
                assertThat(opened).hasSize(5);
                assertSameScores(
                        joint,
                        reader.visitVectorSearch(new VectorSearch(landmarks[59], k, "v", fixed))
                                .get()
                                .get(),
                        k);
            }
            opened.clear();
            try (GlobalIndexReader reader =
                    indexer.createReader(input, metas, 32768, null, executor)) {
                ScoredGlobalIndexResult independent =
                        reader.visitVectorSearch(far).get(30, TimeUnit.SECONDS).get();
                assertThat(opened).hasSize(9);
                List<Optional<ScoredGlobalIndexResult>> batch =
                        reader.visitBatchVectorSearch(
                                        new BatchVectorSearch(
                                                new float[][] {
                                                    landmarks[63], landmarks[59], landmarks[59]
                                                },
                                                k,
                                                "v"))
                                .get(30, TimeUnit.SECONDS);
                assertSameScores(independent, batch.get(0).get(), k);
                assertSameScores(joint, batch.get(1).get(), k);
                assertSameScores(joint, batch.get(2).get(), k);
            }
            RoaringNavigableMap64 include = new RoaringNavigableMap64();
            include.addRange(new Range(0, 32767));
            for (VectorSearch other :
                    Arrays.asList(
                            new VectorSearch(
                                    landmarks[59],
                                    k,
                                    "v",
                                    Collections.singletonMap("ivf.nprobe", "8")),
                            new VectorSearch(
                                    landmarks[59],
                                    k,
                                    "v",
                                    Collections.singletonMap("centroid.nprobe", "8")),
                            new VectorSearch(
                                    landmarks[59],
                                    k,
                                    "v",
                                    Collections.singletonMap(
                                            "ivf.max_initial_filter_expansion_factor", "2")),
                            new VectorSearch(landmarks[59], k, "v").withIncludeRowIds(include),
                            new VectorSearch(landmarks[59], 1, "v"),
                            new VectorSearch(landmarks[59], 101, "v"))) {
                opened.clear();
                try (GlobalIndexReader reader =
                        indexer.createReader(input, metas, 32768, null, executor)) {
                    assertThat(
                                    reader.visitVectorSearch(other)
                                            .get()
                                            .get()
                                            .results()
                                            .getLongCardinality())
                            .isEqualTo(other.limit());
                    assertThat(opened).hasSize(9);
                }
            }
        } finally {
            executor.shutdownNow();
        }
    }

    private static NativeCentroidVectorGlobalIndexer flatIndexer(int localLists) {
        Map<String, String> options = new HashMap<>();
        options.put("dimension", "8");
        options.put("index.type", "ivf_flat");
        options.put("metric", "l2");
        options.put("nlist", "64");
        options.put("centroid.local-nlist", String.valueOf(localLists));
        return new NativeCentroidVectorGlobalIndexer(
                new ArrayType(new FloatType()), options, "ivf-flat", 1.0);
    }

    private List<GlobalIndexIOMeta> buildMixedShards(
            FileIO io, NativeCentroidVectorGlobalIndexer indexer, float[][] landmarks)
            throws Exception {
        Path directory = new Path(temp.toString());
        GlobalIndexFileWriter files = fileWriter(io, directory);
        float[] sample = new float[64 * 8];
        for (int i = 0; i < 64; i++) {
            landmarks[i][0] = i * 100;
            sample[8 * i] = i * 100;
        }
        List<GlobalIndexIOMeta> metas = new ArrayList<>();
        String id = "joint-probes";
        try (VectorTrainingModel model =
                indexer.trainGlobalModel(
                        64, Collections.singletonList(new TrainingBatch(sample, 64)).iterator())) {
            ResultEntry routing =
                    indexer.writeRoutingModel(
                            files, "router", model.serialize(), id, model.nlist());
            metas.add(toIOMeta(io, directory, routing));
            NativeCentroidVectorGlobalIndexer smallIndexer = flatIndexer(4);
            Random random = new Random(42);
            for (int i = 0; i < landmarks.length; i++) {
                int centroid = model.assignCentroid(landmarks[i]);
                // Neighbours of the last landmark encounter one small shard.
                GlobalIndexSingleColumnWriter writer =
                        (i == 63 ? smallIndexer : indexer).createLocalWriter(files, centroid, id);
                try {
                    for (int j = 0; j < 512; j++) {
                        float[] row = landmarks[i].clone();
                        for (int dim = 0; dim < row.length; dim++) {
                            row[dim] += random.nextFloat() - 0.5f;
                        }
                        writer.write(row, 512L * i + j);
                    }
                    metas.add(toIOMeta(io, directory, writer.finish().get(0)));
                } finally {
                    ((Closeable) writer).close();
                }
            }
        }
        return metas;
    }

    private static void assertSameScores(
            ScoredGlobalIndexResult expected, ScoredGlobalIndexResult actual, int k) {
        assertThat(actual.results().toArray(k)).containsExactly(expected.results().toArray(k));
        for (long row : expected.results()) {
            assertThat(actual.scoreGetter().score(row))
                    .isEqualTo(expected.scoreGetter().score(row));
        }
    }

    private static GlobalIndexIOMeta toIOMeta(FileIO io, Path directory, ResultEntry entry)
            throws java.io.IOException {
        Path path = new Path(directory, entry.fileName());
        return new GlobalIndexIOMeta(path, io.getFileSize(path), entry.rowCount(), entry.meta());
    }

    private static GlobalIndexFileWriter fileWriter(FileIO io, Path directory) {
        return new GlobalIndexFileWriter() {
            @Override
            public String newFileName(String prefix) {
                return prefix + "-" + UUID.randomUUID() + ".index";
            }

            @Override
            public PositionOutputStream newOutputStream(String name) throws java.io.IOException {
                return io.newOutputStream(new Path(directory, name), false);
            }
        };
    }
}
