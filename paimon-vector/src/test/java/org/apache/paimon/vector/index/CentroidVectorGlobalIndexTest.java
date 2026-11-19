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
import org.apache.paimon.fs.SeekableInputStreamWrapper;
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
import org.apache.paimon.utils.SemaphoredDelegatingExecutor;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
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
        coarse.put("nlist", "auto");
        coarse.put("expected-vector-count", "4000000");
        long[] expected;
        try (VectorIndexTrainer trainer = VectorIndexTrainer.create(coarse)) {
            expected = trainer.planTrainingSample(4000000);
        }
        assertThat(expected).hasSize(131072);
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
                assertThat(indexer.planGlobalTrainingSample(4000000)).isEqualTo(expected);
                try (VectorTrainingModel model =
                        indexer.trainGlobalModel(
                                4000000,
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
        if (diskann) {
            options.put("ivf.train.max-points-per-centroid", "32");
        } else {
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
                                VectorIndexShardMeta.routing(id, model.nlist(), "l2").serialize()));
                GlobalIndexSingleColumnWriter empty = model.createCentroidWriter(files, 0, id);
                try {
                    empty.write(null, 0);
                    assertThat(empty).extracting("tempVectorFile", "writeBuf").containsOnlyNulls();
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
        // A router written before the manifest included the metric can recover it from a shard.
        GlobalIndexIOMeta router = metas.get(0);
        List<GlobalIndexIOMeta> legacy = new ArrayList<>(metas);
        legacy.set(
                0,
                new GlobalIndexIOMeta(
                        router.filePath(),
                        router.fileSize(),
                        0,
                        VectorIndexShardMeta.routing("test-model", 4, null).serialize()));
        Map<String, String> otherMetric = new HashMap<>(options);
        otherMetric.put("metric", "cosine");
        NativeCentroidVectorGlobalIndexer fromTable =
                new NativeCentroidVectorGlobalIndexer(
                        new ArrayType(new FloatType()), otherMetric, type, 1);
        assertThat(fromTable.forRead(file -> io.newInputStream(file.filePath()), legacy).metric())
                .isEqualTo("l2");
        ExecutorService executor =
                new SemaphoredDelegatingExecutor(Executors.newSingleThreadExecutor(), 1, false);
        CountDownLatch releaseRouter = new CountDownLatch(1);
        AtomicInteger liveStreams = new AtomicInteger();
        AtomicInteger peakStreams = new AtomicInteger();
        Map<Path, AtomicInteger> opened = new ConcurrentHashMap<>();
        GlobalIndexFileReader fileReader =
                meta -> {
                    opened.computeIfAbsent(meta.filePath(), p -> new AtomicInteger())
                            .incrementAndGet();
                    if (meta.filePath().equals(router.filePath())) {
                        try {
                            if (!releaseRouter.await(30, TimeUnit.SECONDS)) {
                                throw new IOException(
                                        "Timed out waiting for the query to submit its continuations.");
                            }
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new IOException(e);
                        }
                    }
                    SeekableInputStream in = io.newInputStream(meta.filePath());
                    peakStreams.accumulateAndGet(liveStreams.incrementAndGet(), Math::max);
                    return new SeekableInputStreamWrapper(in) {
                        @Override
                        public void close() throws IOException {
                            try {
                                super.close();
                            } finally {
                                liveStreams.decrementAndGet();
                            }
                        }
                    };
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
            CompletableFuture<Optional<ScoredGlobalIndexResult>> pending =
                    reader.visitVectorSearch(new VectorSearch(vectors[0], 1, "v", search));
            releaseRouter.countDown();
            ScoredGlobalIndexResult one = pending.get(30, TimeUnit.SECONDS).get();
            assertThat(liveStreams.get()).isZero();
            assertThat(one.results().contains(BASE)).isTrue();
            assertThat(opened).hasSize(2); // routing model and the selected centroid only
            for (GlobalIndexIOMeta file : metas.subList(1, metas.size())) {
                boolean selected =
                        VectorIndexShardMeta.deserialize(file.metadata()).centroid()
                                == centroidByRow[0];
                assertThat(opened.containsKey(file.filePath())).isEqualTo(selected);
            }
            opened.clear();
            float[][] queries = {vectors[0], vectors[200], vectors[0]};
            List<Optional<ScoredGlobalIndexResult>> batch =
                    reader.visitBatchVectorSearch(new BatchVectorSearch(queries, 1, "v", search))
                            .get(30, TimeUnit.SECONDS);
            assertThat(liveStreams.get()).isZero();
            assertThat(peakStreams.get()).isOne();
            assertThat(batch).hasSize(3);
            assertThat(batch.get(0).get().results().contains(BASE)).isTrue();
            assertThat(batch.get(1).get().results().contains(BASE + 101L * 200)).isTrue();
            assertThat(batch.get(2).get().results().contains(BASE)).isTrue();
            assertThat(opened.values()).allSatisfy(count -> assertThat(count.get()).isEqualTo(1));

            // The first batch may already have opened shards near either query. Count fresh opens.
            try (GlobalIndexReader fresh =
                    indexer.createReader(fileReader, metas, rowCount, null, executor)) {
                opened.clear();
                assertThat(fresh.visitVectorSearch(new VectorSearch(vectors[0], 1, "v")).get())
                        .isPresent();
                assertThat(opened).hasSize(4); // router and three of four shards
                VectorSearch all =
                        new VectorSearch(
                                vectors[0],
                                1,
                                "v",
                                Collections.singletonMap("centroid.nprobe", "100"));
                assertThat(fresh.visitVectorSearch(all).get()).isPresent();
                assertThat(opened).hasSize(5);
                VectorSearch invalid =
                        new VectorSearch(
                                vectors[0],
                                1,
                                "v",
                                Collections.singletonMap("centroid.nprobe", "0"));
                assertThatThrownBy(() -> fresh.visitVectorSearch(invalid).get())
                        .hasRootCauseInstanceOf(IllegalArgumentException.class);
            }

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

    @ParameterizedTest
    @ValueSource(strings = {"l2", "inner_product", "cosine"})
    void legacyMetricMatchesTheNativeRouter(String metric) throws IOException {
        Map<String, String> options = new HashMap<>();
        options.put("dimension", "2");
        options.put("index.type", "ivf_flat");
        options.put("nlist", "1");
        options.put("metric", metric);
        try (VectorIndexTraining training =
                        VectorIndexTrainer.train(options, new float[] {1f, 1f}, 1);
                CentroidRouter router = CentroidRouter.fromTraining(training)) {
            assertThat(VectorTrainingModelIO.legacyMetric(router.serialize())).isEqualTo(metric);
        }
    }

    @ParameterizedTest
    @CsvSource({
        // nlist, indexed vectors in the range, top-K, eligible row IDs (-1 for no filter), probes
        "3, 25000, 100, -1, 3",
        "4, 25000, 100, -1, 3",
        "5, 25000, 100, -1, 5",
        "7, 25000, 100, -1, 7",
        "8, 25000, 100, -1, 4",
        "9, 25000, 100, -1, 8",
        "64, 1000000, 100, -1, 8",
        "129, 1000000, 100, -1, 9",
        "8, 25000, 5000, -1, 7",
        "8, 25000, 2147483647, -1, 8",
        "8, 25000, 100, 20000, 5",
        "8, 25000, 100, 12500, 8",
        "8, 25000, 100, 0, 1",
        "4, 25000, 100, 26000, 3",
        "8, 9223372036854775807, 100, 9223372036854775806, 5"
    })
    void sizesInitialOuterProbes(int nlist, long rows, int topK, long matching, int probes) {
        assertThat(NativeCentroidRoutedIndexReader.automaticWidth(nlist, rows, topK, matching))
                .isEqualTo(probes);
    }

    @ParameterizedTest
    @CsvSource({
        "ivf-flat,current",
        "ivf-pq,current",
        "ivf-flat,legacy",
        "ivf-pq,legacy",
        "ivf-pq,mixed"
    })
    void joinProbesUsingTheNearestShardLayoutsForSingleAndBatchSearch(
            String type, String descriptors) throws Exception {
        FileIO io = new LocalFileIO();
        NativeCentroidVectorGlobalIndexer indexer = jointIndexer(type, 64);
        float[][] landmarks = new float[64][8];
        List<GlobalIndexIOMeta> metas = buildMixedShards(io, indexer, landmarks, type);
        if (!"current".equals(descriptors)) {
            for (int i = 0; i < metas.size(); i++) {
                GlobalIndexIOMeta file = metas.get(i);
                VectorIndexShardMeta meta = VectorIndexShardMeta.deserialize(file.metadata());
                if ("mixed".equals(descriptors) && i % 2 == 0) {
                    continue;
                }
                metas.set(
                        i,
                        new GlobalIndexIOMeta(
                                file.filePath(),
                                file.fileSize(),
                                file.rowCount(),
                                meta.routingModel()
                                        ? VectorIndexShardMeta.routing(
                                                        meta.modelId(), meta.nlist(), null)
                                                .serialize()
                                        : VectorIndexShardMeta.centroid(
                                                        meta.modelId(), meta.centroid())
                                                .wrap(meta.payloadMeta())));
            }
        }
        Map<Path, Integer> opened = new ConcurrentHashMap<>();
        AtomicInteger active = new AtomicInteger();
        GlobalIndexFileReader input =
                meta -> {
                    opened.merge(meta.filePath(), 1, Integer::sum);
                    assertThat(active.incrementAndGet())
                            .as("open streams with one I/O permit")
                            .isOne();
                    return new SeekableInputStreamWrapper(io.newInputStream(meta.filePath())) {
                        @Override
                        public void close() throws IOException {
                            try {
                                super.close();
                            } finally {
                                active.decrementAndGet();
                            }
                        }
                    };
                };
        indexer = indexer.forRead(input, metas);
        if ("legacy".equals(descriptors)) {
            assertThat(opened).containsOnlyKeys(metas.get(0).filePath());
        } else {
            assertThat(opened).isEmpty();
        }
        opened.clear();
        // No dependency on a spare executor thread to open a shard or complete a planned search.
        java.util.concurrent.ThreadPoolExecutor worker =
                (java.util.concurrent.ThreadPoolExecutor) Executors.newFixedThreadPool(1);
        ExecutorService executor = new SemaphoredDelegatingExecutor(worker, 1, false);
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
                assertThat(opened)
                        .hasSize(5)
                        .allSatisfy((file, count) -> assertThat(count).isOne());
                assertThat(active.get()).isZero();
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
                assertThat(opened)
                        .hasSize(9)
                        .allSatisfy((file, count) -> assertThat(count).isOne());
                assertSameScores(
                        independent,
                        reader.visitVectorSearch(
                                        new VectorSearch(
                                                landmarks[63],
                                                k,
                                                "v",
                                                Collections.singletonMap("centroid.nprobe", "8")))
                                .get()
                                .get(),
                        k);
                opened.clear();
                List<Optional<ScoredGlobalIndexResult>> batch =
                        reader.visitBatchVectorSearch(
                                        new BatchVectorSearch(
                                                new float[][] {
                                                    landmarks[63], landmarks[59], landmarks[59]
                                                },
                                                k,
                                                "v"))
                                .get(30, TimeUnit.SECONDS);
                assertThat(opened)
                        .isNotEmpty()
                        .allSatisfy((file, count) -> assertThat(count).isOne());
                assertThat(active.get()).isZero();
                assertSameScores(independent, batch.get(0).get(), k);
                assertSameScores(joint, batch.get(1).get(), k);
                assertSameScores(joint, batch.get(2).get(), k);
            }
            RoaringNavigableMap64 single = new RoaringNavigableMap64();
            single.add(0);
            try (GlobalIndexReader reader =
                    indexer.createReader(input, metas, 32768, null, executor)) {
                long before = worker.getTaskCount();
                List<Optional<ScoredGlobalIndexResult>> hits =
                        reader.visitBatchVectorSearch(
                                        new BatchVectorSearch(
                                                        new float[][] {
                                                            landmarks[0],
                                                            landmarks[1],
                                                            landmarks[2],
                                                            landmarks[3]
                                                        },
                                                        k,
                                                        "v")
                                                .withIncludeRowIds(single))
                                .get(30, TimeUnit.SECONDS);
                assertThat(hits)
                        .allSatisfy(
                                hit ->
                                        assertThat(hit.get().results().toArray(k))
                                                .containsExactly(0L));
                // One routing task plus a single batch task for each of the 64 shards.
                assertThat(worker.getTaskCount() - before).isEqualTo(65);
            }
            // Qualifying row IDs can include null vectors. With this dense filter, automatic
            // routing starts at eight shards and widens until it finds the only indexed match.
            single.addRange(new Range(32768, 65535));
            try (GlobalIndexReader reader =
                    indexer.createReader(input, metas, 65536, null, executor)) {
                long before = worker.getTaskCount();
                List<Optional<ScoredGlobalIndexResult>> hits =
                        reader.visitBatchVectorSearch(
                                        new BatchVectorSearch(
                                                        new float[][] {
                                                            landmarks[63], landmarks[63]
                                                        },
                                                        1,
                                                        "v")
                                                .withIncludeRowIds(single))
                                .get(30, TimeUnit.SECONDS);
                assertThat(hits)
                        .allSatisfy(
                                hit ->
                                        assertThat(hit.get().results().toArray(1))
                                                .containsExactly(0L));
                assertThat(worker.getTaskCount() - before).isEqualTo(65);
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

    private static NativeCentroidVectorGlobalIndexer jointIndexer(String type, int localLists) {
        Map<String, String> options = new HashMap<>();
        options.put("dimension", "8");
        options.put("index.type", type.replace('-', '_'));
        if ("ivf-pq".equals(type)) {
            options.put("pq.m", "2");
        }
        options.put("metric", "l2");
        options.put("nlist", "64");
        options.put("centroid.local-nlist", localLists == 0 ? "auto" : String.valueOf(localLists));
        return new NativeCentroidVectorGlobalIndexer(
                new ArrayType(new FloatType()), options, type, 1.0);
    }

    private List<GlobalIndexIOMeta> buildMixedShards(
            FileIO io, NativeCentroidVectorGlobalIndexer indexer, float[][] landmarks, String type)
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
            NativeCentroidVectorGlobalIndexer smallIndexer = jointIndexer(type, 0);
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
                    GlobalIndexIOMeta shard = toIOMeta(io, directory, writer.finish().get(0));
                    VectorIndexShardMeta meta = VectorIndexShardMeta.deserialize(shard.metadata());
                    assertThat(meta.localNlist()).isEqualTo(i == 63 ? 8 : 64);
                    assertThat(meta.metric()).isEqualTo("l2");
                    metas.add(shard);
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
