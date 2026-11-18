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

import org.apache.paimon.globalindex.CentroidVectorGlobalIndexer;
import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.GlobalIndexReader;
import org.apache.paimon.globalindex.GlobalIndexSingleColumnWriter;
import org.apache.paimon.globalindex.GlobalIndexWriter;
import org.apache.paimon.globalindex.VectorGlobalModelTrainer;
import org.apache.paimon.globalindex.VectorIndexShardMeta;
import org.apache.paimon.globalindex.VectorTrainingModel;
import org.apache.paimon.globalindex.io.GlobalIndexFileReader;
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;
import org.apache.paimon.index.vector.CentroidRouter;
import org.apache.paimon.index.vector.VectorIndexTrainer;
import org.apache.paimon.index.vector.VectorIndexTraining;
import org.apache.paimon.types.DataType;
import org.apache.paimon.utils.Range;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;

/** Native vector global indexer backed by paimon-vector-index-java. */
public class NativeVectorGlobalIndexer implements CentroidVectorGlobalIndexer {

    static final String DEFAULT_METRIC = "inner_product";

    private final DataType fieldType;
    private final Map<String, String> options;
    private final String identifier;
    private final String localNlist;
    private final double trainSampleRatio;

    public NativeVectorGlobalIndexer(
            DataType fieldType, Map<String, String> options, String identifier) {
        this(
                fieldType,
                options,
                identifier,
                NativeVectorGlobalIndexerFactory.DEFAULT_TRAIN_SAMPLE_RATIO);
    }

    public NativeVectorGlobalIndexer(
            DataType fieldType,
            Map<String, String> options,
            String identifier,
            double trainSampleRatio) {
        this.fieldType = fieldType;
        this.options = Objects.requireNonNull(options, "options must not be null");
        this.identifier = Objects.requireNonNull(identifier, "identifier must not be null");
        this.localNlist = this.options.get("centroid.local-nlist");
        if (localNlist != null
                && ("diskann".equals(identifier)
                        || (!"auto".equals(localNlist) && Integer.parseInt(localNlist) <= 0))) {
            throw new IllegalArgumentException(
                    "centroid.local-nlist requires local IVF and a positive count or auto.");
        }
        if (Double.isNaN(trainSampleRatio)
                || Double.isInfinite(trainSampleRatio)
                || trainSampleRatio <= 0
                || trainSampleRatio > 1) {
            throw new IllegalArgumentException(
                    "trainSampleRatio must be greater than 0 and less than or equal to 1: "
                            + trainSampleRatio);
        }
        this.trainSampleRatio = trainSampleRatio;
    }

    @Override
    public GlobalIndexWriter createWriter(GlobalIndexFileWriter fileWriter) {
        return new NativeVectorGlobalIndexWriter(
                fileWriter, fieldType, options, identifier, trainSampleRatio);
    }

    @Override
    public GlobalIndexReader createReader(
            GlobalIndexFileReader fileReader,
            List<GlobalIndexIOMeta> files,
            long totalRowCount,
            List<Range> rowRanges,
            ExecutorService executor) {
        if (files.stream().anyMatch(file -> VectorIndexShardMeta.isWrapped(file.metadata()))) {
            return new NativeCentroidRoutedIndexReader(
                    fileReader, files, fieldType, executor, identifier);
        }
        return new NativeVectorGlobalIndexReader(
                fileReader, files, fieldType, executor, identifier);
    }

    @Override
    public long[] planGlobalTrainingSample(long vectorCount) {
        long candidates =
                NativeVectorGlobalIndexWriter.trainingCandidateCount(vectorCount, trainSampleRatio);
        if (candidates == 0) {
            throw new IllegalArgumentException(
                    "Cannot train a centroid model without non-null vectors.");
        }
        try (VectorIndexTrainer trainer = createCoarseTrainer(vectorCount)) {
            long[] positions = trainer.planTrainingSample(candidates);
            for (int i = 0; i < positions.length; i++) {
                positions[i] =
                        NativeVectorGlobalIndexWriter.sampleIndex(
                                positions[i], vectorCount, candidates);
            }
            return positions;
        }
    }

    VectorIndexTrainer createCoarseTrainer(long vectorCount) {
        Map<String, String> coarse = new LinkedHashMap<>();
        coarse.put("index.type", "ivf_flat");
        coarse.put("dimension", options.get("dimension"));
        coarse.put("metric", options.get("metric"));
        coarse.put("ivf.coarse-assignment", "exact");
        String nlist = options.getOrDefault("nlist", "auto");
        if ("diskann".equals(identifier) && "auto".equals(nlist.trim())) {
            nlist = "64";
        }
        coarse.put("nlist", nlist);
        coarse.put(
                "expected-vector-count",
                options.getOrDefault("expected-vector-count", Long.toString(vectorCount)));
        if (options.containsKey("ivf.train.max-points-per-centroid")) {
            coarse.put(
                    "ivf.train.max-points-per-centroid",
                    options.get("ivf.train.max-points-per-centroid"));
        }
        return VectorIndexTrainer.create(coarse);
    }

    GlobalIndexSingleColumnWriter createLocalWriter(
            GlobalIndexFileWriter fileWriter, int centroid, String modelId) {
        Map<String, String> local = new LinkedHashMap<>(options);
        if ("diskann".equals(identifier)) {
            local.remove("nlist");
        } else {
            local.put("nlist", localNlist == null ? "auto" : localNlist);
        }
        local.remove("expected-vector-count");
        return new NativeVectorGlobalIndexWriter(
                fileWriter,
                fieldType,
                local,
                identifier,
                trainSampleRatio,
                VectorIndexShardMeta.centroid(modelId, centroid));
    }

    @Override
    public VectorTrainingModel trainGlobalModel(long vectorCount, Iterator<TrainingBatch> batches) {
        try (VectorIndexTrainer trainer = createCoarseTrainer(vectorCount)) {
            while (batches.hasNext()) {
                TrainingBatch batch = batches.next();
                trainer.addTrainingVectors(batch.vectors, batch.count);
            }
            try (VectorIndexTraining coarse = trainer.finishTraining()) {
                return wrap(CentroidRouter.fromTraining(coarse));
            }
        }
    }

    @Override
    public VectorGlobalModelTrainer createGlobalModelTrainer() {
        return new NativeVectorGlobalModelTrainer(
                fieldType, options, trainSampleRatio, identifier, this);
    }

    @Override
    public VectorTrainingModel restoreTrainingModel(byte[] payload) {
        return wrap(CentroidRouter.deserialize(payload));
    }

    NativeVectorTrainingModel wrap(CentroidRouter model) {
        return new NativeVectorTrainingModel(model, this);
    }

    @Override
    public String metric() {
        return options.getOrDefault("metric", DEFAULT_METRIC);
    }
}
