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

import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.globalindex.CentroidIndexFile;
import org.apache.paimon.globalindex.CentroidVectorGlobalIndexer;
import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.GlobalIndexSingleColumnWriter;
import org.apache.paimon.globalindex.GlobalIndexWriter;
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.globalindex.VectorGlobalModelTrainer;
import org.apache.paimon.globalindex.VectorTrainingModel;
import org.apache.paimon.globalindex.io.GlobalIndexFileReader;
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;
import org.apache.paimon.index.vector.CentroidRouter;
import org.apache.paimon.index.vector.VectorIndexTrainer;
import org.apache.paimon.index.vector.VectorIndexTraining;
import org.apache.paimon.types.DataType;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Outer centroid routing with an independently trained native index in each populated shard. */
final class NativeCentroidVectorGlobalIndexer extends NativeVectorGlobalIndexer
        implements CentroidVectorGlobalIndexer {

    NativeCentroidVectorGlobalIndexer(
            DataType fieldType,
            Map<String, String> options,
            String identifier,
            double trainSampleRatio) {
        super(fieldType, options, identifier, trainSampleRatio);
    }

    @Override
    public GlobalIndexWriter createWriter(GlobalIndexFileWriter fileWriter) {
        throw new UnsupportedOperationException(
                "Centroid indexes require a routed, distributed build.");
    }

    @Override
    public ResultEntry writeRoutingModel(
            GlobalIndexFileWriter io, String name, byte[] payload, String modelId)
            throws IOException {
        try (OutputStream out = io.newOutputStream(name)) {
            VectorTrainingModelIO.write(out, payload, modelId);
        }
        return new ResultEntry(name, 0, new VectorIndexMeta(null, metric().trim()).serialize());
    }

    static String storedMetric(GlobalIndexFileReader reader, List<GlobalIndexIOMeta> files) {
        for (GlobalIndexIOMeta file : files) {
            CentroidIndexFile meta = (CentroidIndexFile) file;
            if (meta.routingModel()) {
                String metric = VectorIndexMeta.read(file.metadata()).metric();
                if (metric != null) {
                    return metric;
                }
                try (SeekableInputStream in = reader.getInputStream(file)) {
                    return VectorTrainingModelIO.readLegacyMetric(
                            in,
                            file.fileSize(),
                            meta.modelId(),
                            CentroidRouter.maxSerializedBytes());
                } catch (IOException e) {
                    throw new UncheckedIOException("Cannot read centroid routing metric.", e);
                }
            }
        }
        throw new IllegalArgumentException("Missing centroid routing model.");
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
        coarse.put("nlist", options.getOrDefault("centroid.nlist", "auto"));
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

    @Override
    public GlobalIndexSingleColumnWriter createCentroidWriter(GlobalIndexFileWriter fileWriter) {
        Map<String, String> local = new LinkedHashMap<>(options);
        if ("diskann".equals(identifier)) {
            local.remove("nlist");
            local.remove("ivf.train.max-points-per-centroid");
        }
        local.remove("centroid.nlist");
        local.remove("expected-vector-count");
        return new NativeVectorGlobalIndexWriter(
                fileWriter, fieldType, local, identifier, trainSampleRatio, true);
    }

    @Override
    public VectorTrainingModel trainGlobalModel(long vectorCount, Iterator<TrainingBatch> batches) {
        try (VectorIndexTrainer trainer = createCoarseTrainer(vectorCount)) {
            while (batches.hasNext()) {
                TrainingBatch batch = batches.next();
                trainer.addTrainingVectors(batch.vectors, batch.count);
            }
            try (VectorIndexTraining coarse = trainer.finishTraining()) {
                return new NativeVectorTrainingModel(CentroidRouter.fromTraining(coarse));
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
        return new NativeVectorTrainingModel(CentroidRouter.deserialize(payload));
    }
}
