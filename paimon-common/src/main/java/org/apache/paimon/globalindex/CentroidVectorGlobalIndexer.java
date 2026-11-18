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

import java.io.Serializable;
import java.util.Iterator;

/** A vector indexer that can train and restore a router for independently built shards. */
public interface CentroidVectorGlobalIndexer extends VectorGlobalIndexer {

    /** Return non-null input ordinals in the order required for training. */
    long[] planGlobalTrainingSample(long vectorCount);

    /** Train on ordered, preselected batches using the full non-null corpus size. */
    VectorTrainingModel trainGlobalModel(long vectorCount, Iterator<TrainingBatch> batches);

    VectorGlobalModelTrainer createGlobalModelTrainer();

    VectorTrainingModel restoreTrainingModel(byte[] payload);

    /** A row-major batch selected by the distributed sampler. */
    final class TrainingBatch implements Serializable {
        private static final long serialVersionUID = 1L;
        public final float[] vectors;
        public final int count;

        public TrainingBatch(float[] vectors, int count) {
            this.vectors = vectors;
            this.count = count;
        }
    }
}
