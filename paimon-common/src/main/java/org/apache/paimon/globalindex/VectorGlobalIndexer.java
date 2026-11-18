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

/** A {@link GlobalIndexer} that supports vector similarity search. */
public interface VectorGlobalIndexer extends GlobalIndexer {

    /** Returns the metric name used to convert vector distances to comparable scores. */
    String metric();

    /** Zero-based ordinals in the non-null input stream, in the order required for training. */
    default long[] planGlobalTrainingSample(long vectorCount) {
        throw new UnsupportedOperationException("Centroid training is not supported.");
    }

    /** Train using ordered, preselected batches and the full non-null corpus size. */
    default VectorTrainingModel trainGlobalModel(
            long vectorCount, Iterator<TrainingBatch> batches) {
        throw new UnsupportedOperationException("Centroid training is not supported.");
    }

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

    default VectorGlobalModelTrainer createGlobalModelTrainer() {
        throw new UnsupportedOperationException(
                "Centroid sharding is not supported by this vector index.");
    }

    default VectorTrainingModel restoreTrainingModel(byte[] payload) {
        throw new UnsupportedOperationException(
                "Centroid sharding is not supported by this vector index.");
    }
}
