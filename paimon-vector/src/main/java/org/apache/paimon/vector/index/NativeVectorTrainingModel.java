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

import org.apache.paimon.globalindex.GlobalIndexSingleColumnWriter;
import org.apache.paimon.globalindex.VectorTrainingModel;
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;
import org.apache.paimon.index.vector.CentroidRouter;

/** JNI-backed portable partition model and independent centroid writers. */
final class NativeVectorTrainingModel implements VectorTrainingModel {
    private final CentroidRouter training;

    private final NativeVectorGlobalIndexer localIndexer;

    NativeVectorTrainingModel(CentroidRouter training, NativeVectorGlobalIndexer localIndexer) {
        this.localIndexer = localIndexer;
        this.training = training;
    }

    @Override
    public int nlist() {
        return training.nlist();
    }

    @Override
    public int assignCentroid(float[] vector) {
        return training.assignCentroid(vector);
    }

    @Override
    public int[] nearestCentroids(float[] vector, int nprobe) {
        return training.findNearestCentroids(vector, nprobe);
    }

    @Override
    public byte[] serialize() {
        return training.serialize();
    }

    @Override
    public void close() {
        training.close();
    }

    @Override
    public GlobalIndexSingleColumnWriter createCentroidWriter(
            GlobalIndexFileWriter fileWriter, int centroid, String modelId) {
        if (centroid < 0 || centroid >= nlist()) {
            throw new IllegalArgumentException("Invalid routing centroid.");
        }
        return localIndexer.createLocalWriter(fileWriter, centroid, modelId);
    }
}
