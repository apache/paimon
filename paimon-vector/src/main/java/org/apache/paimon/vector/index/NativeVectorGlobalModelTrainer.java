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

import org.apache.paimon.globalindex.VectorGlobalModelTrainer;
import org.apache.paimon.globalindex.VectorTrainingModel;
import org.apache.paimon.types.DataType;

import java.io.IOException;
import java.util.Map;

/** Reuses bounded heap ingestion and deterministic disk-backed sampling from the range writer. */
final class NativeVectorGlobalModelTrainer implements VectorGlobalModelTrainer {
    private final NativeVectorGlobalIndexWriter samples;
    private final NativeVectorGlobalIndexer indexer;
    private boolean finished;

    NativeVectorGlobalModelTrainer(
            DataType fieldType,
            Map<String, String> options,
            double ratio,
            String identifier,
            NativeVectorGlobalIndexer indexer) {
        this.indexer = indexer;
        this.samples =
                new NativeVectorGlobalIndexWriter(null, fieldType, options, identifier, ratio);
    }

    @Override
    public void write(Object value, long absoluteRowId) {
        if (finished) {
            throw new IllegalStateException("Model trainer is finished.");
        }
        samples.write(value, absoluteRowId);
    }

    @Override
    public VectorTrainingModel finishTraining() throws IOException {
        if (finished) {
            throw new IllegalStateException("Model trainer is finished.");
        }
        finished = true;
        try {
            return indexer.wrap(samples.finishRoutingModel(indexer));
        } finally {
            samples.close();
        }
    }

    @Override
    public void close() {
        finished = true;
        samples.close();
    }
}
