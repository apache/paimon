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

package org.apache.paimon.lumina.index;

import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.GlobalIndexReader;
import org.apache.paimon.globalindex.GlobalIndexWriter;
import org.apache.paimon.globalindex.VectorGlobalIndexer;
import org.apache.paimon.globalindex.io.GlobalIndexFileReader;
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataType;
import org.apache.paimon.utils.Range;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;

/** Lumina vector global indexer. */
public class LuminaVectorGlobalIndexer implements VectorGlobalIndexer {

    private final DataType fieldType;
    private final LuminaVectorIndexOptions options;

    // Metric persisted in the index files read by this indexer, guarded by this.
    private volatile String persistedMetric;
    private boolean persistedMetricConflict;

    public LuminaVectorGlobalIndexer(DataType fieldType, Options options) {
        this.fieldType = fieldType;
        this.options = new LuminaVectorIndexOptions(options);
    }

    @Override
    public GlobalIndexWriter createWriter(GlobalIndexFileWriter fileWriter) {
        return new LuminaVectorGlobalIndexWriter(fileWriter, fieldType, options);
    }

    @Override
    public GlobalIndexReader createReader(
            GlobalIndexFileReader fileReader,
            List<GlobalIndexIOMeta> files,
            long totalRowCount,
            List<Range> rowRanges,
            ExecutorService executor) {
        LuminaVectorGlobalIndexReader reader =
                new LuminaVectorGlobalIndexReader(fileReader, files, fieldType, options, executor);
        recordPersistedMetric(files.get(0).metadata());
        return reader;
    }

    /**
     * Returns the metric persisted in the index files read so far, so that raw search and
     * refinement score rows in the same metric as the index. Falls back to the configured metric
     * before any reader is created.
     */
    @Override
    public String metric() {
        String metric = persistedMetric;
        return metric == null ? options.metric().getLuminaName() : metric;
    }

    private synchronized void recordPersistedMetric(byte[] metadata) {
        if (persistedMetricConflict) {
            return;
        }
        String metric;
        try {
            metric = LuminaIndexMeta.deserialize(metadata).metric().getLuminaName();
        } catch (IOException | IllegalArgumentException e) {
            // The reader reports invalid metadata when it loads the index.
            return;
        }
        if (persistedMetric == null) {
            persistedMetric = metric;
        } else if (!persistedMetric.equals(metric)) {
            // Shards built with different metrics have no common metric. Keep using the
            // configured metric, as before, instead of failing the search.
            persistedMetricConflict = true;
            persistedMetric = null;
        }
    }
}
