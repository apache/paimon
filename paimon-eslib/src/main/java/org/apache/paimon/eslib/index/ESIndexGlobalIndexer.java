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

package org.apache.paimon.eslib.index;

import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.GlobalIndexReader;
import org.apache.paimon.globalindex.GlobalIndexWriter;
import org.apache.paimon.globalindex.VectorGlobalIndexer;
import org.apache.paimon.globalindex.io.GlobalIndexFileReader;
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataField;

import org.elasticsearch.eslib.api.model.FieldIndexConfig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 * ES multi-index global indexer using ESLib. Builds Lucene-based indexes supporting vector,
 * fulltext, and scalar fields.
 */
public class ESIndexGlobalIndexer implements VectorGlobalIndexer {

    private final List<DataField> fields;
    private final ESIndexOptions indexOptions;
    private final String configuredVectorMetric;
    private volatile String readerVectorMetric;

    public ESIndexGlobalIndexer(List<DataField> fields, Options options) {
        this.fields = Collections.unmodifiableList(new ArrayList<>(fields));
        this.indexOptions = new ESIndexOptions(this.fields, options);
        this.configuredVectorMetric = primaryVectorMetric(this.fields, this.indexOptions);
    }

    @Override
    public GlobalIndexWriter createWriter(GlobalIndexFileWriter fileWriter) throws IOException {
        return new ESIndexGlobalIndexWriter(fileWriter, fields, indexOptions);
    }

    @Override
    public GlobalIndexReader createReader(
            GlobalIndexFileReader fileReader,
            List<GlobalIndexIOMeta> files,
            ExecutorService executor) {
        ESIndexGlobalIndexReader reader =
                new ESIndexGlobalIndexReader(fileReader, files, fields, indexOptions, executor);
        try {
            registerReaderVectorMetric(reader.primaryVectorMetric());
            return reader;
        } catch (RuntimeException e) {
            try {
                reader.close();
            } catch (IOException closeFailure) {
                e.addSuppressed(closeFailure);
            }
            throw e;
        }
    }

    @Override
    public String metric() {
        String metric = readerVectorMetric;
        return metric == null ? configuredVectorMetric : metric;
    }

    private synchronized void registerReaderVectorMetric(String metric) {
        if (metric == null) {
            return;
        }
        if (readerVectorMetric == null) {
            readerVectorMetric = metric;
        } else if (!readerVectorMetric.equals(metric)) {
            throw new IllegalArgumentException(
                    "Cannot combine es-index shards with different vector metrics: "
                            + readerVectorMetric
                            + " and "
                            + metric
                            + ".");
        }
    }

    private static String primaryVectorMetric(List<DataField> fields, ESIndexOptions indexOptions) {
        if (fields.isEmpty()) {
            return null;
        }
        FieldIndexConfig config = indexOptions.getConfig(fields.get(0).name());
        return config != null && config.indexType() == FieldIndexConfig.IndexType.VECTOR
                ? ESIndexOptions.toPaimonVectorMetric(config.metric())
                : null;
    }
}
