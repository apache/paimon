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
import org.apache.paimon.utils.Range;

import javax.annotation.Nullable;

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
    private final Options options;
    @Nullable private final ESIndexOptions indexOptions;
    private final String configuredVectorMetric;
    private volatile String readerVectorMetric;

    public ESIndexGlobalIndexer(List<DataField> fields, Options options) {
        this.fields = Collections.unmodifiableList(new ArrayList<>(fields));
        this.options = options;
        this.indexOptions = tryParseIndexOptions(this.fields, options);
        this.configuredVectorMetric =
                this.fields.isEmpty()
                        ? null
                        : ESIndexOptions.configuredVectorMetric(this.fields.get(0), options);
    }

    /**
     * Readers use the field configuration persisted in the index metadata, so the current options
     * only have to describe a build when a writer is created. For example, the dimension of an
     * ARRAY&lt;FLOAT&gt; column is usually passed to the build procedure only and is absent from
     * the table options that the read path passes here.
     */
    @Nullable
    private static ESIndexOptions tryParseIndexOptions(List<DataField> fields, Options options) {
        try {
            return new ESIndexOptions(fields, options);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    @Override
    public GlobalIndexWriter createWriter(GlobalIndexFileWriter fileWriter) throws IOException {
        // Re-parse so that options which cannot describe a build fail here with the original
        // message instead of being silently ignored.
        ESIndexOptions writerOptions =
                indexOptions != null ? indexOptions : new ESIndexOptions(fields, options);
        return new ESIndexGlobalIndexWriter(fileWriter, fields, writerOptions);
    }

    @Override
    public GlobalIndexReader createReader(
            GlobalIndexFileReader fileReader,
            List<GlobalIndexIOMeta> files,
            long totalRowCount,
            List<Range> rowRanges,
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
}
