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
import org.apache.paimon.globalindex.GlobalIndexer;
import org.apache.paimon.globalindex.io.GlobalIndexFileReader;
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataField;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 * ES multi-index global indexer using ESLib. Builds Lucene-based indexes supporting vector,
 * fulltext, and scalar fields.
 */
public class ESIndexGlobalIndexer implements GlobalIndexer {

    private final List<DataField> fields;
    private final ESIndexOptions indexOptions;
    private final ExecutorService searchExecutor;

    public ESIndexGlobalIndexer(List<DataField> fields, Options options) {
        this(fields, options, null);
    }

    public ESIndexGlobalIndexer(
            List<DataField> fields, Options options, ExecutorService searchExecutor) {
        this.fields = fields;
        this.indexOptions = new ESIndexOptions(fields, options);
        this.searchExecutor = searchExecutor;
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
        return new ESIndexGlobalIndexReader(fileReader, files, fields, indexOptions, executor);
    }
}
