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

import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.GlobalIndexReader;
import org.apache.paimon.globalindex.GlobalIndexWriter;
import org.apache.paimon.globalindex.GlobalIndexer;
import org.apache.paimon.globalindex.io.GlobalIndexFileReader;
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;
import org.apache.paimon.index.ivfpq.IndexType;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataType;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;

/** Vector global indexer backed by paimon-vector-index. */
public class VectorGlobalIndexer implements GlobalIndexer {

    private final DataType fieldType;
    private final Options options;
    private final IndexType indexType;
    private final String identifier;

    public VectorGlobalIndexer(
            DataType fieldType, Options options, IndexType indexType, String identifier) {
        this.fieldType = fieldType;
        this.options = options;
        this.indexType = Objects.requireNonNull(indexType, "indexType must not be null");
        this.identifier = Objects.requireNonNull(identifier, "identifier must not be null");
    }

    @Override
    public GlobalIndexWriter createWriter(GlobalIndexFileWriter fileWriter) {
        return new VectorGlobalIndexWriter(fileWriter, fieldType, options, indexType, identifier);
    }

    @Override
    public GlobalIndexReader createReader(
            GlobalIndexFileReader fileReader,
            List<GlobalIndexIOMeta> files,
            ExecutorService executor) {
        return new VectorGlobalIndexReader(fileReader, files, fieldType, executor);
    }
}
