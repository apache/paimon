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
import org.apache.paimon.globalindex.io.GlobalIndexFileReader;
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;
import org.apache.paimon.types.DataType;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;

/** Native vector global indexer backed by paimon-vector-index. */
public class NativeVectorGlobalIndexer implements org.apache.paimon.globalindex.VectorGlobalIndexer {

    private final DataType fieldType;
    private final Map<String, String> options;
    private final String identifier;

    public NativeVectorGlobalIndexer(
            DataType fieldType, Map<String, String> options, String identifier) {
        this.fieldType = fieldType;
        this.options = Objects.requireNonNull(options, "options must not be null");
        this.identifier = Objects.requireNonNull(identifier, "identifier must not be null");
    }

    @Override
    public GlobalIndexWriter createWriter(GlobalIndexFileWriter fileWriter) {
        return new VectorGlobalIndexWriter(fileWriter, fieldType, options, identifier);
    }

    @Override
    public GlobalIndexReader createReader(
            GlobalIndexFileReader fileReader,
            List<GlobalIndexIOMeta> files,
            ExecutorService executor) {
        return new VectorGlobalIndexReader(fileReader, files, fieldType, executor);
    }

    @Override
    public String metric() {
        return options.getOrDefault("metric", "l2");
    }
}
