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

package org.apache.paimon.elasticsearch.index;

import org.apache.paimon.elasticsearch.index.model.ESVectorIndexOptions;
import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.GlobalIndexReader;
import org.apache.paimon.globalindex.GlobalIndexWriter;
import org.apache.paimon.globalindex.GlobalIndexer;
import org.apache.paimon.globalindex.io.GlobalIndexFileReader;
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataType;

import java.io.IOException;
import java.util.List;

/** ES (DiskBBQ) vector global indexer. */
public class ESVectorGlobalIndexer implements GlobalIndexer {

    private final String fieldName;
    private final DataType fieldType;
    private final ESVectorIndexOptions options;

    public ESVectorGlobalIndexer(String fieldName, DataType fieldType, Options options) {
        this.fieldName = fieldName;
        this.fieldType = fieldType;
        this.options = new ESVectorIndexOptions(options);
    }

    @Override
    public GlobalIndexWriter createWriter(GlobalIndexFileWriter fileWriter) {
        try {
            return new ESVectorGlobalIndexWriter(fileWriter, fieldName, fieldType, options);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create ES vector index writer", e);
        }
    }

    @Override
    public GlobalIndexReader createReader(
            GlobalIndexFileReader fileReader, List<GlobalIndexIOMeta> files) {
        return new ESVectorGlobalIndexReader(fileReader, files, fieldType, options);
    }
}
