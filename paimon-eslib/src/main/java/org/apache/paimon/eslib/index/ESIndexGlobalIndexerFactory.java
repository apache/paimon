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

import org.apache.paimon.globalindex.GlobalIndexer;
import org.apache.paimon.globalindex.GlobalIndexerFactory;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataField;

import java.util.List;

/**
 * Factory for creating ES multi-index global indexers. Supports vector (DiskBBQ/HNSW), fulltext
 * (BM25), and scalar fields.
 */
public class ESIndexGlobalIndexerFactory implements GlobalIndexerFactory {

    public static final String IDENTIFIER = "es-index";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public boolean supportsFullTextSearch() {
        return true;
    }

    @Override
    public GlobalIndexer create(DataField field, Options options) {
        return new ESIndexGlobalIndexer(java.util.Collections.singletonList(field), options);
    }

    @Override
    public GlobalIndexer create(
            DataField indexField, List<DataField> extraFields, Options options) {
        List<DataField> fields;
        if (extraFields == null || extraFields.isEmpty()) {
            fields = java.util.Collections.singletonList(indexField);
        } else {
            fields = new java.util.ArrayList<>(extraFields.size() + 1);
            fields.add(indexField);
            fields.addAll(extraFields);
        }
        return new ESIndexGlobalIndexer(fields, options);
    }
}
