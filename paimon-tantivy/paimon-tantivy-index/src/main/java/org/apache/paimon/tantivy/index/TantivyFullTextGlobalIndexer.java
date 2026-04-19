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

package org.apache.paimon.tantivy.index;

import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.GlobalIndexReader;
import org.apache.paimon.globalindex.GlobalIndexWriter;
import org.apache.paimon.globalindex.GlobalIndexer;
import org.apache.paimon.globalindex.io.GlobalIndexFileReader;
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Tantivy full-text global indexer. */
public class TantivyFullTextGlobalIndexer implements GlobalIndexer {

    private final Map<String, ArchiveLayout> layoutCache = new ConcurrentHashMap<>();
    private final TantivySearcherPool searcherPool;

    public TantivyFullTextGlobalIndexer() {
        this(
                new TantivySearcherPool(
                        TantivyFullTextIndexOptions.SEARCHER_POOL_MAX_SIZE.defaultValue()));
    }

    public TantivyFullTextGlobalIndexer(TantivySearcherPool searcherPool) {
        this.searcherPool = searcherPool;
    }

    @Override
    public GlobalIndexWriter createWriter(GlobalIndexFileWriter fileWriter) {
        return new TantivyFullTextGlobalIndexWriter(fileWriter);
    }

    @Override
    public GlobalIndexReader createReader(
            GlobalIndexFileReader fileReader, List<GlobalIndexIOMeta> files) {
        return new TantivyFullTextGlobalIndexReader(fileReader, files, layoutCache, searcherPool);
    }
}
