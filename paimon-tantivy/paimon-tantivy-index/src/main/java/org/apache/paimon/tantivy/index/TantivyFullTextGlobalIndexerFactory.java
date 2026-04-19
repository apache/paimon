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

import org.apache.paimon.globalindex.GlobalIndexer;
import org.apache.paimon.globalindex.GlobalIndexerFactory;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataField;

/** Factory for creating Tantivy full-text index. */
public class TantivyFullTextGlobalIndexerFactory implements GlobalIndexerFactory {

    public static final String IDENTIFIER = "tantivy-fulltext";

    /**
     * Shared pool across all indexers created by this factory. This factory instance is a JVM-level
     * singleton (loaded once via {@link java.util.ServiceLoader}), so the pool naturally survives
     * across queries and scanners.
     *
     * <p>The pool is initialized lazily on the first {@link #create} call so that the pool size can
     * be read from user-supplied options.
     */
    private volatile TantivySearcherPool searcherPool;

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public GlobalIndexer create(DataField field, Options options) {
        if (searcherPool == null) {
            synchronized (this) {
                if (searcherPool == null) {
                    int maxSize = options.get(TantivyFullTextIndexOptions.SEARCHER_POOL_MAX_SIZE);
                    searcherPool = new TantivySearcherPool(maxSize);
                }
            }
        }
        return new TantivyFullTextGlobalIndexer(searcherPool);
    }
}
