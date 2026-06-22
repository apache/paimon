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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Factory for creating ES multi-index global indexers. Supports vector (DiskBBQ/HNSW/Native),
 * fulltext (BM25), and scalar fields.
 */
public class ESIndexGlobalIndexerFactory implements GlobalIndexerFactory {

    public static final String IDENTIFIER = "es-index";

    private static final String READ_SEARCH_THREADS_KEY =
            "global-index.es-index.read-search-threads";
    private static final int DEFAULT_READ_SEARCH_THREADS = -1;

    private static volatile ExecutorService readSearchExecutor;
    private static final Object LOCK = new Object();

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public GlobalIndexer create(DataField field, Options options) {
        return new ESIndexGlobalIndexer(
                java.util.Collections.singletonList(field),
                options,
                getOrCreateReadSearchExecutor(options));
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
        return new ESIndexGlobalIndexer(fields, options, getOrCreateReadSearchExecutor(options));
    }

    /**
     * Returns the shared read/search thread pool. Default (unset or -1) creates a pool sized to
     * CPU/2. Set to 0 to disable parallel search (returns null → serial only).
     */
    private static ExecutorService getOrCreateReadSearchExecutor(Options options) {
        int threads = options.getInteger(READ_SEARCH_THREADS_KEY, DEFAULT_READ_SEARCH_THREADS);
        if (threads == 0) {
            return null;
        }
        if (threads < 0) {
            threads = Math.max(2, Runtime.getRuntime().availableProcessors() / 2);
        }
        if (readSearchExecutor == null) {
            int finalThreads = threads;
            synchronized (LOCK) {
                if (readSearchExecutor == null) {
                    readSearchExecutor = createExecutor(finalThreads);
                }
            }
        }
        return readSearchExecutor;
    }

    private static ExecutorService createExecutor(int threads) {
        ThreadPoolExecutor executor =
                new ThreadPoolExecutor(
                        threads,
                        threads,
                        60L,
                        TimeUnit.SECONDS,
                        new LinkedBlockingQueue<>(256),
                        r -> {
                            Thread t = new Thread(r, "paimon-es-search");
                            t.setDaemon(true);
                            return t;
                        },
                        new ThreadPoolExecutor.CallerRunsPolicy());
        executor.allowCoreThreadTimeOut(true);
        return executor;
    }
}
