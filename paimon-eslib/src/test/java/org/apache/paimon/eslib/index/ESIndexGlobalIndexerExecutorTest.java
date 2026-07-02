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

import org.apache.paimon.fs.Path;
import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.GlobalIndexReader;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * Verifies that the read/search executor controlled by {@code
 * global-index.es-index.read-search-threads} (resolved in the factory and stored on the indexer) is
 * the one actually handed to the reader, rather than the caller-supplied executor.
 */
class ESIndexGlobalIndexerExecutorTest {

    private static final List<DataField> FIELDS =
            Arrays.asList(new DataField(0, "k", DataTypes.STRING()));

    private static List<GlobalIndexIOMeta> oneFile() {
        // The reader requires exactly one file per shard; the meta is never read (no ensureLoaded).
        return Arrays.asList(new GlobalIndexIOMeta(new Path("/tmp/none.index"), 0L, new byte[0]));
    }

    @Test
    void configuredExecutorIsUsedInsteadOfCallerExecutor() {
        ExecutorService configured = Executors.newSingleThreadExecutor();
        ExecutorService caller = Executors.newSingleThreadExecutor();
        try {
            ESIndexGlobalIndexer indexer =
                    new ESIndexGlobalIndexer(FIELDS, new Options(), configured);
            GlobalIndexReader reader = indexer.createReader(meta -> null, oneFile(), caller);
            assertSame(
                    configured,
                    ((ESIndexGlobalIndexReader) reader).searchExecutor(),
                    "reader must use the ES-configured executor, not the caller's");
        } finally {
            configured.shutdownNow();
            caller.shutdownNow();
        }
    }

    @Test
    void zeroThreadsDisablesAsyncEvenWhenCallerProvidesExecutor() {
        ExecutorService caller = Executors.newSingleThreadExecutor();
        try {
            // read-search-threads = 0 -> factory passes a null pool -> serial; the caller executor
            // must NOT be substituted, otherwise the option could not disable async execution.
            ESIndexGlobalIndexer indexer = new ESIndexGlobalIndexer(FIELDS, new Options(), null);
            GlobalIndexReader reader = indexer.createReader(meta -> null, oneFile(), caller);
            assertNull(
                    ((ESIndexGlobalIndexReader) reader).searchExecutor(),
                    "read-search-threads=0 must leave the reader serial (null executor)");
        } finally {
            caller.shutdownNow();
        }
    }
}
