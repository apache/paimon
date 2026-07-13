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
 * Verifies that the factory-configured DiskBBQ executor and Paimon's caller-owned async executor
 * remain separate when handed to the reader.
 */
class ESIndexGlobalIndexerExecutorTest {

    private static final List<DataField> FIELDS =
            Arrays.asList(new DataField(0, "k", DataTypes.STRING()));

    private static List<GlobalIndexIOMeta> oneFile() {
        // The reader requires exactly one file per shard; the meta is never read (no ensureLoaded).
        return Arrays.asList(new GlobalIndexIOMeta(new Path("/tmp/none.index"), 0L, new byte[0]));
    }

    @Test
    void configuredAndCallerExecutorsAreKeptSeparate() {
        ExecutorService configured = Executors.newSingleThreadExecutor();
        ExecutorService caller = Executors.newSingleThreadExecutor();
        try {
            ESIndexGlobalIndexer indexer =
                    new ESIndexGlobalIndexer(FIELDS, new Options(), configured);
            GlobalIndexReader reader = indexer.createReader(meta -> null, oneFile(), caller);
            assertSame(
                    caller,
                    ((ESIndexGlobalIndexReader) reader).queryExecutor(),
                    "outer reader futures must use Paimon's caller executor");
            assertSame(
                    configured,
                    ((ESIndexGlobalIndexReader) reader).searchExecutor(),
                    "DiskBBQ must use the ES-configured executor");
        } finally {
            configured.shutdownNow();
            caller.shutdownNow();
        }
    }

    @Test
    void zeroThreadsDisablesOnlyDiskBBQParallelism() {
        ExecutorService caller = Executors.newSingleThreadExecutor();
        try {
            // read-search-threads = 0 -> factory passes a null DiskBBQ pool. The outer future still
            // uses Paimon's caller-owned executor; substituting it as the DiskBBQ pool would
            // reintroduce the nested-executor deadlock.
            ESIndexGlobalIndexer indexer = new ESIndexGlobalIndexer(FIELDS, new Options(), null);
            GlobalIndexReader reader = indexer.createReader(meta -> null, oneFile(), caller);
            assertSame(caller, ((ESIndexGlobalIndexReader) reader).queryExecutor());
            assertNull(
                    ((ESIndexGlobalIndexReader) reader).searchExecutor(),
                    "read-search-threads=0 must leave DiskBBQ serial");
        } finally {
            caller.shutdownNow();
        }
    }

    @Test
    void sameExecutorDisablesNestedDiskBBQParallelism() {
        ExecutorService shared = Executors.newSingleThreadExecutor();
        try {
            ESIndexGlobalIndexReader reader =
                    new ESIndexGlobalIndexReader(
                            meta -> null,
                            oneFile(),
                            FIELDS,
                            new ESIndexOptions(FIELDS, new Options()),
                            shared,
                            shared);
            assertSame(shared, reader.queryExecutor());
            assertNull(
                    reader.searchExecutor(),
                    "DiskBBQ must be serial when both execution layers receive the same pool");
        } finally {
            shared.shutdownNow();
        }
    }
}
