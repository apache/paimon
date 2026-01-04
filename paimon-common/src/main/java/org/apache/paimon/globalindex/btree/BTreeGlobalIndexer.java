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

package org.apache.paimon.globalindex.btree;

import org.apache.paimon.compression.BlockCompressionFactory;
import org.apache.paimon.compression.CompressOptions;
import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.GlobalIndexReader;
import org.apache.paimon.globalindex.GlobalIndexer;
import org.apache.paimon.globalindex.io.GlobalIndexFileReader;
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;
import org.apache.paimon.io.cache.CacheManager;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataField;
import org.apache.paimon.utils.LazyField;

import java.io.IOException;
import java.util.List;

/**
 * The {@link GlobalIndexer} for btree index. We do not build a B-tree directly in memory, instead,
 * we form a logical B-tree via multi-level metadata over SST files that store the actual data, as
 * below:
 *
 * <pre>
 *                                             BTree-Index
 *                                             /         \
 *                                            /    ...    \
 *                                           /             \
 *     +--------------------------------------+           +------------+
 *     |               SST File               |           |            |
 *     +--------------------------------------+           |            |
 *     |              Root Index              |           |            |
 *     |             /   ...    \             |    ...    |  SST File  |
 *     |     Leaf Index  ...  Leaf Index      |           |            |
 *     |     /  ...   \       /  ...   \      |           |            |
 *     | DataBlock       ...        DataBlock |           |            |
 *     +--------------------------------------+           +------------+
 * </pre>
 *
 * <p>This approach significantly reduces memory pressure during index reads.
 */
public class BTreeGlobalIndexer implements GlobalIndexer {

    private final KeySerializer keySerializer;
    private final Options options;
    private final LazyField<CacheManager> cacheManager;

    public BTreeGlobalIndexer(DataField dataField, Options options) {
        this.keySerializer = KeySerializer.create(dataField.type());
        this.options = options;
        // todo: cacheManager can be null to disallow data cache.
        this.cacheManager =
                new LazyField<>(
                        () ->
                                new CacheManager(
                                        options.get(BTreeIndexOptions.BTREE_INDEX_CACHE_SIZE),
                                        options.get(
                                                BTreeIndexOptions
                                                        .BTREE_INDEX_HIGH_PRIORITY_POOL_RATIO)));
    }

    @Override
    public BTreeIndexWriter createWriter(GlobalIndexFileWriter fileWriter) throws IOException {
        long blockSize = options.get(BTreeIndexOptions.BTREE_INDEX_BLOCK_SIZE).getBytes();
        CompressOptions compressOptions =
                new CompressOptions(
                        options.get(BTreeIndexOptions.BTREE_INDEX_COMPRESSION),
                        options.get(BTreeIndexOptions.BTREE_INDEX_COMPRESSION_LEVEL));
        return new BTreeIndexWriter(
                fileWriter,
                keySerializer,
                (int) blockSize,
                BlockCompressionFactory.create(compressOptions));
    }

    @Override
    public GlobalIndexReader createReader(
            GlobalIndexFileReader fileReader, List<GlobalIndexIOMeta> files) throws IOException {
        return new LazyFilteredBTreeReader(files, keySerializer, fileReader, cacheManager.get());
    }
}
