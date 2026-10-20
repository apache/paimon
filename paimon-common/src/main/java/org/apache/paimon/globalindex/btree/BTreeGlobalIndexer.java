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
import org.apache.paimon.globalindex.CompositeKeySerializer;
import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.GlobalIndexKeyExtractor;
import org.apache.paimon.globalindex.GlobalIndexReader;
import org.apache.paimon.globalindex.GlobalIndexer;
import org.apache.paimon.globalindex.KeySerializer;
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.globalindex.SortedGlobalIndexer;
import org.apache.paimon.globalindex.io.GlobalIndexFileReader;
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;
import org.apache.paimon.io.cache.CacheManager;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.BloomFilter;
import org.apache.paimon.utils.LazyField;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RoaringNavigableMap64;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.function.LongUnaryOperator;

import static org.apache.paimon.utils.Preconditions.checkArgument;

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
public class BTreeGlobalIndexer implements SortedGlobalIndexer {

    private static final double BLOOM_FILTER_FPP = 0.05;

    private final KeySerializer keySerializer;
    private final List<DataField> fields;
    private final GlobalIndexKeyExtractor keyExtractor;
    private final Options options;
    private final long fallbackScanMaxSize;
    private final LazyField<CacheManager> cacheManager;

    public BTreeGlobalIndexer(List<DataField> fields, Options options) {
        this.fields = new ArrayList<>(fields);
        checkArgument(!fields.isEmpty(), "BTree index requires at least one field.");
        for (DataField field : fields) {
            if (field.type() instanceof RowType) {
                throw new UnsupportedOperationException(
                        "BTree index columns must have scalar types: " + field.name());
            }
        }
        DataType keyType = fields.size() == 1 ? fields.get(0).type() : new RowType(fields);
        this.keySerializer =
                fields.size() == 1
                        ? KeySerializer.create(keyType)
                        : new CompositeKeySerializer((RowType) keyType);
        this.keyExtractor = GlobalIndexKeyExtractor.identity(keyType);
        this.options = options;
        this.fallbackScanMaxSize =
                options.get(BTreeIndexOptions.BTREE_INDEX_FALLBACK_SCAN_MAX_SIZE).getBytes();
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
    public GlobalIndexKeyExtractor keyExtractor() {
        return keyExtractor;
    }

    @Override
    public BTreeIndexWriter createWriter(GlobalIndexFileWriter fileWriter) throws IOException {
        long blockSize = options.get(BTreeIndexOptions.BTREE_INDEX_BLOCK_SIZE).getBytes();
        CompressOptions compressOptions =
                new CompressOptions(
                        options.get(BTreeIndexOptions.BTREE_INDEX_COMPRESSION),
                        options.get(BTreeIndexOptions.BTREE_INDEX_COMPRESSION_LEVEL));
        BloomFilter.Builder bloomFilterBuilder =
                options.get(BTreeIndexOptions.BTREE_INDEX_BLOOM_FILTER_ENABLED)
                        ? BloomFilter.dynamicBuilder(BLOOM_FILTER_FPP)
                        : null;
        return new BTreeIndexWriter(
                fileWriter,
                keySerializer,
                (int) blockSize,
                bloomFilterBuilder,
                BlockCompressionFactory.create(compressOptions),
                options.get(BTreeIndexOptions.BTREE_INDEX_FILE_VERSION));
    }

    @Override
    public GlobalIndexReader createReader(
            GlobalIndexFileReader fileReader,
            List<GlobalIndexIOMeta> files,
            long totalRowCount,
            @Nullable List<Range> rowRanges,
            ExecutorService executor) {
        return new LazyFilteredBTreeReader(
                files,
                fields,
                keySerializer,
                fileReader,
                cacheManager.get(),
                fallbackScanMaxSize,
                totalRowCount,
                rowRanges,
                executor);
    }

    @Override
    public List<ResultEntry> rewriteRowIds(
            GlobalIndexFileReader fileReader,
            List<GlobalIndexIOMeta> files,
            long sourceRowCount,
            long targetRowCount,
            LongUnaryOperator mapping,
            GlobalIndexFileWriter fileWriter)
            throws IOException {
        checkArgument(sourceRowCount >= 0 && targetRowCount >= 0, "Invalid row count.");
        List<ResultEntry> results = new ArrayList<>();
        if (targetRowCount == 0) {
            return results;
        }
        for (GlobalIndexIOMeta file : files) {
            try (BTreeIndexReader reader =
                    new BTreeIndexReader(
                            keySerializer, fileReader, file, cacheManager.get(), null)) {
                RoaringNavigableMap64 nullRows = new RoaringNavigableMap64();
                reader.scanNullRowIds(
                        rowId ->
                                addMappedRowId(
                                        nullRows, rowId, sourceRowCount, targetRowCount, mapping));
                BTreeIndexWriter writer = null;
                try {
                    if (!nullRows.isEmpty()) {
                        writer = createWriter(fileWriter);
                        for (long rowId : nullRows) {
                            writer.write(null, rowId);
                        }
                    }
                    BTreeIndexReader.EntryIterator entries = reader.entryIterator();
                    while (entries.hasNext()) {
                        BTreeIndexReader.KeyRowIds entry = entries.next();
                        RoaringNavigableMap64 rowIds = new RoaringNavigableMap64();
                        for (long rowId : entry.rowIds()) {
                            addMappedRowId(rowIds, rowId, sourceRowCount, targetRowCount, mapping);
                        }
                        if (!rowIds.isEmpty()) {
                            if (writer == null) {
                                writer = createWriter(fileWriter);
                            }
                            // A permutation can reverse a posting list; v2 requires sorted,
                            // unique IDs.
                            for (long rowId : rowIds) {
                                writer.write(entry.key(), rowId);
                            }
                        }
                    }
                    if (writer != null) {
                        results.addAll(writer.finish());
                    }
                } finally {
                    if (writer != null) {
                        writer.close();
                    }
                }
            }
        }
        return results;
    }

    private static void addMappedRowId(
            RoaringNavigableMap64 result,
            long sourceRowId,
            long sourceRowCount,
            long targetRowCount,
            LongUnaryOperator mapping) {
        checkArgument(
                sourceRowId >= 0 && sourceRowId < sourceRowCount,
                "Invalid source row ID: %s",
                sourceRowId);
        long target = mapping.applyAsLong(sourceRowId);
        checkArgument(target >= -1 && target < targetRowCount, "Invalid target row ID: %s", target);
        if (target >= 0) {
            result.add(target);
        }
    }
}
