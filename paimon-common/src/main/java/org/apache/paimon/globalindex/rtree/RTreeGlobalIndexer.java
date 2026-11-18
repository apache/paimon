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

package org.apache.paimon.globalindex.rtree;

import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.GlobalIndexReader;
import org.apache.paimon.globalindex.GlobalIndexer;
import org.apache.paimon.globalindex.io.GlobalIndexFileReader;
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.utils.Range;

import javax.annotation.Nullable;

import java.util.List;
import java.util.concurrent.ExecutorService;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Immutable R-tree with bounded-memory builds and demand-loaded pages. */
public class RTreeGlobalIndexer implements GlobalIndexer {

    private final DataField field;
    private final int pageSize;
    private final int partitionRows;
    private final RTreeIndexReadContext readContext;
    private final int readBatchBytes;
    private final int readFileConcurrency;

    public RTreeGlobalIndexer(DataField field, Options options) {
        checkArgument(
                field.type().is(DataTypeRoot.GEOMETRY),
                "R-tree index requires GEOMETRY, but field '%s' is %s.",
                field.name(),
                field.type());
        this.field = field;
        pageSize = options.get(RTreeIndexOptions.PAGE_SIZE);
        partitionRows = options.get(RTreeIndexOptions.PARTITION_ROW_COUNT);
        int cachePages = options.get(RTreeIndexOptions.READ_CACHE_PAGES);
        checkArgument(pageSize >= 2 && pageSize <= 4096, "R-tree page size must be in [2, 4096].");
        checkArgument(
                partitionRows >= 1 && partitionRows <= 1048576,
                "R-tree partition row count must be in [1, 1048576].");
        checkArgument(cachePages >= 0, "R-tree cache page count must be non-negative.");
        long cacheBytes = options.get(RTreeIndexOptions.READ_CACHE_SIZE).getBytes();
        if (!options.contains(RTreeIndexOptions.READ_CACHE_SIZE)
                && options.contains(RTreeIndexOptions.READ_CACHE_PAGES)) {
            cacheBytes = (long) cachePages * RTreeIndexFile.pageBytes(pageSize);
        }
        checkArgument(cacheBytes >= 0, "R-tree cache size must be non-negative.");
        long batchBytes = options.get(RTreeIndexOptions.READ_BATCH_SIZE).getBytes();
        checkArgument(
                batchBytes >= 4096 && batchBytes <= 16 * 1024 * 1024,
                "R-tree read batch size must be in [4 KiB, 16 MiB].");
        readBatchBytes = (int) batchBytes;
        readFileConcurrency = options.get(RTreeIndexOptions.READ_FILE_CONCURRENCY);
        checkArgument(readFileConcurrency > 0, "R-tree read file concurrency must be positive.");
        readContext = new RTreeIndexReadContext(cacheBytes);
    }

    @Override
    public RTreeGlobalIndexWriter createWriter(GlobalIndexFileWriter fileWriter) {
        return new RTreeGlobalIndexWriter(fileWriter, pageSize, partitionRows);
    }

    @Override
    public GlobalIndexReader createReader(
            GlobalIndexFileReader fileReader,
            List<GlobalIndexIOMeta> files,
            long totalRowCount,
            @Nullable List<Range> rowRanges,
            ExecutorService executor) {
        checkArgument(totalRowCount >= 0, "R-tree total row count must be non-negative.");
        checkArgument(
                !files.isEmpty() || totalRowCount == 0,
                "R-tree index files are missing for %s source rows.",
                totalRowCount);
        return new RTreeGlobalIndexReader(
                field,
                fileReader,
                files,
                totalRowCount,
                rowRanges,
                executor,
                readContext,
                readBatchBytes,
                readFileConcurrency);
    }
}
