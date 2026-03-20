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

package org.apache.paimon.globalindex;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.IndexPathFactory;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataField;
import org.apache.paimon.utils.Range;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** Utils for global index build. */
public class GlobalIndexBuilderUtils {

    public static List<IndexFileMeta> toIndexFileMetas(
            FileIO fileIO,
            IndexPathFactory indexPathFactory,
            CoreOptions options,
            Range range,
            int indexFieldId,
            String indexType,
            List<ResultEntry> entries)
            throws IOException {
        List<IndexFileMeta> results = new ArrayList<>();
        for (ResultEntry entry : entries) {
            String fileName = entry.fileName();
            long fileSize = fileIO.getFileSize(indexPathFactory.toPath(fileName));
            GlobalIndexMeta globalIndexMeta =
                    new GlobalIndexMeta(range.from, range.to, indexFieldId, null, entry.meta());

            Path externalPathDir = options.globalIndexExternalPath();
            String externalPathString = null;
            if (externalPathDir != null) {
                Path externalPath = new Path(externalPathDir, fileName);
                externalPathString = externalPath.toString();
            }
            IndexFileMeta indexFileMeta =
                    new IndexFileMeta(
                            indexType,
                            fileName,
                            fileSize,
                            entry.rowCount(),
                            globalIndexMeta,
                            externalPathString);
            results.add(indexFileMeta);
        }
        return results;
    }

    /**
     * Adjust rowsPerShard if the estimated shard count exceeds maxShard. Returns the adjusted
     * rowsPerShard value.
     */
    public static long adjustRowsPerShard(long rowsPerShard, long totalRowCount, int maxShard) {
        long estimatedShards = ceilDivision(totalRowCount, rowsPerShard);
        if (estimatedShards > maxShard) {
            return ceilDivision(totalRowCount, maxShard);
        }
        return rowsPerShard;
    }

    /** Integer ceiling division: returns ceil(a / b) for positive a and b. */
    private static long ceilDivision(long a, long b) {
        return (a + b - 1) / b;
    }

    public static GlobalIndexWriter createIndexWriter(
            FileStoreTable table, String indexType, DataField indexField, Options options)
            throws IOException {
        GlobalIndexer globalIndexer = GlobalIndexer.create(indexType, indexField, options);
        return globalIndexer.createWriter(createGlobalIndexFileReadWrite(table));
    }

    private static GlobalIndexFileReadWrite createGlobalIndexFileReadWrite(FileStoreTable table) {
        IndexPathFactory indexPathFactory = table.store().pathFactory().globalIndexFileFactory();
        return new GlobalIndexFileReadWrite(table.fileIO(), indexPathFactory);
    }
}
