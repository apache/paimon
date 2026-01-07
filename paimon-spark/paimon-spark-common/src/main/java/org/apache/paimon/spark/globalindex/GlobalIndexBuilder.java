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

package org.apache.paimon.spark.globalindex;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.globalindex.GlobalIndexFileReadWrite;
import org.apache.paimon.globalindex.GlobalIndexWriter;
import org.apache.paimon.globalindex.GlobalIndexer;
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.utils.CloseableIterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** This is a class who truly build index file and generate index metas. */
public abstract class GlobalIndexBuilder {

    protected final GlobalIndexBuilderContext context;

    protected GlobalIndexBuilder(GlobalIndexBuilderContext context) {
        this.context = context;
    }

    public abstract List<CommitMessage> build(CloseableIterator<InternalRow> data)
            throws IOException;

    protected List<IndexFileMeta> convertToIndexMeta(
            long rangeStart, long rangeEnd, List<ResultEntry> entries) throws IOException {
        List<IndexFileMeta> results = new ArrayList<>();
        for (ResultEntry entry : entries) {
            String fileName = entry.fileName();
            GlobalIndexFileReadWrite readWrite = context.globalIndexFileReadWrite();
            long fileSize = readWrite.fileSize(fileName);
            GlobalIndexMeta globalIndexMeta =
                    new GlobalIndexMeta(
                            rangeStart, rangeEnd, context.indexField().id(), null, entry.meta());
            IndexFileMeta indexFileMeta =
                    new IndexFileMeta(
                            context.indexType(),
                            fileName,
                            fileSize,
                            entry.rowCount(),
                            globalIndexMeta);
            results.add(indexFileMeta);
        }
        return results;
    }

    protected GlobalIndexWriter createIndexWriter() throws IOException {
        GlobalIndexer globalIndexer =
                GlobalIndexer.create(context.indexType(), context.indexField(), context.options());
        return globalIndexer.createWriter(context.globalIndexFileReadWrite());
    }
}
