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
import org.apache.paimon.globalindex.GlobalIndexSingletonWriter;
import org.apache.paimon.globalindex.GlobalIndexer;
import org.apache.paimon.globalindex.IndexedSplit;
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.utils.LongCounter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** This is a class who truly build index file and generate index metas. */
public abstract class GlobalIndexBuilder {

    protected final GlobalIndexBuilderContext context;

    protected GlobalIndexBuilder(GlobalIndexBuilderContext context) {
        this.context = context;
    }

    public CommitMessage build(IndexedSplit indexedSplit) throws IOException {
        ReadBuilder builder = context.table().newReadBuilder();
        builder.withReadType(context.readType());
        RecordReader<InternalRow> rows = builder.newRead().createReader(indexedSplit);
        LongCounter rowCounter = new LongCounter(0);
        List<ResultEntry> resultEntries = writePaimonRows(context, rows, rowCounter);
        List<IndexFileMeta> indexFileMetas =
                convertToIndexMeta(context, rowCounter.getValue(), resultEntries);
        DataIncrement dataIncrement = DataIncrement.indexIncrement(indexFileMetas);
        return new CommitMessageImpl(
                context.partition(), 0, null, dataIncrement, CompactIncrement.emptyIncrement());
    }

    private static List<IndexFileMeta> convertToIndexMeta(
            GlobalIndexBuilderContext context, long totalRowCount, List<ResultEntry> entries)
            throws IOException {
        List<IndexFileMeta> results = new ArrayList<>();
        long rangeEnd = context.startOffset() + totalRowCount - 1;
        for (ResultEntry entry : entries) {
            String fileName = entry.fileName();
            GlobalIndexFileReadWrite readWrite = context.globalIndexFileReadWrite();
            long fileSize = readWrite.fileSize(fileName);
            GlobalIndexMeta globalIndexMeta =
                    new GlobalIndexMeta(
                            context.startOffset(),
                            rangeEnd,
                            context.indexField().id(),
                            null,
                            entry.meta());
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

    private static List<ResultEntry> writePaimonRows(
            GlobalIndexBuilderContext context,
            RecordReader<InternalRow> rows,
            LongCounter rowCounter)
            throws IOException {
        GlobalIndexer globalIndexer =
                GlobalIndexer.create(context.indexType(), context.indexField(), context.options());
        GlobalIndexSingletonWriter globalIndexWriter =
                (GlobalIndexSingletonWriter)
                        globalIndexer.createWriter(context.globalIndexFileReadWrite());
        InternalRow.FieldGetter getter =
                InternalRow.createFieldGetter(
                        context.indexField().type(),
                        context.readType().getFieldIndex(context.indexField().name()));
        rows.forEachRemaining(
                row -> {
                    Object indexO = getter.getFieldOrNull(row);
                    globalIndexWriter.write(indexO);
                    rowCounter.add(1);
                });
        return globalIndexWriter.finish();
    }
}
