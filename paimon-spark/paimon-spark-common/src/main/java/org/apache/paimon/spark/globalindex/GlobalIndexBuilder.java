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
import org.apache.paimon.globalindex.bitmap.BitmapGlobalIndexerFactory;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.IndexFileMetaSerializer;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.utils.InstantiationUtil;
import org.apache.paimon.utils.Range;

import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/** This is a class who truly build index file and generate index metas. */
public abstract class GlobalIndexBuilder {

    protected final GlobalIndexBuilderContext context;

    protected GlobalIndexBuilder(GlobalIndexBuilderContext context) {
        this.context = context;
    }

    public CommitMessage build(DataSplit dataSplit) throws IOException {
        final GlobalIndexBuilderContext buildContext = this.context;
        JavaSparkContext javaSparkContext =
                new JavaSparkContext(buildContext.spark().sparkContext());
        byte[] dsBytes = InstantiationUtil.serializeObject(dataSplit);
        IndexFileMetaSerializer indexFileMetaSerializer = new IndexFileMetaSerializer();
        List<IndexFileMeta> indexEntries =
                javaSparkContext.parallelize(Collections.singletonList(dsBytes))
                        .map(
                                splitBytes -> {
                                    DataSplit split =
                                            InstantiationUtil.deserializeObject(
                                                    splitBytes,
                                                    GlobalIndexBuilder.class.getClassLoader());
                                    ReadBuilder builder = buildContext.table().newReadBuilder();
                                    builder.withRowIds(buildContext.range().toListLong())
                                            .withReadType(buildContext.readType());
                                    RecordReader<InternalRow> rows =
                                            builder.newRead().createReader(split);
                                    List<GlobalIndexWriter.ResultEntry> resultEntries =
                                            writePaimonRows(buildContext, rows);
                                    return convertToIndexMeta(buildContext, resultEntries);
                                })
                        .flatMap(
                                e ->
                                        e.stream()
                                                .map(
                                                        entry -> {
                                                            try {
                                                                return indexFileMetaSerializer
                                                                        .serializeToBytes(entry);
                                                            } catch (IOException ex) {
                                                                throw new RuntimeException(ex);
                                                            }
                                                        })
                                                .iterator())
                        .collect().stream()
                        .map(
                                e -> {
                                    try {
                                        return indexFileMetaSerializer.deserializeFromBytes(e);
                                    } catch (IOException ex) {
                                        throw new RuntimeException(ex);
                                    }
                                })
                        .collect(Collectors.toList());

        DataIncrement dataIncrement = DataIncrement.indexIncrement(indexEntries);
        return new CommitMessageImpl(
                context.partition(), 0, null, dataIncrement, CompactIncrement.emptyIncrement());
    }

    private static List<IndexFileMeta> convertToIndexMeta(
            GlobalIndexBuilderContext context, List<GlobalIndexWriter.ResultEntry> entries)
            throws IOException {
        List<IndexFileMeta> results = new ArrayList<>();
        for (GlobalIndexWriter.ResultEntry entry : entries) {
            String fileName = entry.fileName();
            Range range = entry.rowRange().addOffset(context.range().from);
            GlobalIndexFileReadWrite readWrite = context.globalIndexFileReadWrite();
            long fileSize = readWrite.fileSize(fileName);
            GlobalIndexMeta globalIndexMeta =
                    new GlobalIndexMeta(
                            range.from, range.to, context.indexField().id(), null, entry.meta());
            IndexFileMeta indexFileMeta =
                    new IndexFileMeta(
                            BitmapGlobalIndexerFactory.IDENTIFIER,
                            fileName,
                            fileSize,
                            range.to - range.from + 1,
                            globalIndexMeta);
            results.add(indexFileMeta);
        }
        return results;
    }

    private static List<GlobalIndexWriter.ResultEntry> writePaimonRows(
            GlobalIndexBuilderContext context, RecordReader<InternalRow> rows) throws IOException {
        GlobalIndexer globalIndexer =
                GlobalIndexer.create(
                        context.indexType(), context.indexField().type(), context.options());
        GlobalIndexWriter globalIndexWriter =
                globalIndexer.createWriter(context.globalIndexFileReadWrite());
        InternalRow.FieldGetter getter =
                InternalRow.createFieldGetter(
                        context.indexField().type(),
                        context.readType().getFieldIndex(context.indexField().name()));
        rows.forEachRemaining(
                row -> {
                    Object indexO = getter.getFieldOrNull(row);
                    globalIndexWriter.write(indexO);
                });
        return globalIndexWriter.finish();
    }
}
