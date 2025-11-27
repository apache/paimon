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
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.IndexManifestEntrySerializer;
import org.apache.paimon.reader.RecordReader;
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

    public List<IndexManifestEntry> build(DataSplit dataSplit) throws IOException {
        final GlobalIndexBuilderContext buildContext = this.context;
        JavaSparkContext javaSparkContext =
                new JavaSparkContext(buildContext.spark().sparkContext());
        byte[] dsBytes = InstantiationUtil.serializeObject(dataSplit);
        IndexManifestEntrySerializer indexManifestEntrySerializer =
                new IndexManifestEntrySerializer();
        return javaSparkContext.parallelize(Collections.singletonList(dsBytes))
                .map(
                        splitBytes -> {
                            DataSplit split =
                                    InstantiationUtil.deserializeObject(
                                            splitBytes, GlobalIndexBuilder.class.getClassLoader());
                            ReadBuilder builder = buildContext.table().newReadBuilder();
                            builder.withRowIds(buildContext.range().toListLong())
                                    .withReadType(buildContext.readType());
                            RecordReader<InternalRow> rows = builder.newRead().createReader(split);
                            List<GlobalIndexWriter.ResultEntry> resultEntries =
                                    writePaimonRows(buildContext, rows);
                            return convertToEntry(buildContext, resultEntries);
                        })
                .flatMap(
                        e ->
                                e.stream()
                                        .map(
                                                entry -> {
                                                    try {
                                                        return indexManifestEntrySerializer
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
                                return indexManifestEntrySerializer.deserializeFromBytes(e);
                            } catch (IOException ex) {
                                throw new RuntimeException(ex);
                            }
                        })
                .collect(Collectors.toList());
    }

    private static List<IndexManifestEntry> convertToEntry(
            GlobalIndexBuilderContext context, List<GlobalIndexWriter.ResultEntry> entries)
            throws IOException {
        List<IndexManifestEntry> results = new ArrayList<>();
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
            results.add(
                    new IndexManifestEntry(
                            FileKind.ADD, context.partitionFromBytes(), 0, indexFileMeta));
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
