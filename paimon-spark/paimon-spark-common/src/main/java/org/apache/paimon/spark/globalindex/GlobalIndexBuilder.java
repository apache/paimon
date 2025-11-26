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
import org.apache.paimon.spark.SparkRow;
import org.apache.paimon.utils.Range;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

/** This is a class who truly build index file and generate index metas. */
public abstract class GlobalIndexBuilder {

    protected final GlobalIndexBuilderContext context;

    protected GlobalIndexBuilder(GlobalIndexBuilderContext context) {
        this.context = context;
    }

    public List<IndexManifestEntry> build(Dataset<Row> input) {
        Dataset<Row> customDs = customTopo(input);
        final GlobalIndexBuilderContext globalContext = this.context;
        final IndexManifestEntrySerializer manifestEntrySerializer =
                new IndexManifestEntrySerializer();
        List<byte[]> result =
                customDs.toJavaRDD()
                        .mapPartitions(
                                (FlatMapFunction<Iterator<Row>, GlobalIndexWriter.ResultEntry>)
                                        rows -> writeRows(globalContext, rows))
                        .map(
                                entry -> {
                                    String fileName = entry.fileName();
                                    Range range =
                                            entry.rowRange()
                                                    .addOffset(globalContext.rangeStartOffset());
                                    GlobalIndexFileReadWrite readWrite =
                                            globalContext.globalIndexFileReadWrite();
                                    long fileSize = readWrite.fileSize(fileName);
                                    GlobalIndexMeta globalIndexMeta =
                                            new GlobalIndexMeta(
                                                    range.from,
                                                    range.to,
                                                    globalContext.indexField().id(),
                                                    null,
                                                    entry.meta());
                                    IndexFileMeta indexFileMeta =
                                            new IndexFileMeta(
                                                    BitmapGlobalIndexerFactory.IDENTIFIER,
                                                    fileName,
                                                    fileSize,
                                                    range.to - range.from + 1,
                                                    globalIndexMeta);
                                    IndexManifestEntry indexManifestEntry =
                                            new IndexManifestEntry(
                                                    FileKind.ADD,
                                                    globalContext.partitionFromBytes(),
                                                    0,
                                                    indexFileMeta);
                                    return manifestEntrySerializer.serializeToBytes(
                                            indexManifestEntry);
                                })
                        .collect();

        return result.stream()
                .map(
                        b -> {
                            try {
                                return manifestEntrySerializer.deserializeFromBytes(b);
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        })
                .collect(Collectors.toList());
    }

    private static Iterator<GlobalIndexWriter.ResultEntry> writeRows(
            GlobalIndexBuilderContext context, Iterator<Row> partitionRows) throws IOException {
        GlobalIndexer globalIndexer =
                GlobalIndexer.create(
                        context.indexType(), context.indexField().type(), context.options());
        GlobalIndexWriter globalIndexWriter =
                globalIndexer.createWriter(context.globalIndexFileReadWrite());
        InternalRow.FieldGetter getter =
                InternalRow.createFieldGetter(
                        context.indexField().type(),
                        context.readType().getFieldIndex(context.indexField().name()));
        partitionRows.forEachRemaining(
                row -> {
                    Object indexO = getter.getFieldOrNull(new SparkRow(context.readType(), row));
                    globalIndexWriter.write(indexO);
                });

        return globalIndexWriter.finish().iterator();
    }

    public abstract Dataset<Row> customTopo(Dataset<Row> input);
}
