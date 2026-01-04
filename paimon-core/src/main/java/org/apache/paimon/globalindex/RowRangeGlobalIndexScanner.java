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

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.IndexPathFactory;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.VectorSearch;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Range;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.IntFunction;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/** Scanner for shard-based global indexes. */
public class RowRangeGlobalIndexScanner implements Closeable {

    private final Options options;
    private final GlobalIndexEvaluator globalIndexEvaluator;

    public RowRangeGlobalIndexScanner(
            Options options,
            RowType rowType,
            FileIO fileIO,
            IndexPathFactory indexPathFactory,
            Range range,
            List<IndexManifestEntry> entries) {
        this.options = options;
        for (IndexManifestEntry entry : entries) {
            GlobalIndexMeta meta = entry.indexFile().globalIndexMeta();
            checkArgument(
                    meta != null
                            && Range.intersect(
                                    range.from, range.to, meta.rowRangeStart(), meta.rowRangeEnd()),
                    "All index files must have an intersection with row range ["
                            + range.from
                            + ", "
                            + range.to
                            + ")");
        }

        GlobalIndexFileReadWrite indexFileReadWrite =
                new GlobalIndexFileReadWrite(fileIO, indexPathFactory);

        Map<Integer, Map<String, Map<Range, List<IndexFileMeta>>>> indexMetas = new HashMap<>();
        for (IndexManifestEntry entry : entries) {
            GlobalIndexMeta meta = entry.indexFile().globalIndexMeta();
            checkArgument(meta != null, "Global index meta must not be null");
            int fieldId = meta.indexFieldId();
            String indexType = entry.indexFile().indexType();
            indexMetas
                    .computeIfAbsent(fieldId, k -> new HashMap<>())
                    .computeIfAbsent(indexType, k -> new HashMap<>())
                    .computeIfAbsent(
                            new Range(meta.rowRangeStart(), meta.rowRangeStart()),
                            k -> new ArrayList<>())
                    .add(entry.indexFile());
        }

        IntFunction<Collection<GlobalIndexReader>> readersFunction =
                fieldId ->
                        createReaders(
                                indexFileReadWrite,
                                indexMetas.get(fieldId),
                                rowType.getField(fieldId));
        this.globalIndexEvaluator = new GlobalIndexEvaluator(rowType, readersFunction);
    }

    public Optional<GlobalIndexResult> scan(
            Predicate predicate, @Nullable VectorSearch vectorSearch) {
        return globalIndexEvaluator.evaluate(predicate, vectorSearch);
    }

    private Collection<GlobalIndexReader> createReaders(
            GlobalIndexFileReadWrite indexFileReadWrite,
            Map<String, Map<Range, List<IndexFileMeta>>> indexMetas,
            DataField dataField) {
        if (indexMetas == null) {
            return Collections.emptyList();
        }

        Set<GlobalIndexReader> readers = new HashSet<>();
        try {
            for (Map.Entry<String, Map<Range, List<IndexFileMeta>>> entry : indexMetas.entrySet()) {
                String indexType = entry.getKey();
                Map<Range, List<IndexFileMeta>> metas = entry.getValue();
                GlobalIndexerFactory globalIndexerFactory =
                        GlobalIndexerFactoryUtils.load(indexType);
                GlobalIndexer globalIndexer = globalIndexerFactory.create(dataField, options);

                List<GlobalIndexReader> unionReader = new ArrayList<>();
                for (Map.Entry<Range, List<IndexFileMeta>> rangeMetas : metas.entrySet()) {
                    Range range = rangeMetas.getKey();
                    List<IndexFileMeta> indexFileMetas = rangeMetas.getValue();

                    List<GlobalIndexIOMeta> globalMetas =
                            indexFileMetas.stream()
                                    .map(this::toGlobalMeta)
                                    .collect(Collectors.toList());
                    GlobalIndexReader innerReader =
                            new OffsetGlobalIndexReader(
                                    globalIndexer.createReader(indexFileReadWrite, globalMetas),
                                    range.from);
                    unionReader.add(innerReader);
                }

                readers.add(new UnionGlobalIndexReader(unionReader));
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to create global index reader", e);
        }

        return readers;
    }

    private GlobalIndexIOMeta toGlobalMeta(IndexFileMeta meta) {
        GlobalIndexMeta globalIndex = meta.globalIndexMeta();
        checkNotNull(globalIndex);
        return new GlobalIndexIOMeta(meta.fileName(), meta.fileSize(), globalIndex.indexMeta());
    }

    @Override
    public void close() throws IOException {
        globalIndexEvaluator.close();
    }
}
