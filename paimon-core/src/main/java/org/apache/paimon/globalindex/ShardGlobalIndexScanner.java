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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Range;

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
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Scanner for shard-based global indexes. */
public class ShardGlobalIndexScanner implements Closeable {

    private final FileStoreTable fileStoreTable;
    private final GlobalIndexFileReadWrite globalIndexFileInputHelper;
    private final Map<Integer, Map<String, List<IndexFileMeta>>> indexMetas;
    private final Map<Integer, DataField> columnIdToDataField;

    private GlobalIndexEvaluator globalIndexEvaluator;

    public ShardGlobalIndexScanner(
            FileStoreTable fileStoreTable,
            BinaryRow partition,
            long rowRangeStart,
            long rowRangeEnd,
            List<IndexManifestEntry> entries) {
        this.fileStoreTable = fileStoreTable;
        FileIO fileIO = fileStoreTable.fileIO();
        this.globalIndexFileInputHelper =
                new GlobalIndexFileReadWrite(
                        fileIO,
                        fileStoreTable.store().pathFactory().indexFileFactory(partition, 0));
        RowType rowType = fileStoreTable.rowType();
        this.columnIdToDataField = new HashMap<>();
        for (DataField dataField : rowType.getFields()) {
            columnIdToDataField.put(dataField.id(), dataField);
        }
        this.indexMetas = new HashMap<>();
        for (IndexManifestEntry entry : entries) {
            GlobalIndexMeta meta = entry.indexFile().globalIndexMeta();
            checkArgument(meta != null, "Global index meta must not be null");
            int fieldId = meta.indexFieldId();
            String indexType = entry.indexFile().indexType();
            indexMetas
                    .computeIfAbsent(fieldId, k -> new HashMap<>())
                    .computeIfAbsent(indexType, k -> new ArrayList<>())
                    .add(entry.indexFile());
        }

        checkArgument(
                entries.stream()
                        .allMatch(
                                m ->
                                        m.indexFile().globalIndexMeta() != null
                                                && Range.intersect(
                                                        rowRangeStart,
                                                        rowRangeEnd,
                                                        m.indexFile()
                                                                .globalIndexMeta()
                                                                .rowRangeStart(),
                                                        m.indexFile()
                                                                .globalIndexMeta()
                                                                .rowRangeEnd())),
                "All index files must have an intersection with row range ["
                        + rowRangeStart
                        + ", "
                        + rowRangeEnd
                        + ")");
    }

    public Optional<GlobalIndexResult> scan(Predicate predicate) {
        GlobalIndexEvaluator globalIndexPredicate = getGlobalIndexEvaluator();
        return globalIndexPredicate.evaluate(predicate);
    }

    private GlobalIndexEvaluator getGlobalIndexEvaluator() {
        if (globalIndexEvaluator == null) {
            Function<Integer, Collection<GlobalIndexReader>> readerFunction =
                    fieldId -> {
                        if (!indexMetas.containsKey(fieldId)) {
                            return Collections.emptyList();
                        }
                        Map<String, List<IndexFileMeta>> indexMetas = this.indexMetas.get(fieldId);

                        Set<GlobalIndexReader> readers = new HashSet<>();
                        try {
                            for (Map.Entry<String, List<IndexFileMeta>> entry :
                                    indexMetas.entrySet()) {
                                String indexType = entry.getKey();
                                List<IndexFileMeta> metas = entry.getValue();
                                GlobalIndexerFactory globalIndexerFactory =
                                        GlobalIndexerFactoryUtils.load(indexType);
                                GlobalIndexer globalIndexer =
                                        globalIndexerFactory.create(
                                                columnIdToDataField.get(fieldId).type(),
                                                new Options(fileStoreTable.options()));
                                readers.add(
                                        globalIndexer.createReader(
                                                globalIndexFileInputHelper,
                                                metas.stream()
                                                        .map(
                                                                meta ->
                                                                        new GlobalIndexIOMeta(
                                                                                meta.fileName(),
                                                                                meta.fileSize(),
                                                                                new Range(
                                                                                        meta.globalIndexMeta()
                                                                                                .rowRangeStart(),
                                                                                        meta.globalIndexMeta()
                                                                                                .rowRangeEnd()),
                                                                                meta.globalIndexMeta()
                                                                                        .indexMeta()))
                                                        .collect(Collectors.toList())));
                            }
                        } catch (IOException e) {
                            throw new RuntimeException("Failed to create global index reader", e);
                        }

                        return readers;
                    };

            globalIndexEvaluator =
                    new GlobalIndexEvaluator(readerFunction, fileStoreTable.rowType());
        }
        return globalIndexEvaluator;
    }

    @Override
    public void close() throws IOException {
        if (globalIndexEvaluator != null) {
            globalIndexEvaluator.close();
        }
    }
}
