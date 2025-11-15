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

package org.apache.paimon.index.globalindex;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Scanner for shard-based global indexes. */
public class ShardGlobalIndexScanner implements Closeable {

    private final FileStoreTable fileStoreTable;
    private final GlobalIndexFileHelper globalIndexFileInputHelper;
    private final Map<Integer, Map<String, List<IndexFileMeta>>> indexMetas;
    private final Map<Integer, DataField> columnIdToDataField;

    private GlobalIndexPredicate globalIndexPredicate;

    public ShardGlobalIndexScanner(
            FileStoreTable fileStoreTable,
            BinaryRow partition,
            long shardId,
            List<IndexManifestEntry> entries) {
        this.fileStoreTable = fileStoreTable;
        FileIO fileIO = fileStoreTable.fileIO();
        this.globalIndexFileInputHelper =
                new GlobalIndexFileHelper(
                        fileIO,
                        fileStoreTable.store().pathFactory().indexFileFactory(partition, 0));
        RowType rowType = fileStoreTable.rowType();
        this.columnIdToDataField = new HashMap<>();
        for (DataField dataField : rowType.getFields()) {
            columnIdToDataField.put(dataField.id(), dataField);
        }
        this.indexMetas = new HashMap<>();
        for (IndexManifestEntry entry : entries) {
            int fieldId = entry.indexFile().indexFieldId();
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
                                        m.indexFile().getShard() != null
                                                && m.indexFile().getShard() == shardId),
                "All index files must belong to shard " + shardId);
    }

    public GlobalIndexResult scan(Predicate predicate) {
        GlobalIndexPredicate globalIndexPredicate = getGlobalIndexPredicate();
        return globalIndexPredicate.evaluate(predicate);
    }

    private GlobalIndexPredicate getGlobalIndexPredicate() {
        if (globalIndexPredicate == null) {
            Function<Integer, Collection<GlobalIndexLeafPredicator>> readerFunction =
                    fieldId -> {
                        if (!indexMetas.containsKey(fieldId)) {
                            return Collections.emptyList();
                        }
                        Map<String, List<IndexFileMeta>> indexMetas = this.indexMetas.get(fieldId);

                        Set<GlobalIndexLeafPredicator> readers = new HashSet<>();
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
                                                globalIndexFileInputHelper, metas));
                            }
                        } catch (IOException e) {
                            throw new RuntimeException("Failed to create global index reader", e);
                        }

                        return readers;
                    };

            globalIndexPredicate =
                    new GlobalIndexPredicate(readerFunction, fileStoreTable.rowType());
        }
        return globalIndexPredicate;
    }

    @Override
    public void close() throws IOException {
        if (globalIndexPredicate != null) {
            globalIndexPredicate.close();
        }
    }
}
