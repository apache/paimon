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

package org.apache.paimon.globalindex.btree;

import org.apache.paimon.globalindex.FilePathGlobalIndexResult;
import org.apache.paimon.globalindex.GlobalIndexEvaluator;
import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.GlobalIndexReader;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.globalindex.OffsetGlobalIndexReader;
import org.apache.paimon.globalindex.UnionGlobalIndexReader;
import org.apache.paimon.globalindex.io.GlobalIndexFileReader;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Range;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

/**
 * Reader for BTree-with-file-meta index (btree + btree_file_meta).
 *
 * <p>btree key index files are evaluated with the standard BTree predicate visitor to obtain
 * matching row IDs. btree_file_meta files are read fully to obtain {@link
 * org.apache.paimon.manifest.ManifestEntry} rows for the files covered by this index shard.
 *
 * <p>Returns a {@link FilePathGlobalIndexResult} combining both.
 */
public class BTreeWithFileMetaReader {

    private final Map<Range, List<GlobalIndexIOMeta>> keyIndexByRange;
    private final Map<Range, List<GlobalIndexIOMeta>> binaryFileMetaByRange;
    private final DataField indexField;
    private final Options options;
    private final GlobalIndexFileReader fileReader;
    private final ExecutorService executor;

    public BTreeWithFileMetaReader(
            Map<Range, List<GlobalIndexIOMeta>> keyIndexByRange,
            Map<Range, List<GlobalIndexIOMeta>> binaryFileMetaByRange,
            DataField indexField,
            Options options,
            GlobalIndexFileReader fileReader,
            ExecutorService executor) {
        this.keyIndexByRange = keyIndexByRange;
        this.binaryFileMetaByRange = binaryFileMetaByRange;
        this.indexField = indexField;
        this.options = options;
        this.fileReader = fileReader;
        this.executor = executor;
    }

    /**
     * Evaluates the predicate against the btree key index and reads btree_file_meta manifest
     * entries.
     *
     * @return {@link Optional#empty()} if no btree key index files exist, otherwise a {@link
     *     FilePathGlobalIndexResult} with row-level bitmap and manifest entry rows.
     */
    public Optional<FilePathGlobalIndexResult> scan(Predicate predicate) throws IOException {
        if (keyIndexByRange.isEmpty()) {
            return Optional.empty();
        }

        // ---- btree key index: standard BTree predicate evaluation ----
        BTreeGlobalIndexer indexer = new BTreeGlobalIndexer(indexField, options);
        RowType singleFieldRowType = new RowType(false, Collections.singletonList(indexField));

        List<GlobalIndexReader> unionReaders = new ArrayList<>();
        for (Map.Entry<Range, List<GlobalIndexIOMeta>> entry : keyIndexByRange.entrySet()) {
            Range range = entry.getKey();
            List<GlobalIndexIOMeta> metas = entry.getValue();
            GlobalIndexReader innerReader = indexer.createReader(fileReader, metas);
            unionReaders.add(new OffsetGlobalIndexReader(innerReader, range.from, range.to));
        }

        GlobalIndexReader keyIndexUnion = new UnionGlobalIndexReader(unionReaders, executor);
        GlobalIndexEvaluator evaluator =
                new GlobalIndexEvaluator(
                        singleFieldRowType, fieldId -> Collections.singletonList(keyIndexUnion));

        Optional<GlobalIndexResult> keyIndexResult;
        try {
            keyIndexResult = evaluator.evaluate(predicate);
        } finally {
            evaluator.close();
        }

        if (!keyIndexResult.isPresent() || keyIndexResult.get().results().isEmpty()) {
            return Optional.empty();
        }

        // ---- btree_file_meta: read all ManifestEntry raw bytes ----
        List<byte[]> manifestEntryBytes = readAllBinaryFileMetaEntries();
        if (manifestEntryBytes.isEmpty()) {
            // No btree_file_meta data — return key-index result only (no file-path optimization)
            return Optional.empty();
        }

        return Optional.of(new FilePathGlobalIndexResult(manifestEntryBytes, keyIndexResult.get()));
    }

    private List<byte[]> readAllBinaryFileMetaEntries() throws IOException {
        // Use LinkedHashMap to deduplicate by fileName across multiple SST files.
        // When the same range is written by multiple parallel subtasks, each subtask produces a
        // complete btree_file_meta SST with identical entries. putIfAbsent keeps only the first
        // occurrence and preserves insertion order.
        LinkedHashMap<String, byte[]> seen = new LinkedHashMap<>();

        for (List<GlobalIndexIOMeta> metas : binaryFileMetaByRange.values()) {
            for (GlobalIndexIOMeta meta : metas) {
                BinaryFileMetaIndexReader reader =
                        new BinaryFileMetaIndexReader(fileReader, meta, options);
                try {
                    BinaryFileMetaIndexReader.EntryIterator it = reader.iterator();
                    while (it.hasNext()) {
                        String fileName = it.nextKey();
                        byte[] valueBytes = it.nextValue();
                        seen.putIfAbsent(fileName, valueBytes);
                    }
                } finally {
                    reader.close();
                }
            }
        }
        return new ArrayList<>(seen.values());
    }
}
