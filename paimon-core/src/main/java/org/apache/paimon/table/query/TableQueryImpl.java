/*
 *
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.apache.paimon.table.query;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.FileStore;
import org.apache.paimon.KeyValue;
import org.apache.paimon.KeyValueFileStore;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.KeyValueFileReaderFactory;
import org.apache.paimon.io.cache.CacheManager;
import org.apache.paimon.lookup.hash.HashLookupStoreFactory;
import org.apache.paimon.mergetree.Levels;
import org.apache.paimon.mergetree.LookupLevels;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.KeyComparatorSupplier;
import org.apache.paimon.utils.Preconditions;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.apache.paimon.CoreOptions.LOOKUP_CACHE_MAX_MEMORY_SIZE;

/** Implementation for {@link TableQuery}. */
public class TableQueryImpl implements TableQuery {

    private final Map<BinaryRow, Map<Integer, LookupLevels>> tableView;

    private final CoreOptions options;

    private final Supplier<Comparator<InternalRow>> keyComparatorSupplier;

    private final KeyValueFileReaderFactory.Builder readerFactoryBuilder;

    private final HashLookupStoreFactory hashLookupStoreFactory;

    private IOManager ioManager;

    public TableQueryImpl(FileStoreTable table) {
        this.options = table.coreOptions();
        this.tableView = new HashMap<>();
        FileStore<?> tableStore = table.store();
        if (!(tableStore instanceof KeyValueFileStore)) {
            throw new UnsupportedOperationException(
                    "Table Query only supports table with primary key.");
        }
        KeyValueFileStore store = (KeyValueFileStore) tableStore;

        this.readerFactoryBuilder = store.newReaderFactoryBuilder();
        this.keyComparatorSupplier = new KeyComparatorSupplier(readerFactoryBuilder.keyType());
        this.hashLookupStoreFactory =
                new HashLookupStoreFactory(
                        new CacheManager(
                                options.pageSize(),
                                options.toConfiguration().get(LOOKUP_CACHE_MAX_MEMORY_SIZE)),
                        options.toConfiguration().get(CoreOptions.LOOKUP_HASH_LOAD_FACTOR));
        Preconditions.checkArgument(
                options.changelogProducer() == CoreOptions.ChangelogProducer.LOOKUP,
                "Only lookup compaction table supported.");
    }

    @Override
    public void refreshFiles(
            BinaryRow partition,
            int bucket,
            List<DataFileMeta> beforeFiles,
            List<DataFileMeta> dataFiles) {
        LookupLevels lookupLevels =
                tableView.computeIfAbsent(partition, k -> new HashMap<>()).get(bucket);
        if (lookupLevels == null) {
            Preconditions.checkArgument(
                    beforeFiles.isEmpty(),
                    "The before file should be empty for the initial phase.");
            newLookupLevels(partition, bucket, dataFiles);
        } else {
            lookupLevels.getLevels().update(beforeFiles, dataFiles);
        }
    }

    private void newLookupLevels(BinaryRow partition, int bucket, List<DataFileMeta> dataFiles) {
        Levels levels = new Levels(keyComparatorSupplier.get(), dataFiles, options.numLevels());
        KeyValueFileReaderFactory factory = readerFactoryBuilder.build(partition, bucket);
        LookupLevels lookupLevels =
                new LookupLevels(
                        levels,
                        keyComparatorSupplier.get(),
                        readerFactoryBuilder.keyType(),
                        readerFactoryBuilder.projectedValueType(),
                        file ->
                                factory.createRecordReader(
                                        file.schemaId(),
                                        file.fileName(),
                                        file.fileSize(),
                                        file.level()),
                        () ->
                                Preconditions.checkNotNull(ioManager, "IOManager is required.")
                                        .createChannel()
                                        .getPathFile(),
                        hashLookupStoreFactory,
                        options.toConfiguration().get(CoreOptions.LOOKUP_CACHE_FILE_RETENTION),
                        options.toConfiguration().get(CoreOptions.LOOKUP_CACHE_MAX_DISK_SIZE));
        tableView.computeIfAbsent(partition, k -> new HashMap<>()).put(bucket, lookupLevels);
    }

    @Nullable
    @Override
    public InternalRow lookup(BinaryRow partition, int bucket, InternalRow key) throws IOException {
        Map<Integer, LookupLevels> buckets = tableView.get(partition);
        if (buckets == null || buckets.isEmpty()) {
            return null;
        }
        LookupLevels lookupLevels = buckets.get(bucket);
        if (lookupLevels == null) {
            return null;
        }

        // lookup start from level 1.
        KeyValue kv = lookupLevels.lookup(key, 1);
        if (kv == null || kv.valueKind().isRetract()) {
            return null;
        } else {
            return kv.value();
        }
    }

    @Override
    public TableQuery withValueProjection(int[][] projection) {
        this.readerFactoryBuilder.withValueProjection(projection);
        return this;
    }

    @Override
    public TableQuery withIOManager(IOManager ioManager) {
        this.ioManager = ioManager;
        return this;
    }

    @Override
    public void close() throws IOException {
        for (Map.Entry<BinaryRow, Map<Integer, LookupLevels>> buckets : tableView.entrySet()) {
            for (Map.Entry<Integer, LookupLevels> bucket : buckets.getValue().entrySet()) {
                bucket.getValue().close();
            }
        }
    }
}
