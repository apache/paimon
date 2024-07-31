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

package org.apache.paimon.table.query;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.FileStore;
import org.apache.paimon.KeyValue;
import org.apache.paimon.KeyValueFileStore;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.data.serializer.InternalSerializers;
import org.apache.paimon.data.serializer.RowCompactedSerializer;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.KeyValueFileReaderFactory;
import org.apache.paimon.io.cache.CacheManager;
import org.apache.paimon.lookup.LookupStoreFactory;
import org.apache.paimon.mergetree.Levels;
import org.apache.paimon.mergetree.LookupFile;
import org.apache.paimon.mergetree.LookupLevels;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.InternalRowPartitionComputer;
import org.apache.paimon.utils.KeyComparatorSupplier;
import org.apache.paimon.utils.Preconditions;

import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Cache;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.apache.paimon.CoreOptions.MergeEngine.DEDUPLICATE;
import static org.apache.paimon.lookup.LookupStoreFactory.bfGenerator;

/** Implementation for {@link TableQuery} for caching data and file in local. */
public class LocalTableQuery implements TableQuery {

    private final Map<BinaryRow, Map<Integer, LookupLevels<KeyValue>>> tableView;

    private final CoreOptions options;

    private final Supplier<Comparator<InternalRow>> keyComparatorSupplier;

    private final KeyValueFileReaderFactory.Builder readerFactoryBuilder;

    private final LookupStoreFactory lookupStoreFactory;

    private final int startLevel;

    private IOManager ioManager;

    @Nullable private Cache<String, LookupFile> lookupFileCache;

    private final RowType partitionType;

    public LocalTableQuery(FileStoreTable table) {
        this.options = table.coreOptions();
        this.tableView = new HashMap<>();
        FileStore<?> tableStore = table.store();
        if (!(tableStore instanceof KeyValueFileStore)) {
            throw new UnsupportedOperationException(
                    "Table Query only supports table with primary key.");
        }
        KeyValueFileStore store = (KeyValueFileStore) tableStore;

        this.readerFactoryBuilder = store.newReaderFactoryBuilder();
        this.partitionType = table.schema().logicalPartitionType();
        RowType keyType = readerFactoryBuilder.keyType();
        this.keyComparatorSupplier = new KeyComparatorSupplier(readerFactoryBuilder.keyType());
        this.lookupStoreFactory =
                LookupStoreFactory.create(
                        options,
                        new CacheManager(options.lookupCacheMaxMemory()),
                        new RowCompactedSerializer(keyType).createSliceComparator());

        if (options.needLookup()) {
            startLevel = 1;
        } else {
            if (options.sequenceField().size() > 0) {
                throw new UnsupportedOperationException(
                        "Not support sequence field definition, but is: "
                                + options.sequenceField());
            }

            if (options.mergeEngine() != DEDUPLICATE) {
                throw new UnsupportedOperationException(
                        "Only support deduplicate merge engine, but is: " + options.mergeEngine());
            }

            startLevel = 0;
        }
    }

    public void refreshFiles(
            BinaryRow partition,
            int bucket,
            List<DataFileMeta> beforeFiles,
            List<DataFileMeta> dataFiles) {
        LookupLevels<KeyValue> lookupLevels =
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
        // TODO pass DeletionVector factory
        KeyValueFileReaderFactory factory =
                readerFactoryBuilder.build(partition, bucket, DeletionVector.emptyFactory());
        Options options = this.options.toConfiguration();
        if (lookupFileCache == null) {
            lookupFileCache =
                    LookupFile.createCache(
                            options.get(CoreOptions.LOOKUP_CACHE_FILE_RETENTION),
                            options.get(CoreOptions.LOOKUP_CACHE_MAX_DISK_SIZE));
        }

        LookupLevels<KeyValue> lookupLevels =
                new LookupLevels<>(
                        levels,
                        keyComparatorSupplier.get(),
                        readerFactoryBuilder.keyType(),
                        new LookupLevels.KeyValueProcessor(
                                readerFactoryBuilder.projectedValueType()),
                        file ->
                                factory.createRecordReader(
                                        file.schemaId(),
                                        file.fileName(),
                                        file.fileSize(),
                                        file.level()),
                        file ->
                                Preconditions.checkNotNull(ioManager, "IOManager is required.")
                                        .createChannel(
                                                LookupFile.localFilePrefix(
                                                        InternalRowPartitionComputer
                                                                .paritionToString(
                                                                        partitionType,
                                                                        partition,
                                                                        "-"),
                                                        bucket,
                                                        file,
                                                        100))
                                        .getPathFile(),
                        lookupStoreFactory,
                        bfGenerator(options),
                        lookupFileCache);

        tableView.computeIfAbsent(partition, k -> new HashMap<>()).put(bucket, lookupLevels);
    }

    /** TODO remove synchronized and supports multiple thread to lookup. */
    @Nullable
    @Override
    public synchronized InternalRow lookup(BinaryRow partition, int bucket, InternalRow key)
            throws IOException {
        Map<Integer, LookupLevels<KeyValue>> buckets = tableView.get(partition);
        if (buckets == null || buckets.isEmpty()) {
            return null;
        }
        LookupLevels<KeyValue> lookupLevels = buckets.get(bucket);
        if (lookupLevels == null) {
            return null;
        }

        KeyValue kv = lookupLevels.lookup(key, startLevel);
        if (kv == null || kv.valueKind().isRetract()) {
            return null;
        } else {
            return kv.value();
        }
    }

    @Override
    public LocalTableQuery withValueProjection(int[][] projection) {
        this.readerFactoryBuilder.withValueProjection(projection);
        return this;
    }

    public LocalTableQuery withIOManager(IOManager ioManager) {
        this.ioManager = ioManager;
        return this;
    }

    @Override
    public InternalRowSerializer createValueSerializer() {
        return InternalSerializers.create(readerFactoryBuilder.projectedValueType());
    }

    @Override
    public void close() throws IOException {
        for (Map.Entry<BinaryRow, Map<Integer, LookupLevels<KeyValue>>> buckets :
                tableView.entrySet()) {
            for (Map.Entry<Integer, LookupLevels<KeyValue>> bucket :
                    buckets.getValue().entrySet()) {
                bucket.getValue().close();
            }
        }
        if (lookupFileCache != null) {
            lookupFileCache.invalidateAll();
        }
        tableView.clear();
    }
}
