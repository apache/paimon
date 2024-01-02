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
import org.apache.paimon.KeyValue;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.format.FileFormatDiscover;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.KeyValueFileReaderFactory;
import org.apache.paimon.io.cache.CacheManager;
import org.apache.paimon.lookup.hash.HashLookupStoreFactory;
import org.apache.paimon.mergetree.Levels;
import org.apache.paimon.mergetree.LookupLevels;
import org.apache.paimon.schema.KeyValueFieldsExtractor;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.PrimaryKeyTableUtils;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.KeyComparatorSupplier;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.Projection;

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

    private final RowType keyType;

    private RowType projectedValueType;

    private final KeyValueFileReaderFactory.Builder readerFactoryBuilder;

    private IOManager ioManager;

    private final HashLookupStoreFactory hashLookupStoreFactory;

    public TableQueryImpl(FileStoreTable table) {
        this.options = new CoreOptions(table.options());
        this.tableView = new HashMap<>();
        KeyValueFieldsExtractor pkExtractor =
                PrimaryKeyTableUtils.PrimaryKeyFieldsExtractor.EXTRACTOR;
        this.keyType = new RowType(pkExtractor.keyFields(table.schema()));
        CacheManager cacheManager =
                new CacheManager(
                        options.pageSize(),
                        options.toConfiguration().get(LOOKUP_CACHE_MAX_MEMORY_SIZE));
        RowType partitionType = table.schema().logicalPartitionType();
        this.readerFactoryBuilder =
                KeyValueFileReaderFactory.builder(
                        table.fileIO(),
                        new SchemaManager(table.fileIO(), table.location()),
                        table.schema().id(),
                        keyType,
                        table.rowType(),
                        FileFormatDiscover.of(options),
                        new FileStorePathFactory(
                                options.path(),
                                partitionType,
                                options.partitionDefaultName(),
                                options.fileFormat().getFormatIdentifier()),
                        PrimaryKeyTableUtils.PrimaryKeyFieldsExtractor.EXTRACTOR,
                        options);
        this.projectedValueType = readerFactoryBuilder.projectedValueType();
        this.keyComparatorSupplier = new KeyComparatorSupplier(keyType);
        this.hashLookupStoreFactory =
                new HashLookupStoreFactory(
                        cacheManager,
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
        Map<Integer, LookupLevels> buckets =
                tableView.computeIfAbsent(partition, k -> new HashMap<>());
        LookupLevels lookupLevels = buckets.get(bucket);

        if (lookupLevels == null) {
            Preconditions.checkArgument(
                    beforeFiles.isEmpty(),
                    "The before file should be empty for the initial phase.");
            Levels levels = new Levels(keyComparatorSupplier.get(), dataFiles, options.numLevels());

            KeyValueFileReaderFactory factory = readerFactoryBuilder.build(partition, bucket);

            lookupLevels =
                    new LookupLevels(
                            levels,
                            keyComparatorSupplier.get(),
                            keyType,
                            projectedValueType,
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
            buckets.put(bucket, lookupLevels);
        } else {
            lookupLevels.getLevels().update(beforeFiles, dataFiles);
        }
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
        } else {
            // lookup start from level 1.
            KeyValue kv = lookupLevels.lookup(key, 1);
            if (kv == null || kv.valueKind().isRetract()) {
                return null;
            } else {
                return kv.value();
            }
        }
    }

    @Override
    public TableQuery withValueProjection(int[][] projection) {
        this.readerFactoryBuilder.withValueProjection(Projection.of(projection).toNestedIndexes());
        this.projectedValueType = readerFactoryBuilder.projectedValueType();
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
