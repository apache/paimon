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

package org.apache.flink.table.store.connector;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.store.connector.utils.TableStoreUtils;
import org.apache.flink.table.store.file.FileStore;
import org.apache.flink.table.store.file.FileStoreImpl;
import org.apache.flink.table.store.file.mergetree.compact.Accumulator;
import org.apache.flink.table.store.file.mergetree.compact.DeduplicateAccumulator;
import org.apache.flink.table.store.file.mergetree.compact.ValueCountAccumulator;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.apache.flink.table.store.connector.utils.TableStoreUtils.filterFileStoreOptions;
import static org.apache.flink.table.store.connector.utils.TableStoreUtils.tablePath;
import static org.apache.flink.table.store.file.FileStoreOptions.BUCKET;
import static org.apache.flink.table.store.file.FileStoreOptions.TABLE_PATH;

/**
 * Table context to help create {@link
 * org.apache.flink.table.store.connector.source.StoreTableSource} and {@link
 * org.apache.flink.table.store.connector.sink.StoreTableSink}.
 */
public class StoreTableContext {

    private final DynamicTableFactory.Context context;
    private final boolean batchMode;
    private final boolean enableChangeTracking;
    private final ObjectIdentifier tableIdentifier;
    private final int numBucket;

    private RowType rowType;
    private List<String> partitionKeys;
    private RowType partitionType;
    private int[] partitionIndex;
    private int[] primaryKeyIndex;
    private FileStore fileStore;

    public StoreTableContext(DynamicTableFactory.Context context) {
        this.context = context;
        batchMode =
                context.getConfiguration().get(ExecutionOptions.RUNTIME_MODE)
                        == RuntimeExecutionMode.BATCH;
        enableChangeTracking =
                TableStoreUtils.enableChangeTracking(context.getCatalogTable().getOptions());
        if (batchMode && enableChangeTracking) {
            throw new TableException("Change tracking is not supported under batch mode.");
        }
        tableIdentifier = context.getObjectIdentifier();
        numBucket =
                Integer.parseInt(
                        context.getCatalogTable()
                                .getOptions()
                                .getOrDefault(BUCKET.key(), BUCKET.defaultValue().toString()));
        initialize();
    }

    public DynamicTableFactory.Context getContext() {
        return context;
    }

    public boolean batchMode() {
        return batchMode;
    }

    public boolean enableChangeTracking() {
        return enableChangeTracking;
    }

    public ObjectIdentifier tableIdentifier() {
        return tableIdentifier;
    }

    public FileStore fileStore() {
        return fileStore;
    }

    public int[] partitionIndex() {
        return partitionIndex;
    }

    public List<String> getPartitionKeys() {
        return partitionKeys;
    }

    public int[] primaryKeyIndex() {
        return primaryKeyIndex;
    }

    public RowType getRowType() {
        return rowType;
    }

    public RowType getPartitionType() {
        return partitionType;
    }

    public ChangelogMode getChangelogMode() {
        return batchMode || primaryKeyIndex.length > 0
                ? ChangelogMode.insertOnly()
                : ChangelogMode.all();
    }

    public int numBucket() {
        return numBucket;
    }

    // ~ Tools ------------------------------------------------------------------

    private void initialize() {
        rowType =
                (RowType)
                        context.getCatalogTable()
                                .getResolvedSchema()
                                .toPhysicalRowDataType()
                                .getLogicalType();

        partitionKeys = context.getCatalogTable().getPartitionKeys();
        RowType.RowField[] fields = new RowType.RowField[partitionKeys.size()];
        partitionIndex = new int[partitionKeys.size()];
        List<String> fieldNames = rowType.getFieldNames();
        for (int i = 0; i < partitionKeys.size(); i++) {
            int position = fieldNames.indexOf(partitionKeys.get(i));
            partitionIndex[i] = position;
            fields[i] = rowType.getFields().get(position);
        }
        partitionType = new RowType(Arrays.asList(fields));

        primaryKeyIndex = context.getCatalogTable().getResolvedSchema().getPrimaryKeyIndexes();
        RowType keyType = primaryKeyIndex.length > 0 ? getType(rowType, primaryKeyIndex) : rowType;
        RowType valueType =
                primaryKeyIndex.length > 0
                        ? rowType
                        : new RowType(
                                Collections.singletonList(
                                        new RowType.RowField("COUNT", new BigIntType(false))));

        Accumulator accumulator =
                primaryKeyIndex.length > 0
                        ? new DeduplicateAccumulator()
                        : new ValueCountAccumulator();
        Map<String, String> options = context.getCatalogTable().getOptions();
        options.put(TABLE_PATH.key(), tablePath(options, context.getObjectIdentifier()).getPath());
        fileStore =
                new FileStoreImpl(
                        Configuration.fromMap(filterFileStoreOptions(options)),
                        UUID.randomUUID().toString(),
                        partitionType,
                        keyType,
                        valueType,
                        accumulator);
    }

    private static RowType getType(RowType rowType, int[] index) {
        RowType.RowField[] fields = new RowType.RowField[index.length];
        int i = 0;
        for (int idx : index) {
            fields[i++] = rowType.getFields().get(idx);
        }
        return new RowType(Arrays.asList(fields));
    }
}
