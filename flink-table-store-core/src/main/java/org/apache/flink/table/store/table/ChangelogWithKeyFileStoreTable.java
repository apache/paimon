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

package org.apache.flink.table.store.table;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.store.file.FileStore;
import org.apache.flink.table.store.file.FileStoreImpl;
import org.apache.flink.table.store.file.FileStoreOptions;
import org.apache.flink.table.store.file.KeyValue;
import org.apache.flink.table.store.file.WriteMode;
import org.apache.flink.table.store.file.mergetree.compact.DeduplicateMergeFunction;
import org.apache.flink.table.store.file.mergetree.compact.MergeFunction;
import org.apache.flink.table.store.file.mergetree.compact.PartialUpdateMergeFunction;
import org.apache.flink.table.store.file.operation.FileStoreRead;
import org.apache.flink.table.store.file.predicate.Predicate;
import org.apache.flink.table.store.file.schema.Schema;
import org.apache.flink.table.store.table.source.TableRead;
import org.apache.flink.table.store.table.source.TableScan;
import org.apache.flink.table.store.table.source.ValueContentRowDataIterator;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

/** {@link FileStoreTable} for {@link WriteMode#CHANGE_LOG} write mode with primary keys. */
public class ChangelogWithKeyFileStoreTable implements FileStoreTable {

    private final Schema schema;
    private final boolean isStreaming;
    private final FileStoreImpl store;

    ChangelogWithKeyFileStoreTable(
            Schema schema, boolean isStreaming, Configuration conf, String user) {
        this.schema = schema;
        this.isStreaming = isStreaming;

        RowType rowType = schema.logicalRowType();

        // add _KEY_ prefix to avoid conflict with value
        RowType keyType =
                new RowType(
                        schema.logicalPrimaryKeysType().getFields().stream()
                                .map(
                                        f ->
                                                new RowType.RowField(
                                                        "_KEY_" + f.getName(),
                                                        f.getType(),
                                                        f.getDescription().orElse(null)))
                                .collect(Collectors.toList()));

        FileStoreOptions.MergeEngine mergeEngine = conf.get(FileStoreOptions.MERGE_ENGINE);
        MergeFunction mergeFunction;
        switch (mergeEngine) {
            case DEDUPLICATE:
                mergeFunction = new DeduplicateMergeFunction();
                break;
            case PARTIAL_UPDATE:
                List<LogicalType> fieldTypes = rowType.getChildren();
                RowData.FieldGetter[] fieldGetters = new RowData.FieldGetter[fieldTypes.size()];
                for (int i = 0; i < fieldTypes.size(); i++) {
                    fieldGetters[i] = RowData.createFieldGetter(fieldTypes.get(i), i);
                }
                mergeFunction = new PartialUpdateMergeFunction(fieldGetters);
                break;
            default:
                throw new UnsupportedOperationException("Unsupported merge engine: " + mergeEngine);
        }

        this.store =
                new FileStoreImpl(
                        schema.id(),
                        new FileStoreOptions(conf),
                        WriteMode.CHANGE_LOG,
                        user,
                        schema.logicalPartitionType(),
                        keyType,
                        rowType,
                        mergeFunction);
    }

    @Override
    public TableScan newScan() {
        return new TableScan(store.newScan(), schema, store.pathFactory()) {
            @Override
            protected void withNonPartitionFilter(Predicate predicate) {
                scan.withValueFilter(predicate);
            }
        };
    }

    @Override
    public TableRead newRead() {
        FileStoreRead read = store.newRead();
        if (isStreaming) {
            read.withDropDelete(false);
        }

        return new TableRead(read) {
            @Override
            protected void withProjectionImpl(int[][] projection) {
                read.withValueProjection(projection);
            }

            @Override
            protected Iterator<RowData> rowDataIteratorFromKv(KeyValue kv) {
                return new ValueContentRowDataIterator(kv);
            }
        };
    }

    @Override
    public FileStore fileStore() {
        return store;
    }
}
