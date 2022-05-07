/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.store.mapred;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.store.JobConfWrapper;
import org.apache.flink.table.store.RowDataContainer;
import org.apache.flink.table.store.file.FileStore;
import org.apache.flink.table.store.file.FileStoreImpl;
import org.apache.flink.table.store.file.FileStoreOptions;
import org.apache.flink.table.store.file.data.DataFileMeta;
import org.apache.flink.table.store.file.mergetree.compact.DeduplicateMergeFunction;
import org.apache.flink.table.store.file.mergetree.compact.MergeFunction;
import org.apache.flink.table.store.file.mergetree.compact.ValueCountMergeFunction;
import org.apache.flink.table.store.file.operation.FileStoreRead;
import org.apache.flink.table.store.file.operation.FileStoreScan;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * {@link InputFormat} for table store. It divides all files into {@link InputSplit}s (one split per
 * bucket) and creates {@link RecordReader} for each split.
 */
public class TableStoreInputFormat implements InputFormat<Void, RowDataContainer> {

    @Override
    public InputSplit[] getSplits(JobConf jobConf, int numSplits) throws IOException {
        FileStoreImpl store = getFileStore(jobConf);
        FileStoreScan scan = store.newScan();
        List<TableStoreInputSplit> result = new ArrayList<>();
        for (Map.Entry<BinaryRowData, Map<Integer, List<DataFileMeta>>> pe :
                scan.plan().groupByPartFiles().entrySet()) {
            for (Map.Entry<Integer, List<DataFileMeta>> be : pe.getValue().entrySet()) {
                BinaryRowData partition = pe.getKey();
                int bucket = be.getKey();
                String bucketPath =
                        store.pathFactory()
                                .createDataFilePathFactory(partition, bucket)
                                .toPath("")
                                .toString();
                TableStoreInputSplit split =
                        new TableStoreInputSplit(
                                store.partitionType(),
                                store.keyType(),
                                store.valueType(),
                                partition,
                                bucket,
                                be.getValue(),
                                bucketPath);
                result.add(split);
            }
        }
        return result.toArray(new TableStoreInputSplit[0]);
    }

    @Override
    public RecordReader<Void, RowDataContainer> getRecordReader(
            InputSplit inputSplit, JobConf jobConf, Reporter reporter) throws IOException {
        FileStore store = getFileStore(jobConf);
        FileStoreRead read = store.newRead();
        TableStoreInputSplit split = (TableStoreInputSplit) inputSplit;
        org.apache.flink.table.store.file.utils.RecordReader wrapped =
                read.withDropDelete(true)
                        .createReader(split.partition(), split.bucket(), split.files());
        long splitLength = split.getLength();
        return new JobConfWrapper(jobConf).getPrimaryKeyNames().isPresent()
                ? new TableStorePkRecordReader(wrapped, splitLength)
                : new TableStoreCountRecordReader(wrapped, splitLength);
    }

    private FileStoreImpl getFileStore(JobConf jobConf) {
        JobConfWrapper wrapper = new JobConfWrapper(jobConf);

        String catalogName = wrapper.getCatalogName();
        String dbName = wrapper.getDbName();
        String tableName = wrapper.getTableName();
        ObjectIdentifier identifier = ObjectIdentifier.of(catalogName, dbName, tableName);

        Configuration fileStoreOptions = new Configuration();
        fileStoreOptions.setString(FileStoreOptions.PATH, wrapper.getLocation());
        wrapper.updateFileStoreOptions(fileStoreOptions);

        String user = wrapper.getFileStoreUser();

        List<String> columnNames = wrapper.getColumnNames();
        List<LogicalType> columnTypes = wrapper.getColumnTypes();

        List<String> partitionColumnNames = wrapper.getPartitionColumnNames();

        RowType rowType =
                RowType.of(
                        columnTypes.toArray(new LogicalType[0]),
                        columnNames.toArray(new String[0]));
        LogicalType[] partitionLogicalTypes =
                partitionColumnNames.stream()
                        .map(s -> columnTypes.get(columnNames.indexOf(s)))
                        .toArray(LogicalType[]::new);
        RowType partitionType =
                RowType.of(partitionLogicalTypes, partitionColumnNames.toArray(new String[0]));

        // same implementation as org.apache.flink.table.store.connector.TableStore#buildFileStore
        RowType keyType;
        RowType valueType;
        MergeFunction mergeFunction;
        Optional<List<String>> optionalPrimaryKeyNames = wrapper.getPrimaryKeyNames();
        if (optionalPrimaryKeyNames.isPresent()) {
            List<String> primaryKeyNames = optionalPrimaryKeyNames.get();
            LogicalType[] primaryKeyLogicalTypes =
                    primaryKeyNames.stream()
                            .map(
                                    s -> {
                                        int idx = columnNames.indexOf(s);
                                        Preconditions.checkState(
                                                idx >= 0,
                                                "Primary key column "
                                                        + s
                                                        + " not found in table "
                                                        + dbName
                                                        + "."
                                                        + tableName);
                                        return columnTypes.get(idx);
                                    })
                            .toArray(LogicalType[]::new);
            keyType =
                    RowType.of(
                            primaryKeyLogicalTypes,
                            primaryKeyNames.stream().map(s -> "_KEY_" + s).toArray(String[]::new));
            valueType = rowType;
            mergeFunction = new DeduplicateMergeFunction();
        } else {
            keyType = rowType;
            valueType =
                    RowType.of(
                            new LogicalType[] {new BigIntType(false)},
                            new String[] {"_VALUE_COUNT"});
            mergeFunction = new ValueCountMergeFunction();
        }

        return new FileStoreImpl(
                identifier,
                fileStoreOptions,
                user,
                partitionType,
                keyType,
                valueType,
                mergeFunction);
    }
}
