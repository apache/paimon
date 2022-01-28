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

package org.apache.flink.table.store.file.mergetree;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.store.file.FileFormat;
import org.apache.flink.table.store.file.mergetree.compact.Accumulator;
import org.apache.flink.table.store.file.mergetree.sst.SstFile;
import org.apache.flink.table.store.file.utils.FileStorePathFactory;
import org.apache.flink.table.types.logical.RowType;

import java.util.Comparator;
import java.util.concurrent.ExecutorService;

/** Create a new {@link MergeTree} with specific partition and bucket. */
public class MergeTreeFactory {

    private final Comparator<RowData> keyComparator;
    private final Accumulator accumulator;
    private final MergeTreeOptions mergeTreeOptions;
    private final SstFile.Factory sstFileFactory;

    public MergeTreeFactory(
            RowType keyType,
            RowType rowType,
            Comparator<RowData> keyComparator,
            Accumulator accumulator,
            FileFormat fileFormat,
            FileStorePathFactory pathFactory,
            MergeTreeOptions mergeTreeOptions) {
        this.keyComparator = keyComparator;
        this.accumulator = accumulator;
        this.mergeTreeOptions = mergeTreeOptions;
        this.sstFileFactory =
                new SstFile.Factory(
                        keyType, rowType, fileFormat, pathFactory, mergeTreeOptions.targetFileSize);
    }

    public MergeTree create(BinaryRowData partition, int bucket, ExecutorService compactExecutor) {
        return new MergeTree(
                mergeTreeOptions,
                sstFileFactory.create(partition, bucket),
                keyComparator,
                compactExecutor,
                accumulator);
    }
}
