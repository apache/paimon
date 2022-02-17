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

package org.apache.flink.table.store.file.operation;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.store.file.FileFormat;
import org.apache.flink.table.store.file.mergetree.MergeTreeReader;
import org.apache.flink.table.store.file.mergetree.compact.Accumulator;
import org.apache.flink.table.store.file.mergetree.compact.IntervalPartition;
import org.apache.flink.table.store.file.mergetree.sst.SstFileMeta;
import org.apache.flink.table.store.file.mergetree.sst.SstFileReader;
import org.apache.flink.table.store.file.utils.FileStorePathFactory;
import org.apache.flink.table.store.file.utils.RecordReader;
import org.apache.flink.table.types.logical.RowType;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;

/** Default implementation of {@link FileStoreRead}. */
public class FileStoreReadImpl implements FileStoreRead {

    private final SstFileReader.Factory sstFileReaderFactory;
    private final Comparator<RowData> keyComparator;
    private final Accumulator accumulator;

    public FileStoreReadImpl(
            RowType keyType,
            RowType valueType,
            Comparator<RowData> keyComparator,
            Accumulator accumulator,
            FileFormat fileFormat,
            FileStorePathFactory pathFactory) {
        this.sstFileReaderFactory =
                new SstFileReader.Factory(keyType, valueType, fileFormat, pathFactory);
        this.keyComparator = keyComparator;
        this.accumulator = accumulator;
    }

    @Override
    public void withKeyProjection(int[][] projectedFields) {
        // TODO
        throw new UnsupportedOperationException();
    }

    @Override
    public void withValueProjection(int[][] projectedFields) {
        // TODO
        throw new UnsupportedOperationException();
    }

    @Override
    public RecordReader createReader(BinaryRowData partition, int bucket, List<SstFileMeta> files)
            throws IOException {
        return new MergeTreeReader(
                new IntervalPartition(files, keyComparator).partition(),
                true,
                sstFileReaderFactory.create(partition, bucket),
                keyComparator,
                accumulator.copy());
    }
}
