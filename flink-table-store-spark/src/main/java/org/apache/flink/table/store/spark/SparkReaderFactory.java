/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.store.spark;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.store.file.utils.RecordReader;
import org.apache.flink.table.store.file.utils.RecordReaderIterator;
import org.apache.flink.table.store.table.FileStoreTable;
import org.apache.flink.table.store.table.source.Split;
import org.apache.flink.table.store.utils.TypeUtils;
import org.apache.flink.table.types.logical.RowType;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;

import java.io.IOException;
import java.io.UncheckedIOException;

/** A Spark {@link PartitionReaderFactory} for table store. */
public class SparkReaderFactory implements PartitionReaderFactory {

    private static final long serialVersionUID = 1L;

    private final FileStoreTable table;
    private final int[] projectedFields;

    public SparkReaderFactory(FileStoreTable table, int[] projectedFields) {
        this.table = table;
        this.projectedFields = projectedFields;
    }

    private RowType readRowType() {
        return TypeUtils.project(table.rowType(), projectedFields);
    }

    @Override
    public PartitionReader<InternalRow> createReader(InputPartition partition) {
        Split split = ((SparkInputPartition) partition).split();
        RecordReader<RowData> reader;
        try {
            reader =
                    table.newRead(false)
                            .withProjection(projectedFields)
                            .createReader(split.partition(), split.bucket(), split.files());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        RecordReaderIterator<RowData> iterator = new RecordReaderIterator<>(reader);
        SparkInternalRow row = new SparkInternalRow(readRowType());
        return new PartitionReader<InternalRow>() {

            @Override
            public boolean next() {
                if (iterator.hasNext()) {
                    row.replace(iterator.next());
                    return true;
                }
                return false;
            }

            @Override
            public InternalRow get() {
                return row;
            }

            @Override
            public void close() throws IOException {
                try {
                    iterator.close();
                } catch (Exception e) {
                    throw new IOException(e);
                }
            }
        };
    }
}
