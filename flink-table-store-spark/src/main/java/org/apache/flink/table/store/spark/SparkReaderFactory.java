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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.store.file.predicate.Predicate;
import org.apache.flink.table.store.file.utils.RecordReader;
import org.apache.flink.table.store.file.utils.RecordReaderIterator;
import org.apache.flink.table.store.filesystem.FileSystems;
import org.apache.flink.table.store.table.Table;
import org.apache.flink.table.store.table.source.TableRead;
import org.apache.flink.table.store.utils.TypeUtils;
import org.apache.flink.table.types.logical.RowType;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;

import static org.apache.flink.table.store.file.predicate.PredicateBuilder.and;

/** A Spark {@link PartitionReaderFactory} for table store. */
public class SparkReaderFactory implements PartitionReaderFactory {

    private static final long serialVersionUID = 1L;

    private final Table table;
    private final int[] projectedFields;
    private final List<Predicate> predicates;
    private final Configuration conf;

    public SparkReaderFactory(
            Table table, int[] projectedFields, List<Predicate> predicates, Configuration conf) {
        this.table = table;
        this.projectedFields = projectedFields;
        this.predicates = predicates;
        this.conf = conf;
    }

    private RowType readRowType() {
        return TypeUtils.project(table.rowType(), projectedFields);
    }

    @Override
    public PartitionReader<InternalRow> createReader(InputPartition partition) {
        RecordReader<RowData> reader;
        TableRead read = table.newRead().withProjection(projectedFields);
        if (predicates.size() > 0) {
            read.withFilter(and(predicates));
        }
        try {
            reader = read.createReader(((SparkInputPartition) partition).split());
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

    private void readObject(java.io.ObjectInputStream in)
            throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        // TODO move file system initialization into common modules
        FileSystems.initialize(table.location(), conf);
    }
}
