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

package org.apache.flink.table.store.spark;

import org.apache.flink.table.store.data.InternalRow;
import org.apache.flink.table.store.file.predicate.Predicate;
import org.apache.flink.table.store.file.utils.RecordReader;
import org.apache.flink.table.store.file.utils.RecordReaderIterator;
import org.apache.flink.table.store.table.Table;
import org.apache.flink.table.store.table.source.Split;
import org.apache.flink.table.store.table.source.TableRead;
import org.apache.flink.table.store.types.RowType;
import org.apache.flink.table.store.utils.TypeUtils;

import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;

import static org.apache.flink.table.store.file.predicate.PredicateBuilder.and;

/** A Spark {@link InputPartition} for table store. */
public class SparkInputPartition
        implements InputPartition<org.apache.spark.sql.catalyst.InternalRow> {

    private static final long serialVersionUID = 1L;

    private final Table table;
    private final int[] projectedFields;
    private final List<Predicate> predicates;
    private final Split split;

    public SparkInputPartition(
            Table table, int[] projectedFields, List<Predicate> predicates, Split split) {
        this.table = table;
        this.projectedFields = projectedFields;
        this.predicates = predicates;
        this.split = split;
    }

    @Override
    public InputPartitionReader<org.apache.spark.sql.catalyst.InternalRow> createPartitionReader() {
        RecordReader<InternalRow> recordReader;
        try {
            TableRead tableRead = table.newRead();
            if (projectedFields != null) {
                tableRead.withProjection(projectedFields);
            }
            if (predicates.size() > 0) {
                tableRead.withFilter(and(predicates));
            }
            recordReader = tableRead.createReader(split);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        RecordReaderIterator<InternalRow> iterator = new RecordReaderIterator<>(recordReader);
        SparkInternalRow row = new SparkInternalRow(readRowType());
        return new InputPartitionReader<org.apache.spark.sql.catalyst.InternalRow>() {

            @Override
            public boolean next() {
                if (iterator.hasNext()) {
                    row.replace(iterator.next());
                    return true;
                }
                return false;
            }

            @Override
            public org.apache.spark.sql.catalyst.InternalRow get() {
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

    @Override
    public String[] preferredLocations() {
        return new String[0];
    }

    private RowType readRowType() {
        RowType rowType = table.rowType();
        return projectedFields == null ? rowType : TypeUtils.project(rowType, projectedFields);
    }
}
