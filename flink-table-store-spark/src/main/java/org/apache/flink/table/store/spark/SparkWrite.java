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

package org.apache.flink.table.store.spark;

import org.apache.flink.table.store.file.operation.Lock;
import org.apache.flink.table.store.table.Table;
import org.apache.flink.table.store.table.sink.BatchTableCommit;
import org.apache.flink.table.store.table.sink.BatchTableWrite;
import org.apache.flink.table.store.table.sink.BatchWriteBuilder;
import org.apache.flink.table.store.table.sink.CommitMessage;
import org.apache.flink.table.store.table.sink.InnerTableCommit;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.connector.write.V1Write;
import org.apache.spark.sql.sources.InsertableRelation;

import java.util.ArrayList;
import java.util.List;

/** Spark {@link V1Write}, it is required to use v1 write for grouping by bucket. */
public class SparkWrite implements V1Write {

    private final Table table;
    private final Lock.Factory lockFactory;

    public SparkWrite(Table table, Lock.Factory lockFactory) {
        this.table = table;
        this.lockFactory = lockFactory;
    }

    @Override
    public InsertableRelation toInsertableRelation() {
        return (data, overwrite) -> {
            if (overwrite) {
                throw new UnsupportedOperationException("Overwrite is unsupported.");
            }

            BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
            List<CommitMessage> committables =
                    data.toJavaRDD()
                            .groupBy(new ComputeBucket(writeBuilder))
                            .mapValues(new WriteRecords(writeBuilder))
                            .values()
                            .reduce(new ListConcat<>());
            try (BatchTableCommit tableCommit =
                    ((InnerTableCommit) writeBuilder.newCommit()).withLock(lockFactory.create())) {
                tableCommit.commit(committables);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
    }

    private static class ComputeBucket implements Function<Row, Integer> {

        private final BatchWriteBuilder writeBuilder;

        private transient BatchTableWrite lazyWriter;

        private ComputeBucket(BatchWriteBuilder writeBuilder) {
            this.writeBuilder = writeBuilder;
        }

        private BatchTableWrite computer() {
            if (lazyWriter == null) {
                lazyWriter = writeBuilder.newWrite();
            }
            return lazyWriter;
        }

        @Override
        public Integer call(Row row) {
            return computer().getBucket(new SparkRow(writeBuilder.rowType(), row));
        }
    }

    private static class WriteRecords implements Function<Iterable<Row>, List<CommitMessage>> {

        private final BatchWriteBuilder writeBuilder;

        private WriteRecords(BatchWriteBuilder writeBuilder) {
            this.writeBuilder = writeBuilder;
        }

        @Override
        public List<CommitMessage> call(Iterable<Row> iterables) throws Exception {
            try (BatchTableWrite write = writeBuilder.newWrite()) {
                for (Row row : iterables) {
                    write.write(new SparkRow(writeBuilder.rowType(), row));
                }
                return write.prepareCommit();
            }
        }
    }

    private static class ListConcat<T> implements Function2<List<T>, List<T>, List<T>> {

        @Override
        public List<T> call(List<T> v1, List<T> v2) {
            List<T> ret = new ArrayList<>();
            ret.addAll(v1);
            ret.addAll(v2);
            return ret;
        }
    }
}
