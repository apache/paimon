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

package org.apache.paimon.spark;

import org.apache.paimon.operation.Lock;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageSerializer;
import org.apache.paimon.table.sink.InnerTableCommit;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.connector.write.V1Write;
import org.apache.spark.sql.sources.InsertableRelation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

/** Spark {@link V1Write}, it is required to use v1 write for grouping by bucket. */
public class SparkWrite implements V1Write {

    private final Table table;
    private final Lock.Factory lockFactory;

    private final CommitMessageSerializer serializer = new CommitMessageSerializer();

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
                    data.toJavaRDD().groupBy(new ComputeBucket(writeBuilder))
                            .mapValues(new WriteRecords(writeBuilder)).values()
                            .mapPartitions(SparkWrite::serializeCommitMessages).collect().stream()
                            .map(this::deserializeCommitMessage)
                            .collect(Collectors.toList());
            try (BatchTableCommit tableCommit =
                    ((InnerTableCommit) writeBuilder.newCommit()).withLock(lockFactory.create())) {
                tableCommit.commit(committables);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
    }

    private static Iterator<byte[]> serializeCommitMessages(Iterator<List<CommitMessage>> iters) {
        List<byte[]> serialized = new ArrayList<>();
        CommitMessageSerializer innerSerializer = new CommitMessageSerializer();
        while (iters.hasNext()) {
            List<CommitMessage> commitMessages = iters.next();
            for (CommitMessage commitMessage : commitMessages) {
                try {
                    serialized.add(innerSerializer.serialize(commitMessage));
                } catch (IOException e) {
                    throw new RuntimeException("Failed to serialize CommitMessage's object", e);
                }
            }
        }
        return serialized.iterator();
    }

    private CommitMessage deserializeCommitMessage(byte[] bytes) {
        try {
            return serializer.deserialize(serializer.getVersion(), bytes);
        } catch (IOException e) {
            throw new RuntimeException("Failed to deserialize CommitMessage's object", e);
        }
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
