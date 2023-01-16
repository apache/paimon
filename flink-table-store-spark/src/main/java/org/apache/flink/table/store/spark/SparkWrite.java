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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.store.file.operation.Lock;
import org.apache.flink.table.store.filesystem.FileSystems;
import org.apache.flink.table.store.table.SupportsWrite;
import org.apache.flink.table.store.table.sink.BucketComputer;
import org.apache.flink.table.store.table.sink.FileCommittable;
import org.apache.flink.table.store.table.sink.SerializableCommittable;
import org.apache.flink.table.store.table.sink.TableCommit;
import org.apache.flink.table.store.table.sink.TableWrite;
import org.apache.flink.table.store.types.RowType;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.connector.write.V1Write;
import org.apache.spark.sql.sources.InsertableRelation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/** Spark {@link V1Write}, it is required to use v1 write for grouping by bucket. */
public class SparkWrite implements V1Write {

    private final SupportsWrite table;
    private final String queryId;
    private final Lock.Factory lockFactory;
    private final Configuration conf;

    public SparkWrite(
            SupportsWrite table, String queryId, Lock.Factory lockFactory, Configuration conf) {
        this.table = table;
        this.queryId = queryId;
        this.lockFactory = lockFactory;
        this.conf = conf;
    }

    @Override
    public InsertableRelation toInsertableRelation() {
        return (data, overwrite) -> {
            if (overwrite) {
                throw new UnsupportedOperationException("Overwrite is unsupported.");
            }

            long identifier = 0;
            List<SerializableCommittable> committables =
                    data.toJavaRDD()
                            .groupBy(new ComputeBucket(table, conf))
                            .mapValues(new WriteRecords(table, queryId, identifier, conf))
                            .values()
                            .reduce(new ListConcat<>());
            try (TableCommit tableCommit =
                    table.newCommit(queryId).withLock(lockFactory.create())) {
                tableCommit.commit(
                        identifier,
                        committables.stream()
                                .map(SerializableCommittable::delegate)
                                .collect(Collectors.toList()));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
    }

    private static class ComputeBucket implements Function<Row, Integer> {

        private final SupportsWrite table;
        private final RowType type;
        private final Configuration conf;

        private transient BucketComputer lazyComputer;

        private ComputeBucket(SupportsWrite table, Configuration conf) {
            this.table = table;
            this.type = table.rowType();
            this.conf = conf;
        }

        private BucketComputer computer() {
            if (lazyComputer == null) {
                lazyComputer = table.bucketComputer();
            }
            return lazyComputer;
        }

        @Override
        public Integer call(Row row) {
            return computer().bucket(new SparkRow(type, row));
        }

        private void readObject(java.io.ObjectInputStream in)
                throws IOException, ClassNotFoundException {
            in.defaultReadObject();
            // TODO move file system initialization into common modules
            FileSystems.initialize(table.location(), conf);
        }
    }

    private static class WriteRecords
            implements Function<Iterable<Row>, List<SerializableCommittable>> {

        private final SupportsWrite table;
        private final RowType type;
        private final String queryId;
        private final long commitIdentifier;
        private final Configuration conf;

        private WriteRecords(
                SupportsWrite table, String queryId, long commitIdentifier, Configuration conf) {
            this.table = table;
            this.type = table.rowType();
            this.queryId = queryId;
            this.commitIdentifier = commitIdentifier;
            this.conf = conf;
        }

        @Override
        public List<SerializableCommittable> call(Iterable<Row> iterables) throws Exception {
            try (TableWrite write = table.newWrite(queryId)) {
                for (Row row : iterables) {
                    write.write(new SparkRow(type, row));
                }
                List<FileCommittable> committables = write.prepareCommit(true, commitIdentifier);
                return committables.stream()
                        .map(SerializableCommittable::wrap)
                        .collect(Collectors.toList());
            }
        }

        private void readObject(java.io.ObjectInputStream in)
                throws IOException, ClassNotFoundException {
            in.defaultReadObject();
            // TODO move file system initialization into common modules
            FileSystems.initialize(table.location(), conf);
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
