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

import org.apache.flink.table.store.data.InternalRow;
import org.apache.flink.table.store.file.operation.Lock;
import org.apache.flink.table.store.file.predicate.Predicate;
import org.apache.flink.table.store.file.predicate.PredicateFilter;
import org.apache.flink.table.store.reader.RecordReader;
import org.apache.flink.table.store.reader.RecordReaderIterator;
import org.apache.flink.table.store.table.sink.BucketComputer;
import org.apache.flink.table.store.table.sink.TableCommit;
import org.apache.flink.table.store.table.sink.TableWrite;
import org.apache.flink.table.store.table.source.Split;
import org.apache.flink.table.store.types.RowKind;

import java.util.List;

/** An interface for {@link Table} write support. */
public interface SupportsWrite extends Table {

    BucketComputer bucketComputer();

    TableWrite newWrite(String commitUser);

    TableCommit newCommit(String commitUser);

    default void deleteWhere(String commitUser, List<Predicate> filters, Lock.Factory lockFactory) {
        List<Split> splits = newScan().withFilter(filters).plan().splits();
        try (RecordReader<InternalRow> reader = newRead().withFilter(filters).createReader(splits);
                TableWrite write = newWrite(commitUser);
                TableCommit commit = newCommit(commitUser).withLock(lockFactory.create())) {
            RecordReaderIterator<InternalRow> iterator = new RecordReaderIterator<>(reader);
            PredicateFilter filter = new PredicateFilter(rowType(), filters);
            while (iterator.hasNext()) {
                InternalRow row = iterator.next();
                if (filter.test(row)) {
                    row.setRowKind(RowKind.DELETE);
                    write.write(row);
                }
            }

            commit.commit(0, write.prepareCommit(true, 0));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
