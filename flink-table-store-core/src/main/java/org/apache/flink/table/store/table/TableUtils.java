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
import org.apache.flink.table.store.table.sink.BatchTableCommit;
import org.apache.flink.table.store.table.sink.BatchTableWrite;
import org.apache.flink.table.store.table.sink.BatchWriteBuilder;
import org.apache.flink.table.store.table.sink.InnerTableCommit;
import org.apache.flink.table.store.table.source.ReadBuilder;
import org.apache.flink.table.store.table.source.Split;
import org.apache.flink.table.store.types.RowKind;
import org.apache.flink.table.store.utils.CloseableIterator;

import java.util.List;

/** Utils for Table. TODO we can introduce LocalAction maybe? */
public class TableUtils {

    /**
     * Delete according to filters.
     *
     * <p>NOTE: This method is only suitable for deletion of small amount of data.
     */
    public static void deleteWhere(Table table, List<Predicate> filters, Lock.Factory lockFactory) {
        ReadBuilder readBuilder = table.newReadBuilder().withFilter(filters);
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        List<Split> splits = readBuilder.newScan().plan().splits();
        try (RecordReader<InternalRow> reader = readBuilder.newRead().createReader(splits);
                BatchTableWrite write = writeBuilder.newWrite();
                BatchTableCommit commit =
                        ((InnerTableCommit) writeBuilder.newCommit())
                                .withLock(lockFactory.create())) {
            CloseableIterator<InternalRow> iterator = reader.toCloseableIterator();
            PredicateFilter filter = new PredicateFilter(table.rowType(), filters);
            while (iterator.hasNext()) {
                InternalRow row = iterator.next();
                if (filter.test(row)) {
                    row.setRowKind(RowKind.DELETE);
                    write.write(row);
                }
            }

            commit.commit(write.prepareCommit());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
