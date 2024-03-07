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

package org.apache.paimon.table;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.utils.CloseableIterator;

import java.util.List;

/** Utils for Table. TODO we can introduce LocalAction maybe? */
public class TableUtils {

    private static final String TEMP_DIR = System.getProperty("java.io.tmpdir");

    /**
     * Delete according to filters.
     *
     * <p>NOTE: This method is only suitable for deletion of small amount of data.
     *
     * @return the number of deleted records
     */
    public static long deleteWhere(Table table, List<Predicate> filters) {
        ReadBuilder readBuilder = table.newReadBuilder().withFilter(filters);
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        List<Split> splits = readBuilder.newScan().plan().splits();
        long hit = 0;
        try (RecordReader<InternalRow> reader = readBuilder.newRead().createReader(splits);
                BatchTableWrite write = writeBuilder.newWrite();
                // we create temp io manager for writer
                IOManager ioManager = new IOManagerImpl(TEMP_DIR);
                BatchTableCommit commit = writeBuilder.newCommit()) {
            write.withIOManager(ioManager);
            CloseableIterator<InternalRow> iterator = reader.toCloseableIterator();
            Predicate filter = PredicateBuilder.and(filters);
            while (iterator.hasNext()) {
                InternalRow row = iterator.next();
                if (filter.test(row)) {
                    hit++;
                    row.setRowKind(RowKind.DELETE);
                    write.write(row);
                }
            }

            commit.commit(write.prepareCommit());
            return hit;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
