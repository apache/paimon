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

package org.apache.paimon.format.parquet.newreader;

import org.apache.parquet.column.page.PageReadStore;

import java.util.Iterator;
import java.util.stream.Stream;

/* This file is based on source code from the Spark Project (http://spark.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/** Generate row index for columnar batch. */
public class RowIndexGenerator {

    Iterator<Long> rowIndexIterator;

    public void initFromPageReadStore(PageReadStore pageReadStore) {
        long startingRowIdx = pageReadStore.getRowIndexOffset().orElse(0L);

        if (pageReadStore.getRowIndexes().isPresent()) {
            final Iterator<Long> rowIndexes = pageReadStore.getRowIndexes().get();
            rowIndexIterator =
                    new Iterator<Long>() {
                        @Override
                        public boolean hasNext() {
                            return rowIndexes.hasNext();
                        }

                        @Override
                        public Long next() {
                            return rowIndexes.next() + startingRowIdx;
                        }
                    };
        } else {
            long numRowsInRowGroup = pageReadStore.getRowCount();
            rowIndexIterator =
                    Stream.iterate(startingRowIdx, i -> i + 1).limit(numRowsInRowGroup).iterator();
        }
    }

    public void populateRowIndex(ColumnarBatch columnarBatch, int numRows) {
        long[] rowIndexes = new long[numRows];
        for (int i = 0; i < numRows; i++) {
            rowIndexes[i] = rowIndexIterator.next();
        }
        columnarBatch.resetPositions(rowIndexes);
    }
}
