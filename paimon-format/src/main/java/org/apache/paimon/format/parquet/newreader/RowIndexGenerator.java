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

import org.apache.paimon.utils.LongIterator;

import org.apache.parquet.column.page.PageReadStore;

import java.util.PrimitiveIterator;

/* This file is based on source code from the Spark Project (http://spark.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/** Generate row index for columnar batch. */
public class RowIndexGenerator {

    private LongIterator rowIndexIterator;

    public void initFromPageReadStore(PageReadStore pageReadStore) {
        long startingRowIdx = pageReadStore.getRowIndexOffset().orElse(0L);
        PrimitiveIterator.OfLong rowIndexes = pageReadStore.getRowIndexes().orElse(null);
        if (rowIndexes != null) {
            rowIndexIterator =
                    new LongIterator() {
                        @Override
                        public boolean hasNext() {
                            return rowIndexes.hasNext();
                        }

                        @Override
                        public long next() {
                            return rowIndexes.nextLong() + startingRowIdx;
                        }
                    };
        } else {
            long numRowsInRowGroup = pageReadStore.getRowCount();
            rowIndexIterator =
                    LongIterator.fromRange(startingRowIdx, startingRowIdx + numRowsInRowGroup);
        }
    }

    public void populateRowIndex(ColumnarBatch columnarBatch) {
        columnarBatch.resetPositions(rowIndexIterator);
    }
}
