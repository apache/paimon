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

package org.apache.paimon.spark.comet;

import org.apache.comet.parquet.AbstractColumnReader;
import org.apache.comet.parquet.BatchReader;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * A Paimon adaptation of Iceberg's CometColumnarBatchReader. Reads a batch of rows using Comet's
 * native reader and returns a Spark ColumnarBatch.
 */
public class CometColumnarBatchReader implements AutoCloseable {

    private final CometColumnReader[] readers;
    private final BatchReader delegate;
    private final int capacity;
    private ColumnarBatch currentBatch;

    public CometColumnarBatchReader(CometColumnReader[] readers, StructType schema, int capacity) {
        this.readers = readers;
        this.capacity = capacity;

        AbstractColumnReader[] abstractColumnReaders = new AbstractColumnReader[readers.length];
        for (int i = 0; i < readers.length; i++) {
            abstractColumnReaders[i] = readers[i].delegate();
        }

        this.delegate = new BatchReader(abstractColumnReaders);
        this.delegate.setSparkSchema(schema);
    }

    public void setRowGroupInfo(PageReadStore pageStore) {
        for (int i = 0; i < readers.length; i++) {
            try {
                readers[i].reset();
                readers[i].setPageReader(pageStore.getPageReader(readers[i].descriptor()));
            } catch (IOException e) {
                throw new UncheckedIOException(
                        "Failed to setRowGroupInfo for Comet vectorization", e);
            }
        }

        // Update delegate's column readers after reset
        for (int i = 0; i < readers.length; i++) {
            delegate.getColumnReaders()[i] = this.readers[i].delegate();
        }
    }

    public ColumnarBatch read(int numRowsToRead) {
        if (numRowsToRead <= 0) {
            throw new IllegalArgumentException("Invalid number of rows to read: " + numRowsToRead);
        }

        // Fetch rows for all readers in the delegate
        // Delegate.nextBatch returns the number of rows read, but we expect it to match request or
        // be handled.
        // Actually Comet's BatchReader.nextBatch takes the count.
        delegate.nextBatch(numRowsToRead);

        if (currentBatch == null) {
            ColumnVector[] columnVectors = new ColumnVector[readers.length];
            for (int i = 0; i < readers.length; i++) {
                columnVectors[i] = readers[i].delegate().currentBatch();
            }
            currentBatch = new ColumnarBatch(columnVectors);
        }

        currentBatch.setNumRows(numRowsToRead);
        return currentBatch;
    }

    public void setBatchSize(int batchSize) {
        for (CometColumnReader reader : readers) {
            if (reader != null) {
                reader.setBatchSize(batchSize);
            }
        }
    }

    @Override
    public void close() {
        for (CometColumnReader reader : readers) {
            if (reader != null) {
                reader.close();
            }
        }
    }
}
