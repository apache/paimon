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

package org.apache.flink.table.store.mapred;

import org.apache.flink.table.store.RowDataContainer;
import org.apache.flink.table.store.file.utils.RecordReaderIterator;

import org.apache.hadoop.mapred.RecordReader;

import java.io.IOException;

/**
 * Base {@link RecordReader} for table store. Reads {@link
 * org.apache.flink.table.store.file.KeyValue}s from data files and picks out {@link
 * org.apache.flink.table.data.RowData} for Hive to consume.
 */
public abstract class AbstractTableStoreRecordReader
        implements RecordReader<Void, RowDataContainer> {

    protected final RecordReaderIterator iterator;
    private final long splitLength;

    protected float progress;

    public AbstractTableStoreRecordReader(
            org.apache.flink.table.store.file.utils.RecordReader wrapped, long splitLength) {
        this.iterator = new RecordReaderIterator(wrapped);
        this.splitLength = splitLength;
        this.progress = 0;
    }

    @Override
    public Void createKey() {
        return null;
    }

    @Override
    public RowDataContainer createValue() {
        return new RowDataContainer();
    }

    @Override
    public long getPos() throws IOException {
        return (long) (splitLength * getProgress());
    }

    @Override
    public void close() throws IOException {
        try {
            iterator.close();
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    public float getProgress() throws IOException {
        // TODO make this more precise
        return progress;
    }
}
