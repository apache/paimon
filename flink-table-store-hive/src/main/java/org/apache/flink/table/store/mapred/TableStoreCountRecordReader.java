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
import org.apache.flink.table.store.file.KeyValue;
import org.apache.flink.table.store.file.utils.RecordReader;

import java.io.IOException;

/** An {@link AbstractTableStoreRecordReader} for table without primary keys. */
public class TableStoreCountRecordReader extends AbstractTableStoreRecordReader {

    private KeyValue current;
    private long remainingCount;

    public TableStoreCountRecordReader(RecordReader wrapped, long splitLength) {
        super(wrapped, splitLength);
        this.current = null;
        this.remainingCount = 0;
    }

    @Override
    public boolean next(Void key, RowDataContainer value) throws IOException {
        if (remainingCount > 0) {
            remainingCount--;
            value.set(current.key());
            return true;
        } else if (iterator.hasNext()) {
            current = iterator.next();
            value.set(current.key());
            remainingCount = current.value().getLong(0) - 1;
            return true;
        } else {
            progress = 1;
            return false;
        }
    }
}
