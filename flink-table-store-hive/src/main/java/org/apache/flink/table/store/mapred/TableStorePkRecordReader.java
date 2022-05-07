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

/** An {@link AbstractTableStoreRecordReader} for table with primary keys. */
public class TableStorePkRecordReader extends AbstractTableStoreRecordReader {

    public TableStorePkRecordReader(RecordReader wrapped, long splitLength) {
        super(wrapped, splitLength);
    }

    @Override
    public boolean next(Void key, RowDataContainer value) throws IOException {
        if (iterator.hasNext()) {
            KeyValue kv = iterator.next();
            value.set(kv.value());
            return true;
        } else {
            progress = 1;
            return false;
        }
    }
}
