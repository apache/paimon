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

package org.apache.paimon.flink.sink;

import org.apache.paimon.flink.FlinkRowWrapper;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.SinkRecord;

import org.apache.flink.table.data.RowData;

import javax.annotation.Nullable;

import java.io.IOException;

/**
 * An {@link AbstractStoreWriteOperator} which accepts Flink's {@link RowData}. The schema of its
 * writer never changes.
 */
public class RowDataStoreWriteOperator extends AbstractStoreWriteOperator<RowData> {

    public RowDataStoreWriteOperator(
            FileStoreTable table,
            @Nullable LogSinkFunction logSinkFunction,
            StoreSinkWrite.Provider storeSinkWriteProvider) {
        super(table, logSinkFunction, storeSinkWriteProvider);
    }

    @Override
    protected SinkRecord processRecord(RowData record) throws Exception {
        try {
            return write.write(new FlinkRowWrapper(record));
        } catch (Exception e) {
            throw new IOException(e);
        }
    }
}
