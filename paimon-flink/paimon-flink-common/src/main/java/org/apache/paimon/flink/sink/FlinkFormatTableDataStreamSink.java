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

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.flink.FlinkRowWrapper;
import org.apache.paimon.table.FormatTable;
import org.apache.paimon.table.format.FormatTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.data.RowData;

import java.util.List;

/** DataStream sink for format tables. */
public class FlinkFormatTableDataStreamSink {

    private final FormatTable table;

    public FlinkFormatTableDataStreamSink(FormatTable table) {
        this.table = table;
    }

    public DataStreamSink<?> sinkFrom(DataStream<RowData> dataStream) {
        return dataStream.addSink(new FormatTableSinkFunction(table));
    }

    /** Sink function for format tables. */
    private static class FormatTableSinkFunction extends RichSinkFunction<RowData> {

        private final FormatTable table;
        private transient FormatTableWrite tableWrite;
        private transient BatchWriteBuilder writeBuilder;

        public FormatTableSinkFunction(FormatTable table) {
            this.table = table;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            this.writeBuilder = table.newBatchWriteBuilder();
            this.tableWrite = (FormatTableWrite) writeBuilder.newWrite();
        }

        @Override
        public void invoke(RowData value, Context context) throws Exception {
            if (tableWrite != null) {
                InternalRow internalRow = new FlinkRowWrapper(value);
                tableWrite.write(internalRow);
            }
        }

        @Override
        public void close() throws Exception {
            if (tableWrite != null) {
                try {
                    // Prepare commit and commit the data
                    List<CommitMessage> committers = tableWrite.prepareCommit();
                    if (!committers.isEmpty()) {
                        tableWrite.commit(committers);
                    }
                } finally {
                    try {
                        tableWrite.close();
                    } catch (Exception e) {
                        // Log and ignore close errors
                    }
                }
            }
            super.close();
        }
    }
}
