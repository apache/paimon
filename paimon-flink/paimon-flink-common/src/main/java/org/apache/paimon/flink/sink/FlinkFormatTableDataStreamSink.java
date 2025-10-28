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

import org.apache.flink.api.connector.sink2.InitContext;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.data.RowData;

import java.util.List;

/** DataStream sink for format tables. */
public class FlinkFormatTableDataStreamSink {

    private final FormatTable table;

    public FlinkFormatTableDataStreamSink(FormatTable table) {
        this.table = table;
    }

    public DataStreamSink<?> sinkFrom(DataStream<RowData> dataStream) {
        return dataStream.sinkTo(new FormatTableSink(table));
    }

    private static class FormatTableSink implements Sink<RowData> {

        private final FormatTable table;

        public FormatTableSink(FormatTable table) {
            this.table = table;
        }

        /**
         * Do not annotate with <code>@override</code> here to maintain compatibility with Flink
         * 2.0+.
         */
        public SinkWriter<RowData> createWriter(InitContext context) {
            return new FormatTableSinkWriter(table);
        }

        /**
         * Do not annotate with <code>@override</code> here to maintain compatibility with Flink
         * 1.18-.
         */
        public SinkWriter<RowData> createWriter(WriterInitContext context) {
            return new FormatTableSinkWriter(table);
        }

        /** Sink writer for format tables using Flink v2 API. */
        private static class FormatTableSinkWriter implements SinkWriter<RowData> {

            private transient FormatTableWrite tableWrite;
            private transient BatchWriteBuilder writeBuilder;

            public FormatTableSinkWriter(FormatTable table) {
                this.writeBuilder = table.newBatchWriteBuilder();
                this.tableWrite = (FormatTableWrite) writeBuilder.newWrite();
            }

            @Override
            public void write(RowData element, Context context) {
                try {
                    InternalRow internalRow = new FlinkRowWrapper(element);
                    tableWrite.write(internalRow);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void flush(boolean endOfInput) {}

            @Override
            public void close() throws Exception {
                if (tableWrite != null) {
                    List<CommitMessage> committers = null;
                    try {
                        // Prepare commit and commit the data
                        committers = tableWrite.prepareCommit();
                        if (!committers.isEmpty()) {
                            tableWrite.commit(committers);
                        }
                    } catch (Exception e) {
                        if (committers != null && !committers.isEmpty()) {
                            tableWrite.discard(committers);
                        }
                        throw new RuntimeException(e);
                    } finally {
                        try {
                            tableWrite.close();
                        } catch (Exception ignore) {
                        }
                    }
                }
            }
        }
    }
}
