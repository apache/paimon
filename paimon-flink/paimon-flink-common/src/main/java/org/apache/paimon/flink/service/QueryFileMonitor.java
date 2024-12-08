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

package org.apache.paimon.flink.service;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.flink.source.AbstractNonCoordinatedSource;
import org.apache.paimon.flink.source.AbstractNonCoordinatedSourceReader;
import org.apache.paimon.flink.source.SimpleSourceSplit;
import org.apache.paimon.flink.utils.InternalTypeInfo;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.ChannelComputer;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.StreamTableScan;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.table.system.FileMonitorTable;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

import static org.apache.paimon.utils.SerializationUtils.deserializeBinaryRow;

/**
 * This is the single (non-parallel) monitoring task, it is responsible for:
 *
 * <ol>
 *   <li>Read incremental files from tables.
 *   <li>Assigning them to downstream tasks for further processing.
 * </ol>
 */
public class QueryFileMonitor extends AbstractNonCoordinatedSource<InternalRow> {

    private static final long serialVersionUID = 1L;

    private final Table table;
    private final long monitorInterval;

    public QueryFileMonitor(Table table) {
        this.table = table;
        this.monitorInterval =
                Options.fromMap(table.options())
                        .get(CoreOptions.CONTINUOUS_DISCOVERY_INTERVAL)
                        .toMillis();
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SourceReader<InternalRow, SimpleSourceSplit> createReader(
            SourceReaderContext sourceReaderContext) throws Exception {
        return new Reader();
    }

    private class Reader extends AbstractNonCoordinatedSourceReader<InternalRow> {
        private transient StreamTableScan scan;
        private transient TableRead read;

        @Override
        public void start() {
            FileMonitorTable monitorTable = new FileMonitorTable((FileStoreTable) table);
            ReadBuilder readBuilder = monitorTable.newReadBuilder().dropStats();
            this.scan = readBuilder.newStreamScan();
            this.read = readBuilder.newRead();
        }

        @Override
        public InputStatus pollNext(ReaderOutput<InternalRow> readerOutput) throws Exception {
            boolean isEmpty = doScan(readerOutput);

            if (isEmpty) {
                Thread.sleep(monitorInterval);
            }
            return InputStatus.MORE_AVAILABLE;
        }

        private boolean doScan(ReaderOutput<InternalRow> readerOutput) throws Exception {
            List<InternalRow> records = new ArrayList<>();
            read.createReader(scan.plan()).forEachRemaining(records::add);
            records.forEach(readerOutput::collect);
            return records.isEmpty();
        }
    }

    public static DataStream<InternalRow> build(StreamExecutionEnvironment env, Table table) {
        return env.fromSource(
                        new QueryFileMonitor(table),
                        WatermarkStrategy.noWatermarks(),
                        "FileMonitor-" + table.name(),
                        InternalTypeInfo.fromRowType(FileMonitorTable.getRowType()))
                .setParallelism(1);
    }

    public static ChannelComputer<InternalRow> createChannelComputer() {
        return new FileMonitorChannelComputer();
    }

    /** A {@link ChannelComputer} to handle rows from {@link FileMonitorTable}. */
    private static class FileMonitorChannelComputer implements ChannelComputer<InternalRow> {

        private int numChannels;

        @Override
        public void setup(int numChannels) {
            this.numChannels = numChannels;
        }

        @Override
        public int channel(InternalRow row) {
            BinaryRow partition = deserializeBinaryRow(row.getBinary(1));
            int bucket = row.getInt(2);
            return ChannelComputer.select(partition, bucket, numChannels);
        }

        @Override
        public String toString() {
            return "FileMonitorChannelComputer{" + "numChannels=" + numChannels + '}';
        }
    }
}
