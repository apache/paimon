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

package org.apache.flink.table.store.connector.sink;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.connector.sink2.StatefulSink;
import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.store.file.writer.RecordWriter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * The base class for file store sink writers.
 *
 * @param <WriterStateT> The type of the writer's state.
 */
public abstract class StoreSinkWriterBase<WriterStateT>
        implements StatefulSink.StatefulSinkWriter<RowData, WriterStateT>,
                TwoPhaseCommittingSink.PrecommittingSinkWriter<RowData, Committable> {

    protected final Map<BinaryRowData, Map<Integer, RecordWriter>> writers;

    protected StoreSinkWriterBase() {
        writers = new HashMap<>();
    }

    @Override
    public List<Committable> prepareCommit() throws IOException, InterruptedException {
        List<Committable> committables = new ArrayList<>();
        Iterator<Map.Entry<BinaryRowData, Map<Integer, RecordWriter>>> partIter =
                writers.entrySet().iterator();
        while (partIter.hasNext()) {
            Map.Entry<BinaryRowData, Map<Integer, RecordWriter>> partEntry = partIter.next();
            BinaryRowData partition = partEntry.getKey();
            Iterator<Map.Entry<Integer, RecordWriter>> bucketIter =
                    partEntry.getValue().entrySet().iterator();
            while (bucketIter.hasNext()) {
                Map.Entry<Integer, RecordWriter> entry = bucketIter.next();
                int bucket = entry.getKey();
                RecordWriter writer = entry.getValue();
                FileCommittable committable;
                try {
                    committable = new FileCommittable(partition, bucket, writer.prepareCommit());
                } catch (Exception e) {
                    throw new IOException(e);
                }
                committables.add(new Committable(Committable.Kind.FILE, committable));

                // clear if no update
                // we need a mechanism to clear writers, otherwise there will be more and more
                // such as yesterday's partition that no longer needs to be written.
                if (committable.increment().newFiles().isEmpty()) {
                    closeWriter(writer);
                    bucketIter.remove();
                }
            }

            if (partEntry.getValue().isEmpty()) {
                partIter.remove();
            }
        }
        return committables;
    }

    @Override
    public List<WriterStateT> snapshotState(long checkpointId) throws IOException {
        return Collections.emptyList();
    }

    @Override
    public void close() throws Exception {
        for (Map<Integer, RecordWriter> bucketWriters : writers.values()) {
            for (RecordWriter writer : bucketWriters.values()) {
                closeWriter(writer);
            }
        }
        writers.clear();
    }

    private void closeWriter(RecordWriter writer) throws IOException {
        try {
            writer.sync();
            writer.close();
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @VisibleForTesting
    Map<BinaryRowData, Map<Integer, RecordWriter>> writers() {
        return writers;
    }
}
