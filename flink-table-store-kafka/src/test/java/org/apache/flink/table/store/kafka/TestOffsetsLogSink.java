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

package org.apache.flink.table.store.kafka;

import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.StatefulSink;
import org.apache.flink.api.connector.sink2.StatefulSink.StatefulSinkWriter;
import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink;
import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink.PrecommittingSinkWriter;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.table.store.log.LogInitContext;
import org.apache.flink.table.store.log.LogSinkProvider;
import org.apache.flink.table.store.table.sink.SinkRecord;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/** Test kafka {@link Sink}. */
public class TestOffsetsLogSink<
                WriterT extends
                        StatefulSinkWriter<SinkRecord, WriterStateT>
                                & PrecommittingSinkWriter<SinkRecord, CommT>,
                CommT,
                WriterStateT>
        implements StatefulSink<SinkRecord, WriterStateT>,
                TwoPhaseCommittingSink<SinkRecord, CommT> {

    private static final Map<String, Map<Integer, Long>> GLOBAL_OFFSETS = new ConcurrentHashMap<>();

    private final LogSinkProvider sinkProvider;
    private final String uuid;
    private final Sink<SinkRecord> sink;

    public TestOffsetsLogSink(LogSinkProvider sinkProvider, String uuid) {
        this.sinkProvider = sinkProvider;
        this.uuid = uuid;
        this.sink = sinkProvider.createSink();
    }

    public static Map<Integer, Long> drainOffsets(String uuid) {
        return GLOBAL_OFFSETS.remove(uuid);
    }

    @Override
    public WriterT createWriter(InitContext initContext) throws IOException {
        return (WriterT) sink.createWriter(wrapContext(initContext));
    }

    private InitContext wrapContext(InitContext initContext) {
        Consumer<?> consumer =
                sinkProvider.createMetadataConsumer(
                        (bucket, offset) -> {
                            Map<Integer, Long> offsets =
                                    GLOBAL_OFFSETS.computeIfAbsent(
                                            uuid, k -> new ConcurrentHashMap<>());
                            long nextOffset = offset + 1;
                            offsets.compute(
                                    bucket,
                                    (k, v) -> v == null ? nextOffset : Math.max(v, nextOffset));
                        });
        return new LogInitContext(initContext, consumer);
    }

    @Override
    public StatefulSinkWriter<SinkRecord, WriterStateT> restoreWriter(
            InitContext initContext, Collection<WriterStateT> collection) throws IOException {
        return ((StatefulSink<SinkRecord, WriterStateT>) sink)
                .restoreWriter(wrapContext(initContext), collection);
    }

    @Override
    public SimpleVersionedSerializer<WriterStateT> getWriterStateSerializer() {
        return ((StatefulSink<SinkRecord, WriterStateT>) sink).getWriterStateSerializer();
    }

    @Override
    public Committer<CommT> createCommitter() throws IOException {
        return ((TwoPhaseCommittingSink<SinkRecord, CommT>) sink).createCommitter();
    }

    @Override
    public SimpleVersionedSerializer<CommT> getCommittableSerializer() {
        return ((TwoPhaseCommittingSink<SinkRecord, CommT>) sink).getCommittableSerializer();
    }
}
