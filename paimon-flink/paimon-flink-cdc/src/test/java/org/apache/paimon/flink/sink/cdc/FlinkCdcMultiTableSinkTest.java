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

package org.apache.paimon.flink.sink.cdc;

import org.apache.paimon.flink.FlinkCatalogFactory;
import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.options.Options;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.junit.jupiter.api.Test;

import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link FlinkCdcMultiTableSink}. */
public class FlinkCdcMultiTableSinkTest {

    @Test
    public void testTransformationParallelism() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(8);
        int inputParallelism = ThreadLocalRandom.current().nextInt(8) + 1;
        DataStreamSource<CdcMultiplexRecord> input =
                env.addSource(
                                new ParallelSourceFunction<CdcMultiplexRecord>() {
                                    @Override
                                    public void run(SourceContext<CdcMultiplexRecord> ctx) {}

                                    @Override
                                    public void cancel() {}
                                })
                        .setParallelism(inputParallelism);

        FlinkCdcMultiTableSink sink =
                new FlinkCdcMultiTableSink(
                        () -> FlinkCatalogFactory.createPaimonCatalog(new Options()),
                        FlinkConnectorOptions.SINK_COMMITTER_CPU.defaultValue(),
                        null,
                        true,
                        UUID.randomUUID().toString());
        DataStreamSink<?> dataStreamSink = sink.sinkFrom(input);

        // check the transformation graph
        Transformation<?> end = dataStreamSink.getTransformation();
        assertThat(end.getName()).isEqualTo("end");

        OneInputTransformation<?, ?> committer =
                (OneInputTransformation<?, ?>) end.getInputs().get(0);
        assertThat(committer.getName()).isEqualTo("Multiplex Global Committer");
        assertThat(committer.getParallelism()).isEqualTo(inputParallelism);

        PartitionTransformation<?> partitioner =
                (PartitionTransformation<?>) committer.getInputs().get(0);
        assertThat(partitioner.getParallelism()).isEqualTo(inputParallelism);

        OneInputTransformation<?, ?> writer =
                (OneInputTransformation<?, ?>) partitioner.getInputs().get(0);
        assertThat(writer.getName()).isEqualTo("CDC MultiplexWriter");
        assertThat(writer.getParallelism()).isEqualTo(inputParallelism);
    }
}
