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

package org.apache.paimon.flink.shuffle;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.runtime.operators.TableStreamOperator;
import org.apache.flink.table.runtime.util.StreamRecordCollector;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**
 * LocalSampleOperator wraps the sample logic on the partition side (the first phase of distributed
 * sample algorithm). Outputs sampled weight with record.
 *
 * <p>See {@link Sampler}.
 */
@Internal
public class LocalSampleOperator<T> extends TableStreamOperator<Tuple2<Double, T>>
        implements OneInputStreamOperator<T, Tuple2<Double, T>>, BoundedOneInput {

    private static final long serialVersionUID = 1L;

    private final int numSample;

    private transient Collector<Tuple2<Double, T>> collector;
    private transient Sampler<T> sampler;

    public LocalSampleOperator(int numSample) {
        this.numSample = numSample;
    }

    @Override
    public void open() throws Exception {
        super.open();
        this.collector = new StreamRecordCollector<>(output);
        sampler = new Sampler<>(numSample, System.nanoTime());
    }

    @Override
    public void processElement(StreamRecord<T> streamRecord) throws Exception {
        sampler.collect(streamRecord.getValue());
    }

    @Override
    public void endInput() throws Exception {
        Iterator<Tuple2<Double, T>> sampled = sampler.sample();
        while (sampled.hasNext()) {
            collector.collect(sampled.next());
        }
    }
}
