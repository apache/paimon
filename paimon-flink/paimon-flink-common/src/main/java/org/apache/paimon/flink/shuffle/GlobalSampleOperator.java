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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

/**
 * Global sample for range partition. Inputs weight with record. Outputs list of sampled record.
 *
 * <p>See {@link Sampler}.
 */
@Internal
public class GlobalSampleOperator<T> extends TableStreamOperator<List<T>>
        implements OneInputStreamOperator<Tuple2<Double, T>, List<T>>, BoundedOneInput {

    private static final long serialVersionUID = 1L;

    private final int numSample;
    private final int rangesNum;
    private final Comparator<T> keyComparator;

    private transient Collector<List<T>> collector;
    private transient Sampler<T> sampler;

    public GlobalSampleOperator(int numSample, Comparator<T> comparator, int rangesNum) {
        this.numSample = numSample;
        this.keyComparator = comparator;
        this.rangesNum = rangesNum;
    }

    @Override
    public void open() throws Exception {
        super.open();
        //noinspection unchecked
        this.sampler = new Sampler<>(numSample, 0L);
        this.collector = new StreamRecordCollector<>(output);
    }

    @Override
    public void processElement(StreamRecord<Tuple2<Double, T>> record) throws Exception {
        Tuple2<Double, T> tuple = record.getValue();
        sampler.collect(tuple.f0, tuple.f1);
    }

    @Override
    public void endInput() throws Exception {
        Iterator<Tuple2<Double, T>> sampled = sampler.sample();

        List<T> sampledData = new ArrayList<>();
        while (sampled.hasNext()) {
            sampledData.add(sampled.next().f1);
        }

        sampledData.sort(keyComparator);

        int boundarySize = rangesNum - 1;
        T[] boundaries = (T[]) new Object[boundarySize];
        if (sampledData.size() > 0) {
            double avgRange = sampledData.size() / (double) rangesNum;
            for (int i = 1; i < rangesNum; i++) {
                T record = sampledData.get((int) (i * avgRange));
                boundaries[i - 1] = record;
            }
        }

        collector.collect(Arrays.asList(boundaries));
    }
}
