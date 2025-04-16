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

package org.apache.paimon.benchmark.compact;

import org.apache.paimon.KeyValue;
import org.apache.paimon.benchmark.Benchmark;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.mergetree.compact.MergeFunction;
import org.apache.paimon.mergetree.compact.MergeFunctionFactory;
import org.apache.paimon.mergetree.compact.PartialUpdateMergeFunction;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableList;

import org.junit.jupiter.api.Test;

/** Benchmark for measure the performance for {@link PartialUpdateMergeFunction}. */
public class PartialUpdateMergeFunctionBenchmark {
    @Test
    public void testSequenceFields() {
        DataField[] fields = new DataField[6];
        fields[0] = new DataField(0, "k", DataTypes.INT());
        fields[1] = new DataField(1, "f1", DataTypes.INT());
        fields[2] = new DataField(2, "f2", DataTypes.INT());
        fields[3] = new DataField(3, "f3", DataTypes.INT());
        fields[4] = new DataField(4, "f4", DataTypes.INT());
        fields[5] = new DataField(5, "f5", DataTypes.INT());
        RowType rowType = RowType.of(fields);

        Options options = new Options();
        options.set("fields.f1.sequence-group", "f2,f3,f4,f5");

        MergeFunctionFactory<KeyValue> factory =
                PartialUpdateMergeFunction.factory(options, rowType, ImmutableList.of("f0"));

        MergeFunction<KeyValue> func = factory.create();

        Benchmark benchmark =
                new Benchmark("partial-update-benchmark", 100)
                        .setNumWarmupIters(1)
                        .setOutputPerIteration(true);

        benchmark.addCase(
                "partial-update-with-sequence-group",
                5,
                () -> {
                    func.reset();
                    for (int i = 0; i < 40000000; i++) {
                        add(func, i, RowKind.INSERT, 1, i, 1, 1, 1, 1);
                    }
                });

        benchmark.run();
    }

    private void add(
            MergeFunction<KeyValue> function, int sequence, RowKind rowKind, Integer... f) {
        function.add(new KeyValue().replace(GenericRow.of(1), sequence, rowKind, GenericRow.of(f)));
    }
}
