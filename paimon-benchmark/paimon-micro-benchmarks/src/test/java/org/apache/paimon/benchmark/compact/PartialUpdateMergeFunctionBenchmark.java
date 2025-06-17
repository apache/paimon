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

    private final int rowCount = 40000000;

    @Test
    public void testUpdateWithSequenceGroup() {
        Options options = new Options();
        options.set("fields.f1.sequence-group", "f2,f3,f4,f5");

        MergeFunctionFactory<KeyValue> factory =
                PartialUpdateMergeFunction.factory(options, getRowType(6), ImmutableList.of("f0"));

        MergeFunction<KeyValue> func = factory.create();

        Benchmark benchmark =
                new Benchmark("partial-update-benchmark", rowCount)
                        .setNumWarmupIters(1)
                        .setOutputPerIteration(true);

        benchmark.addCase(
                "updateWithSequenceGroup",
                5,
                () -> {
                    func.reset();
                    for (int i = 0; i < rowCount; i++) {
                        add(func, i, RowKind.INSERT, 1, i, 1, 1, 1, 1);
                    }
                });

        benchmark.run();
    }

    @Test
    public void testRetractWithSequenceGroup() {
        Options options = new Options();
        options.set("fields.f1.sequence-group", "f2,f3,f4,f5");

        MergeFunctionFactory<KeyValue> factory =
                PartialUpdateMergeFunction.factory(options, getRowType(6), ImmutableList.of("f0"));

        MergeFunction<KeyValue> func = factory.create();

        Benchmark benchmark =
                new Benchmark("partial-update-benchmark", rowCount)
                        .setNumWarmupIters(1)
                        .setOutputPerIteration(true);

        benchmark.addCase(
                "retractWithSequenceGroup",
                5,
                () -> {
                    func.reset();
                    for (int i = 0; i < rowCount; i++) {
                        add(func, i, RowKind.DELETE, 1, i, 1, 1, 1, 1);
                    }
                });

        benchmark.run();
    }

    @Test
    public void testUpdateWithEmptySequenceGroup() {
        Options options = new Options();
        options.set("fields.f1.sequence-group", "f2");
        options.set("fields.f3.sequence-group", "f4");
        options.set("fields.f5.sequence-group", "f6,f7,f8");

        MergeFunctionFactory<KeyValue> factory =
                PartialUpdateMergeFunction.factory(options, getRowType(9), ImmutableList.of("f0"));

        MergeFunction<KeyValue> func = factory.create();

        Benchmark benchmark =
                new Benchmark("partial-update-benchmark", rowCount)
                        .setNumWarmupIters(1)
                        .setOutputPerIteration(true);

        benchmark.addCase(
                "updateWithEmptySequenceGroup",
                5,
                () -> {
                    func.reset();
                    for (int i = 0; i < rowCount; i++) {
                        add(func, i, RowKind.INSERT, 1, i, 1, null, null, null, null, null, null);
                    }
                });

        benchmark.run();
    }

    private RowType getRowType(int numFields) {
        DataField[] fields = new DataField[numFields];
        fields[0] = new DataField(0, "k", DataTypes.INT());

        for (int i = 1; i < numFields; i++) {
            fields[i] = new DataField(i, "f" + i, DataTypes.INT());
        }
        return RowType.of(fields);
    }

    private void add(
            MergeFunction<KeyValue> function, int sequence, RowKind rowKind, Integer... f) {
        function.add(new KeyValue().replace(GenericRow.of(1), sequence, rowKind, GenericRow.of(f)));
    }
}
