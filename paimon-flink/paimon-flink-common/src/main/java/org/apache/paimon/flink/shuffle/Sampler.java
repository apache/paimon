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
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.XORShiftRandom;

import java.util.Comparator;
import java.util.Iterator;
import java.util.PriorityQueue;
import java.util.Random;

/**
 * A simple in memory implementation Sampling, and with only one pass through the input iteration
 * whose size is unpredictable. The basic idea behind this sampler implementation is to generate a
 * random number for each input element as its weight, select the top K elements with max weight. As
 * the weights are generated randomly, so are the selected top K elements. In the first phase, we
 * generate random numbers as the weights for each element and select top K elements as the output
 * of each partitions. In the second phase, we select top K elements from all the outputs of the
 * first phase.
 *
 * <p>This implementation refers to the algorithm described in <a
 * href="researcher.ibm.com/files/us-dpwoodru/tw11.pdf">"Optimal Random Sampling from Distributed
 * Streams Revisited"</a>.
 */
@Internal
public class Sampler<T> {

    private final int numSamples;
    private final Random random;
    private final PriorityQueue<Tuple2<Double, T>> queue;

    private int index = 0;
    private Tuple2<Double, T> smallest = null;

    /**
     * Create a new sampler with reservoir size and a supplied random number generator.
     *
     * @param numSamples Maximum number of samples to retain in reservoir, must be non-negative.
     */
    Sampler(int numSamples, long seed) {
        Preconditions.checkArgument(numSamples >= 0, "numSamples should be non-negative.");
        this.numSamples = numSamples;
        this.random = new XORShiftRandom(seed);
        this.queue = new PriorityQueue<>(numSamples, Comparator.comparingDouble(o -> o.f0));
    }

    void collect(T rowData) {
        collect(random.nextDouble(), rowData);
    }

    void collect(double weight, T key) {
        if (index < numSamples) {
            // Fill the queue with first K elements from input.
            addQueue(weight, key);
        } else {
            // Remove the element with the smallest weight,
            // and append current element into the queue.
            if (weight > smallest.f0) {
                queue.remove();
                addQueue(weight, key);
            }
        }
        index++;
    }

    private void addQueue(double weight, T row) {
        queue.add(new Tuple2<>(weight, row));
        smallest = queue.peek();
    }

    Iterator<Tuple2<Double, T>> sample() {
        return queue.iterator();
    }
}
