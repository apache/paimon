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

package org.apache.paimon.flink.sink.partition;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Partition assignment strategy that randomly distributes records to subtasks based on configured
 * weights.
 */
public class WeightedRandomAssignment {

    private final List<Integer> assignedSubtasks;
    private final List<Long> subtaskWeights;
    private final long keyWeight;
    private final double[] cumulativeWeights;
    private final Random random;

    public WeightedRandomAssignment(
            List<Integer> assignedSubtasks, List<Long> subtaskWeights, Random random) {
        checkArgument(
                assignedSubtasks != null && !assignedSubtasks.isEmpty(),
                "Invalid assigned subtasks: null or empty");
        checkArgument(
                subtaskWeights != null && !subtaskWeights.isEmpty(),
                "Invalid assigned subtask weights: null or empty");
        checkArgument(
                assignedSubtasks.size() == subtaskWeights.size(),
                "Invalid assignment: size mismatch (tasks length = %s, weights length = %s)",
                assignedSubtasks.size(),
                subtaskWeights.size());

        this.assignedSubtasks = assignedSubtasks;
        this.subtaskWeights = subtaskWeights;
        this.keyWeight = subtaskWeights.stream().mapToLong(Long::longValue).sum();
        this.cumulativeWeights = new double[subtaskWeights.size()];
        long cumulativeWeight = 0;
        for (int i = 0; i < subtaskWeights.size(); ++i) {
            cumulativeWeight += subtaskWeights.get(i);
            cumulativeWeights[i] = cumulativeWeight;
        }
        this.random = random;
    }

    public int select() {
        if (assignedSubtasks.size() == 1) {
            return assignedSubtasks.get(0);
        } else {
            double randomNumber = nextDouble(0, keyWeight);
            int index = Arrays.binarySearch(cumulativeWeights, randomNumber);
            int position = Math.abs(index + 1);
            if (position >= assignedSubtasks.size()) {
                position = assignedSubtasks.size() - 1;
            }
            return assignedSubtasks.get(position);
        }
    }

    private double nextDouble(double origin, double bound) {
        double r = random.nextDouble();
        r = r * (bound - origin) + origin;
        if (r >= bound) {
            r = Double.longBitsToDouble(Double.doubleToLongBits(bound) - 1);
        }
        return r;
    }

    @Override
    public String toString() {
        return "WeightedRandomAssignment{"
                + "assignedSubtasks="
                + assignedSubtasks
                + ", subtaskWeights="
                + subtaskWeights
                + ", keyWeight="
                + keyWeight
                + '}';
    }
}
