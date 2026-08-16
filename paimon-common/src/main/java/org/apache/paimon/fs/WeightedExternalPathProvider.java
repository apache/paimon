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

package org.apache.paimon.fs;

import java.util.Arrays;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Provider for weighted external data paths.
 *
 * <p>This provider uses a weighted random algorithm to select paths based on configured weights.
 * Higher weights result in higher probability of selection.
 */
public class WeightedExternalPathProvider implements ExternalPathProvider {

    private final NavigableMap<Double, Path> cumulativeWeightMap;
    private final double totalWeight;
    private final Path relativeBucketPath;

    public WeightedExternalPathProvider(
            List<Path> externalTablePaths, Path relativeBucketPath, int[] weights) {
        checkArgument(
                externalTablePaths.size() == weights.length,
                "The number of external paths and weights should be the same. Paths: "
                        + externalTablePaths.size()
                        + ", Weights: "
                        + weights.length);
        this.relativeBucketPath = relativeBucketPath;
        this.cumulativeWeightMap = buildCumulativeWeightMap(externalTablePaths, weights);
        this.totalWeight = Arrays.stream(weights).sum();
    }

    @Override
    public Path getNextExternalDataPath(String fileName) {
        double randomValue = ThreadLocalRandom.current().nextDouble() * totalWeight;
        Path selectedPath = cumulativeWeightMap.higherEntry(randomValue).getValue();
        return new Path(new Path(selectedPath, relativeBucketPath), fileName);
    }

    private NavigableMap<Double, Path> buildCumulativeWeightMap(
            List<Path> externalTablePaths, int[] weights) {
        NavigableMap<Double, Path> map = new TreeMap<>();
        double cumulativeWeight = 0;
        for (int i = 0; i < externalTablePaths.size(); i++) {
            cumulativeWeight += weights[i];
            map.put(cumulativeWeight, externalTablePaths.get(i));
        }
        return map;
    }
}
