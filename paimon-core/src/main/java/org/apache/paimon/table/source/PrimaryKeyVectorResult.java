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

package org.apache.paimon.table.source;

import org.apache.paimon.globalindex.IndexedSplit;
import org.apache.paimon.utils.RoaringNavigableMap64;

import java.util.ArrayList;
import java.util.List;

import static org.apache.paimon.globalindex.VectorSearchMetric.normalize;

/** Compatibility result adapting primary-key vector candidates to generic physical positions. */
public class PrimaryKeyVectorResult implements GlobalIndexSplitResult {

    private final PrimaryKeyScoredResult delegate;

    PrimaryKeyVectorResult(
            PrimaryKeyVectorScan.Plan plan,
            List<PrimaryKeyVectorRead.Candidate> candidates,
            String metric) {
        String normalizedMetric = normalize(metric);
        List<DataSplit> sourceSplits = new ArrayList<>(plan.splits().size());
        for (VectorSearchSplit split : plan.splits()) {
            sourceSplits.add(((BucketVectorSearchSplit) split).dataSplit());
        }
        List<PrimaryKeySearchPosition> positions = new ArrayList<>(candidates.size());
        for (PrimaryKeyVectorRead.Candidate candidate : candidates) {
            positions.add(
                    new PrimaryKeySearchPosition(
                            candidate.partition(),
                            candidate.bucket(),
                            candidate.dataFileName(),
                            candidate.rowPosition(),
                            score(normalizedMetric, candidate.distance())));
        }
        this.delegate = new PrimaryKeyScoredResult(plan.snapshotId(), sourceSplits, positions);
    }

    @Override
    public long snapshotId() {
        return delegate.snapshotId();
    }

    public List<PrimaryKeySearchPosition> positions() {
        return delegate.positions();
    }

    PrimaryKeyScoredResult scoredResult() {
        return delegate;
    }

    @Override
    public List<IndexedSplit> splits() {
        return delegate.splits();
    }

    @Override
    public RoaringNavigableMap64 results() {
        return delegate.results();
    }

    private static float score(String metric, float distance) {
        if ("l2".equals(metric)) {
            return 1F / (1F + distance);
        } else if ("cosine".equals(metric)) {
            return 1F - distance;
        } else if ("inner_product".equals(metric)) {
            return -distance;
        }
        throw new IllegalArgumentException("Unsupported primary-key vector metric: " + metric);
    }
}
