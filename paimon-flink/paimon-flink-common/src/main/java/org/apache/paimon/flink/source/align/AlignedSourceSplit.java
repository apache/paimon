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

package org.apache.paimon.flink.source.align;

import org.apache.paimon.table.source.Split;

import org.apache.flink.api.connector.source.SourceSplit;

import java.util.List;

/**
 * Record the plan scanned by {@link AlignedSourceReader}, which is used to distribute splits to
 * {@link org.apache.paimon.flink.source.operator.ReadOperator}, restore state, etc.
 */
public class AlignedSourceSplit implements SourceSplit {
    private final List<Split> splits;
    private final long nextSnapshotId;
    private final boolean placeHolder;

    public AlignedSourceSplit(List<Split> splits, long nextSnapshotId, boolean placeHolder) {
        this.splits = splits;
        this.nextSnapshotId = nextSnapshotId;
        this.placeHolder = placeHolder;
    }

    @Override
    public String splitId() {
        return String.valueOf(nextSnapshotId);
    }

    public long getNextSnapshotId() {
        return nextSnapshotId;
    }

    public List<Split> getSplits() {
        return splits;
    }

    public boolean isPlaceHolder() {
        return placeHolder;
    }

    @Override
    public String toString() {
        return "AlignedSourceSplit{"
                + "splits="
                + splits
                + ", nextSnapshotId="
                + nextSnapshotId
                + ", placeHolder="
                + placeHolder
                + '}';
    }
}
