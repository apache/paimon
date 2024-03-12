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

import org.apache.paimon.table.source.snapshot.SnapshotReader;

import org.jetbrains.annotations.Nullable;

import java.io.Serializable;
import java.util.List;

public class SimplePlan implements SnapshotReader.Plan, Serializable {

    private final Long watermark;
    private final Long snapshotId;
    private final List<Split> splits;

    public SimplePlan(Long watermark, Long snapshotId, List<Split> splits) {
        this.watermark = watermark;
        this.snapshotId = snapshotId;
        this.splits = splits;
    }

    @Nullable
    @Override
    public Long watermark() {
        return watermark;
    }

    @Nullable
    @Override
    public Long snapshotId() {
        return snapshotId;
    }

    @Override
    public List<Split> splits() {
        return splits;
    }
}
