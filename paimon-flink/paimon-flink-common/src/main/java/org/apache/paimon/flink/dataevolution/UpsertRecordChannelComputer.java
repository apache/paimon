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

package org.apache.paimon.flink.dataevolution;

import org.apache.paimon.table.sink.ChannelComputer;
import org.apache.paimon.utils.MurmurHashUtils;

/**
 * A {@link ChannelComputer} that routes {@link UpsertRecord}s for the Phase 2 shuffle. UPDATE
 * records are routed by {@code firstRowId} to ensure single-writer-per-file. INSERT records are
 * round-robin distributed to avoid data skew.
 */
public class UpsertRecordChannelComputer implements ChannelComputer<UpsertRecord> {

    private static final long serialVersionUID = 1L;

    private transient int numChannels;
    private transient int insertCounter;

    @Override
    public void setup(int numChannels) {
        this.numChannels = numChannels;
        this.insertCounter = 0;
    }

    @Override
    public int channel(UpsertRecord record) {
        if (record.isInsert()) {
            return (insertCounter++ & Integer.MAX_VALUE) % numChannels;
        }
        long hash = MurmurHashUtils.fmix(record.firstRowId());
        return (int) (hash % numChannels + numChannels) % numChannels;
    }

    @Override
    public String toString() {
        return "shuffle by firstRowId/partition for upsert";
    }
}
