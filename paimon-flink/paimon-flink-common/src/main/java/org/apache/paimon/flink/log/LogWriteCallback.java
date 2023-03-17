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

package org.apache.paimon.flink.log;

import org.apache.paimon.flink.sink.LogSinkFunction.WriteCallback;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAccumulator;

/** A {@link WriteCallback} implementation. */
public class LogWriteCallback implements WriteCallback {

    private final ConcurrentHashMap<Integer, LongAccumulator> offsetMap = new ConcurrentHashMap<>();

    @Override
    public void onCompletion(int bucket, long offset) {
        LongAccumulator acc = offsetMap.get(bucket);
        if (acc == null) {
            // computeIfAbsent will lock on the key
            acc = offsetMap.computeIfAbsent(bucket, k -> new LongAccumulator(Long::max, 0));
        } // else lock free

        // Save the next offset, what we need to provide to the hybrid reading is the starting
        // offset of the next transaction
        acc.accumulate(offset + 1);
    }

    public Map<Integer, Long> offsets() {
        Map<Integer, Long> offsets = new HashMap<>();
        offsetMap.forEach((k, v) -> offsets.put(k, v.longValue()));
        return offsets;
    }
}
