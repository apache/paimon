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

package org.apache.paimon.globalindex;

import java.util.concurrent.atomic.AtomicLong;

/** Shared resource budget for all index readers participating in one query. */
public final class GlobalIndexQueryContext {

    private static final GlobalIndexQueryContext UNLIMITED =
            new GlobalIndexQueryContext(Long.MAX_VALUE);

    private final long maxDecodedRowIds;
    private final AtomicLong decodedRowIds;

    public GlobalIndexQueryContext(long maxDecodedRowIds) {
        if (maxDecodedRowIds <= 0) {
            throw new IllegalArgumentException(
                    "Maximum decoded row id count must be greater than 0.");
        }
        this.maxDecodedRowIds = maxDecodedRowIds;
        this.decodedRowIds = new AtomicLong();
    }

    public static GlobalIndexQueryContext unlimited() {
        return UNLIMITED;
    }

    /** Reserves budget before row IDs are allocated or decoded. */
    public void reserveDecodedRowIds(long count) {
        if (count < 0) {
            throw new IllegalArgumentException("Decoded row id count must not be negative.");
        }
        if (count == 0 || maxDecodedRowIds == Long.MAX_VALUE) {
            return;
        }

        while (true) {
            long current = decodedRowIds.get();
            if (count > maxDecodedRowIds - current) {
                throw new GlobalIndexLookupDeclinedException(
                        String.format(
                                "Global index decoded row-id budget exceeded: used=%s, requested=%s, max=%s.",
                                current, count, maxDecodedRowIds));
            }
            if (decodedRowIds.compareAndSet(current, current + count)) {
                return;
            }
        }
    }

    public long decodedRowIds() {
        return decodedRowIds.get();
    }
}
