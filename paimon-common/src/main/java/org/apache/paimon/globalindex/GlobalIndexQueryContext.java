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

/** Resource budget for scalar index readers participating in one lookup scope. */
public final class GlobalIndexQueryContext {

    private static final GlobalIndexQueryContext UNLIMITED =
            new GlobalIndexQueryContext(
                    Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE);

    private final long maxDecodedRowIds;
    private final long maxReadBytes;
    private final SharedBudget sharedBudget;

    private long decodedRowIds;
    private long readBytes;
    private boolean declined;

    public GlobalIndexQueryContext(long maxDecodedRowIds) {
        this(maxDecodedRowIds, Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE);
    }

    public GlobalIndexQueryContext(
            long maxDecodedRowIds,
            long maxTotalDecodedRowIds,
            long maxReadBytes,
            long maxTotalReadBytes) {
        checkPositive(maxDecodedRowIds, "Maximum decoded row id count");
        checkPositive(maxTotalDecodedRowIds, "Maximum total decoded row id count");
        checkPositive(maxReadBytes, "Maximum index read bytes");
        checkPositive(maxTotalReadBytes, "Maximum total index read bytes");
        this.maxDecodedRowIds = maxDecodedRowIds;
        this.maxReadBytes = maxReadBytes;
        this.sharedBudget = new SharedBudget(maxTotalDecodedRowIds, maxTotalReadBytes);
    }

    private GlobalIndexQueryContext(
            long maxDecodedRowIds, long maxReadBytes, SharedBudget sharedBudget) {
        this.maxDecodedRowIds = maxDecodedRowIds;
        this.maxReadBytes = maxReadBytes;
        this.sharedBudget = sharedBudget;
    }

    public static GlobalIndexQueryContext unlimited() {
        return UNLIMITED;
    }

    public boolean isUnlimited() {
        return maxDecodedRowIds == Long.MAX_VALUE
                && maxReadBytes == Long.MAX_VALUE
                && sharedBudget.isUnlimited();
    }

    /** Creates a field lookup scope with local limits and a shared query-level budget. */
    GlobalIndexQueryContext fork() {
        return isUnlimited()
                ? UNLIMITED
                : new GlobalIndexQueryContext(maxDecodedRowIds, maxReadBytes, sharedBudget);
    }

    /** Reserves budget before row IDs are allocated or decoded. */
    public void reserveDecodedRowIds(long count) {
        reserve(Resource.DECODED_ROW_IDS, count);
    }

    /** Reserves budget before index bytes are read from storage. */
    public void reserveReadBytes(long count) {
        reserve(Resource.READ_BYTES, count);
    }

    private void reserve(Resource resource, long count) {
        if (count < 0) {
            throw new IllegalArgumentException(resource.description + " must not be negative.");
        }
        // In particular, do not lock the shared unlimited singleton across unrelated queries.
        if (count == 0 || isUnlimited()) {
            return;
        }
        reserveBounded(resource, count);
    }

    private synchronized void reserveBounded(Resource resource, long count) {
        if (declined) {
            throw declined(resource, count, "lookup scope already declined");
        }

        long current = resource == Resource.DECODED_ROW_IDS ? decodedRowIds : readBytes;
        long localLimit = resource == Resource.DECODED_ROW_IDS ? maxDecodedRowIds : maxReadBytes;
        if (count > localLimit - current) {
            declined = true;
            throw declined(resource, count, "field limit=" + localLimit);
        }

        if (!sharedBudget.reserve(resource, count)) {
            declined = true;
            throw declined(resource, count, "query limit=" + sharedBudget.limit(resource));
        }

        if (resource == Resource.DECODED_ROW_IDS) {
            decodedRowIds += count;
        } else {
            readBytes += count;
        }
    }

    private GlobalIndexLookupDeclinedException declined(
            Resource resource, long requested, String reason) {
        return new GlobalIndexLookupDeclinedException(
                String.format(
                        "Global index %s budget exceeded: used=%s, requested=%s, %s.",
                        resource.label,
                        resource == Resource.DECODED_ROW_IDS ? decodedRowIds : readBytes,
                        requested,
                        reason));
    }

    public synchronized long decodedRowIds() {
        return decodedRowIds;
    }

    public synchronized long readBytes() {
        return readBytes;
    }

    long totalDecodedRowIds() {
        return sharedBudget.used(Resource.DECODED_ROW_IDS);
    }

    long totalReadBytes() {
        return sharedBudget.used(Resource.READ_BYTES);
    }

    private static void checkPositive(long value, String name) {
        if (value <= 0) {
            throw new IllegalArgumentException(name + " must be greater than 0.");
        }
    }

    private enum Resource {
        DECODED_ROW_IDS("decoded row-id", "Decoded row id count"),
        READ_BYTES("read-byte", "Index read byte count");

        private final String label;
        private final String description;

        Resource(String label, String description) {
            this.label = label;
            this.description = description;
        }
    }

    private static final class SharedBudget {

        private final long maxDecodedRowIds;
        private final long maxReadBytes;
        private final AtomicLong decodedRowIds = new AtomicLong();
        private final AtomicLong readBytes = new AtomicLong();

        private SharedBudget(long maxDecodedRowIds, long maxReadBytes) {
            this.maxDecodedRowIds = maxDecodedRowIds;
            this.maxReadBytes = maxReadBytes;
        }

        private boolean reserve(Resource resource, long count) {
            AtomicLong used = counter(resource);
            long limit = limit(resource);
            while (true) {
                long current = used.get();
                if (count > limit - current) {
                    return false;
                }
                if (used.compareAndSet(current, current + count)) {
                    return true;
                }
            }
        }

        private long used(Resource resource) {
            return counter(resource).get();
        }

        private long limit(Resource resource) {
            return resource == Resource.DECODED_ROW_IDS ? maxDecodedRowIds : maxReadBytes;
        }

        private boolean isUnlimited() {
            return maxDecodedRowIds == Long.MAX_VALUE && maxReadBytes == Long.MAX_VALUE;
        }

        private AtomicLong counter(Resource resource) {
            return resource == Resource.DECODED_ROW_IDS ? decodedRowIds : readBytes;
        }
    }
}
