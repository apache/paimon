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

package org.apache.paimon.manifest;

import org.apache.paimon.manifest.BinaryManifestEntry.ReusableIdentifier;

import javax.annotation.Nullable;

import java.util.Arrays;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkState;

/**
 * Compact, collision-safe set backed by primitive arrays and one identifier byte arena.
 *
 * <p>The reusable identifier is only used as lookup scratch. Added identifier bytes are copied into
 * the arena.
 */
public final class DeletedIdentifierSet {

    private static final float LOAD_FACTOR = 0.75f;

    private @Nullable ReusableIdentifier reusableIdentifier;
    private int[] buckets = filledWithMinusOne(16);
    private long[] hashes = new long[16];
    private int[] partitionIds = new int[16];
    private int[] offsets = new int[16];
    private int[] lengths = new int[16];
    private int[] next = new int[16];
    private byte[] arena = new byte[256];
    private int arenaSize;
    private int size;

    public boolean isEmpty() {
        return size == 0;
    }

    public int size() {
        return size;
    }

    public int retainedIdentifierBytes() {
        return arenaSize;
    }

    public void add(BinaryManifestEntry entry) {
        add(reusableIdentifier().replaceWithPartition(entry));
    }

    public void add(ReusableIdentifier identifier) {
        add(0, identifier);
    }

    public void add(int partitionId, ReusableIdentifier identifier) {
        checkIdentifier(identifier);
        add(partitionId, identifier.bytes(), identifier.length());
    }

    public boolean contains(BinaryManifestEntry entry) {
        return contains(reusableIdentifier().replaceWithPartition(entry));
    }

    public boolean contains(ReusableIdentifier identifier) {
        return contains(0, identifier);
    }

    public boolean contains(int partitionId, ReusableIdentifier identifier) {
        checkIdentifier(identifier);
        return contains(partitionId, identifier.bytes(), identifier.length());
    }

    public void release() {
        if (reusableIdentifier != null) {
            reusableIdentifier.release();
            reusableIdentifier = null;
        }
        buckets = filledWithMinusOne(16);
        hashes = new long[0];
        partitionIds = new int[0];
        offsets = new int[0];
        lengths = new int[0];
        next = new int[0];
        arena = new byte[0];
        arenaSize = 0;
        size = 0;
    }

    void add(int partitionId, byte[] identifier, int length) {
        checkIdentifier(identifier, length);
        long hash = hash(partitionId, identifier, length);
        if (contains(partitionId, identifier, length, hash)) {
            return;
        }
        if (size + 1 > (int) (buckets.length * LOAD_FACTOR)) {
            growBuckets();
        }
        ensureEntryCapacity(size + 1);
        ensureArenaCapacity(length);
        int offset = arenaSize;
        System.arraycopy(identifier, 0, arena, offset, length);
        arenaSize = Math.addExact(arenaSize, length);

        int bucket = bucket(hash);
        hashes[size] = hash;
        partitionIds[size] = partitionId;
        offsets[size] = offset;
        lengths[size] = length;
        next[size] = buckets[bucket];
        buckets[bucket] = size;
        size++;
    }

    boolean contains(int partitionId, byte[] identifier, int length) {
        checkIdentifier(identifier, length);
        return contains(partitionId, identifier, length, hash(partitionId, identifier, length));
    }

    private boolean contains(int partitionId, byte[] identifier, int length, long hash) {
        for (int entry = buckets[bucket(hash)]; entry >= 0; entry = next[entry]) {
            if (hashes[entry] == hash
                    && partitionIds[entry] == partitionId
                    && lengths[entry] == length
                    && bytesEqual(arena, offsets[entry], identifier, length)) {
                return true;
            }
        }
        return false;
    }

    private void growBuckets() {
        checkState(
                buckets.length < (1 << 30), "Too many deleted identifiers in one manifest group.");
        int[] grown = filledWithMinusOne(buckets.length << 1);
        for (int entry = 0; entry < size; entry++) {
            int bucket = bucket(hashes[entry], grown.length);
            next[entry] = grown[bucket];
            grown[bucket] = entry;
        }
        buckets = grown;
    }

    private void ensureEntryCapacity(int required) {
        if (required <= hashes.length) {
            return;
        }
        int grown = Math.max(required, hashes.length + (hashes.length >>> 1));
        hashes = Arrays.copyOf(hashes, grown);
        partitionIds = Arrays.copyOf(partitionIds, grown);
        offsets = Arrays.copyOf(offsets, grown);
        lengths = Arrays.copyOf(lengths, grown);
        next = Arrays.copyOf(next, grown);
    }

    private void ensureArenaCapacity(int additional) {
        int required;
        try {
            required = Math.addExact(arenaSize, additional);
        } catch (ArithmeticException e) {
            throw new IllegalStateException(
                    "Deleted identifier arena exceeds the Java array limit.", e);
        }
        if (required <= arena.length) {
            return;
        }
        int grown = Math.max(required, arena.length + (arena.length >>> 1));
        if (grown < 0) {
            grown = required;
        }
        arena = Arrays.copyOf(arena, grown);
    }

    private int bucket(long hash) {
        return bucket(hash, buckets.length);
    }

    private static int bucket(long hash, int bucketCount) {
        return ((int) (hash ^ (hash >>> 32))) & (bucketCount - 1);
    }

    private static long hash(int partitionId, byte[] bytes, int length) {
        long hash = 0xcbf29ce484222325L;
        hash ^= Integer.toUnsignedLong(partitionId);
        hash *= 0x100000001b3L;
        for (int i = 0; i < length; i++) {
            hash ^= bytes[i] & 0xFFL;
            hash *= 0x100000001b3L;
        }
        return hash;
    }

    private static boolean bytesEqual(byte[] left, int leftOffset, byte[] right, int length) {
        for (int i = 0; i < length; i++) {
            if (left[leftOffset + i] != right[i]) {
                return false;
            }
        }
        return true;
    }

    private static void checkIdentifier(ReusableIdentifier identifier) {
        checkArgument(identifier != null, "Identifier cannot be null.");
    }

    private ReusableIdentifier reusableIdentifier() {
        if (reusableIdentifier == null) {
            reusableIdentifier = new ReusableIdentifier();
        }
        return reusableIdentifier;
    }

    private static void checkIdentifier(byte[] identifier, int length) {
        checkArgument(identifier != null, "Identifier bytes cannot be null.");
        checkArgument(
                length >= 0 && length <= identifier.length,
                "Invalid identifier length %s.",
                length);
    }

    private static int[] filledWithMinusOne(int length) {
        int[] values = new int[length];
        Arrays.fill(values, -1);
        return values;
    }
}
