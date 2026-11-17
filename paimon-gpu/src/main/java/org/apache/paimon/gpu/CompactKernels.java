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

package org.apache.paimon.gpu;

import uk.ac.manchester.tornado.api.KernelContext;
import uk.ac.manchester.tornado.api.types.arrays.ByteArray;
import uk.ac.manchester.tornado.api.types.arrays.DoubleArray;
import uk.ac.manchester.tornado.api.types.arrays.IntArray;
import uk.ac.manchester.tornado.api.types.arrays.LongArray;

/** Device kernels of a deduplicating merge-tree compaction. */
final class CompactKernels {

    static final int GROUP = 256;

    // params: rows, key columns, capacity, drop deletes (0/1), numeric outputs
    static final int P_ROWS = 0;
    static final int P_KEYS = 1;
    static final int P_CAPACITY = 2;
    static final int P_DROP_DELETE = 3;
    static final int P_OUTPUTS = 4;
    static final int P_SIZE = 5;

    /**
     * Folds whether a row adds into its sequence number, {@code 2 * sequence + 1} for an addition
     * and {@code 2 * sequence} for a retraction (UPDATE_BEFORE 1, DELETE 3), so that the sort over
     * keys and sequence numbers puts a retraction before an addition with the same sequence number,
     * as Paimon's merge readers do. The caller keeps sequence numbers in [0, 2^62).
     */
    static void foldKind(KernelContext context, IntArray params, LongArray longs, IntArray ints) {
        int row = context.globalIdx;
        if (row < params.get(P_ROWS)) {
            int slot = params.get(P_KEYS) * params.get(P_CAPACITY) + row;
            int kind = ints.get(row);
            long add = 1;
            if (kind == 1 || kind == 3) {
                add = 0;
            }
            longs.set(slot, longs.get(slot) * 2 + add);
        }
    }

    /** Restores the sequence numbers {@link #foldKind} folded. */
    static void unfoldKind(KernelContext context, IntArray params, LongArray longs) {
        int row = context.globalIdx;
        if (row < params.get(P_ROWS)) {
            int slot = params.get(P_KEYS) * params.get(P_CAPACITY) + row;
            longs.set(slot, longs.get(slot) >> 1);
        }
    }

    /**
     * One byte a sorted position: 1 when its row is the last of its key (highest sequence number,
     * an addition after a retraction of the same number) and, when deletes are dropped, is not a
     * retraction (UPDATE_BEFORE 1, DELETE 3). Two rows of a key with the same sequence number and
     * kind set {@code ties}: Paimon keeps whichever its readers happen to order last, and the
     * compaction is left to it.
     */
    static void markSurvivors(
            KernelContext context,
            IntArray params,
            IntArray order,
            LongArray longs,
            IntArray ints,
            ByteArray mask,
            IntArray ties) {
        int p = context.globalIdx;
        int rows = params.get(P_ROWS);
        int capacity = params.get(P_CAPACITY);
        if (p < capacity) {
            byte keep = 0;
            if (p < rows) {
                keep = 1;
                int row = order.get(p);
                if (p + 1 < rows) {
                    int next = order.get(p + 1);
                    int same = 1;
                    int keys = params.get(P_KEYS);
                    for (int c = 0; c < keys; c++) {
                        if (longs.get(c * capacity + row) != longs.get(c * capacity + next)) {
                            same = 0;
                        }
                    }
                    if (same == 1) {
                        keep = 0;
                        int sequence = keys * capacity;
                        if (longs.get(sequence + row) == longs.get(sequence + next)) {
                            ties.set(0, 1);
                        }
                    }
                }
                if (keep == 1 && params.get(P_DROP_DELETE) == 1) {
                    int kind = ints.get(row);
                    if (kind == 1 || kind == 3) {
                        keep = 0;
                    }
                }
            }
            mask.set(p, keep);
        }
    }

    /** Output row j takes read row order[selected[j]]: its numeric columns and their validity. */
    static void gatherRows(
            KernelContext context,
            IntArray params,
            IntArray count,
            IntArray order,
            IntArray selected,
            IntArray spec,
            IntArray ints,
            LongArray longs,
            DoubleArray doubles,
            ByteArray valid,
            IntArray gatherMap,
            IntArray outInts,
            LongArray outLongs,
            DoubleArray outDoubles,
            ByteArray outValid) {
        int j = context.globalIdx;
        if (j < count.get(0)) {
            int capacity = params.get(P_CAPACITY);
            int row = order.get(selected.get(j));
            gatherMap.set(j, row);
            int outputs = params.get(P_OUTPUTS);
            for (int o = 0; o < outputs; o++) {
                int srcBuffer = spec.get(o * 6);
                int src = spec.get(o * 6 + 1) * capacity + row;
                int dstBuffer = spec.get(o * 6 + 2);
                int dst = spec.get(o * 6 + 3) * capacity + j;
                if (srcBuffer == 0) {
                    outInts.set(dst, ints.get(src));
                } else if (srcBuffer == 1 && dstBuffer == 1) {
                    outLongs.set(dst, longs.get(src));
                } else if (srcBuffer == 1) {
                    outInts.set(dst, (int) longs.get(src));
                } else {
                    outDoubles.set(dst, doubles.get(src));
                }
                int validRead = spec.get(o * 6 + 4);
                if (validRead >= 0) {
                    outValid.set(
                            spec.get(o * 6 + 5) * capacity + j,
                            valid.get(validRead * capacity + row));
                }
            }
        }
    }

    /**
     * Per group of output rows: the least and greatest sequence number and the retractions; and the
     * keys of the first and last output rows. Sequence numbers are long slot {@code keys}.
     */
    static void summarize(
            KernelContext context,
            IntArray params,
            IntArray count,
            IntArray gatherMap,
            LongArray longs,
            IntArray ints,
            LongArray groupMin,
            LongArray groupMax,
            IntArray groupRetract,
            LongArray edges) {
        long[] lmin = context.allocateLongLocalArray(GROUP);
        long[] lmax = context.allocateLongLocalArray(GROUP);
        int[] lret = context.allocateIntLocalArray(GROUP);
        int j = context.globalIdx;
        int t = context.localIdx;
        int n = count.get(0);
        int capacity = params.get(P_CAPACITY);
        int keys = params.get(P_KEYS);
        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;
        int retract = 0;
        if (j < n) {
            int row = gatherMap.get(j);
            long seq = longs.get(keys * capacity + row);
            min = seq;
            max = seq;
            int kind = ints.get(row);
            if (kind == 1 || kind == 3) {
                retract = 1;
            }
            if (j == 0) {
                for (int c = 0; c < keys; c++) {
                    edges.set(c, longs.get(c * capacity + row));
                }
            }
            if (j == n - 1) {
                for (int c = 0; c < keys; c++) {
                    edges.set(keys + c, longs.get(c * capacity + row));
                }
            }
        }
        lmin[t] = min;
        lmax[t] = max;
        lret[t] = retract;
        for (int stride = GROUP / 2; stride > 0; stride /= 2) {
            context.localBarrier();
            if (t < stride) {
                lmin[t] = Math.min(lmin[t], lmin[t + stride]);
                lmax[t] = Math.max(lmax[t], lmax[t + stride]);
                lret[t] = lret[t] + lret[t + stride];
            }
        }
        if (t == 0) {
            int g = context.groupIdx;
            groupMin.set(g, lmin[0]);
            groupMax.set(g, lmax[0]);
            groupRetract.set(g, lret[0]);
        }
    }

    private CompactKernels() {}
}
