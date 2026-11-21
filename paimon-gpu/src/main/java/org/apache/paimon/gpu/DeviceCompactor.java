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

import uk.ac.manchester.tornado.api.GridScheduler;
import uk.ac.manchester.tornado.api.KernelContext;
import uk.ac.manchester.tornado.api.TaskGraph;
import uk.ac.manchester.tornado.api.TornadoExecutionPlan;
import uk.ac.manchester.tornado.api.WorkerGrid1D;
import uk.ac.manchester.tornado.api.enums.DataTransferMode;
import uk.ac.manchester.tornado.api.types.arrays.ByteArray;
import uk.ac.manchester.tornado.api.types.arrays.DoubleArray;
import uk.ac.manchester.tornado.api.types.arrays.IntArray;
import uk.ac.manchester.tornado.api.types.arrays.LongArray;
import uk.ac.manchester.tornado.cudf.Cudf;

import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Runs one batch of a compaction on the device: reads the batch's files, orders the rows by key and
 * sequence number, keeps the last row of each key, and writes the survivors to one Parquet file.
 *
 * <p>One plan a capacity; a batch with more rows, or more string bytes, than the plan holds builds
 * a larger one. Batches run one at a time; a caller waiting for its turn can be interrupted, and
 * once closed the compactor takes no more batches.
 */
final class DeviceCompactor implements AutoCloseable {

    /** What a batch wrote. */
    static final class Summary {
        private final int rows;
        private final long minSequence;
        private final long maxSequence;
        private final long retractions;
        private final long[] minKey;
        private final long[] maxKey;
        private final boolean ties;

        Summary(
                int rows,
                long minSequence,
                long maxSequence,
                long retractions,
                long[] minKey,
                long[] maxKey,
                boolean ties) {
            this.rows = rows;
            this.ties = ties;
            this.minSequence = minSequence;
            this.maxSequence = maxSequence;
            this.retractions = retractions;
            this.minKey = minKey;
            this.maxKey = maxKey;
        }

        int rows() {
            return rows;
        }

        long minSequence() {
            return minSequence;
        }

        long maxSequence() {
            return maxSequence;
        }

        long retractions() {
            return retractions;
        }

        long[] minKey() {
            return minKey;
        }

        long[] maxKey() {
            return maxKey;
        }

        /** Whether two rows of a key had the same sequence number and kind. */
        boolean ties() {
            return ties;
        }
    }

    private static final int MIN_CAPACITY = 1 << 20;

    private final KvLayout layout;
    private final ReentrantLock lock = new ReentrantLock();
    private Plan plan;
    private boolean closed;

    DeviceCompactor(KvLayout layout) {
        this.layout = layout;
    }

    /**
     * The capacity a batch of {@code rows} needs: a power of two, at least {@code MIN_CAPACITY}.
     */
    static int capacityFor(long rows) {
        long capacity = MIN_CAPACITY;
        while (capacity < rows) {
            capacity <<= 1;
        }
        if (capacity > (1 << 30)) {
            throw new IllegalArgumentException("A batch of " + rows + " rows is too large");
        }
        return (int) capacity;
    }

    /**
     * Runs one batch.
     *
     * @throws InterruptedException if the calling thread is interrupted before its turn, or was
     *     interrupted already; no device work has started then
     * @throws CancellationException if the compactor was closed; no device work has started then
     */
    Summary compact(
            List<String> inputs,
            long rows,
            long charsPerColumn,
            String output,
            boolean dropDelete,
            int codec)
            throws InterruptedException {
        lock.lockInterruptibly();
        try {
            // A caller cancelled while it waited, or after its factory closed, starts nothing:
            // a plan built now would have no owner to free it.
            if (Thread.currentThread().isInterrupted()) {
                throw new InterruptedException("Device compaction cancelled");
            }
            if (closed) {
                throw new CancellationException("The device compactor is closed");
            }
            return compactLocked(inputs, rows, charsPerColumn, output, dropDelete, codec);
        } finally {
            lock.unlock();
        }
    }

    private Summary compactLocked(
            List<String> inputs,
            long rows,
            long charsPerColumn,
            String output,
            boolean dropDelete,
            int codec) {
        int capacity = capacityFor(rows);
        long chars = Math.max(1024, Long.highestOneBit(Math.max(1, charsPerColumn - 1)) << 1);
        if (plan == null || plan.capacity < capacity || plan.charsStride < chars) {
            if (plan != null) {
                plan.close();
            }
            plan =
                    new Plan(
                            layout,
                            Math.max(capacity, plan == null ? 0 : plan.capacity),
                            Math.max(chars, plan == null ? 0 : plan.charsStride));
        }
        try {
            return plan.run(inputs, rows, output, dropDelete, codec);
        } catch (RuntimeException | Error failure) {
            long needed = charsNeeded(failure);
            // A plan whose execution failed part-way is not run again: the next batch builds anew.
            try {
                plan.close();
            } catch (RuntimeException closing) {
                failure.addSuppressed(closing);
            }
            plan = null;
            if (needed > charsPerColumn) {
                // A string column decoded to more bytes than the guess: read the batch again,
                // sized.
                return compactLocked(inputs, rows, needed, output, dropDelete, codec);
            }
            throw failure;
        }
    }

    private static final Pattern CHARS_NEEDED = Pattern.compile("decodes to (\\d+) bytes, beyond");

    /** The bytes a string column needed, when that is why the read failed, else -1. */
    private static long charsNeeded(Throwable failure) {
        for (Throwable t = failure; t != null; t = t.getCause()) {
            Matcher m = CHARS_NEEDED.matcher(String.valueOf(t.getMessage()));
            if (m.find()) {
                return Long.parseLong(m.group(1));
            }
        }
        return -1;
    }

    @Override
    public void close() {
        lock.lock();
        try {
            closed = true;
            if (plan != null) {
                plan.close();
                plan = null;
            }
        } finally {
            lock.unlock();
        }
    }

    private static final class Plan implements AutoCloseable {
        final int capacity;
        final long charsStride;
        final KvLayout layout;
        final StringBuilder inputPath = new StringBuilder(1 << 16);
        final StringBuilder outputPath = new StringBuilder(4096);
        final long[] rowsHolder = new long[1];
        final int[] sortSize = new int[2];
        final long[] settings = new long[4];
        final IntArray params = new IntArray(CompactKernels.P_SIZE);
        final IntArray count = new IntArray(1);
        final IntArray ties = new IntArray(1);
        final LongArray groupMin;
        final LongArray groupMax;
        final IntArray groupRetract;
        final LongArray edges;
        final TornadoExecutionPlan execution;

        Plan(KvLayout layout, int capacity, long charsStride) {
            this.layout = layout;
            this.capacity = capacity;
            this.charsStride = charsStride;
            int strings = layout.stringColumns.length;
            IntArray ints = new IntArray(Math.max(1, layout.intSlots) * capacity);
            LongArray longs = new LongArray((long) layout.longSlots * capacity);
            DoubleArray doubles = new DoubleArray(Math.max(1, layout.doubleSlots * capacity));
            ByteArray valid = new ByteArray((long) layout.validColumns() * capacity);
            IntArray order = new IntArray(capacity);
            ByteArray mask = new ByteArray(capacity);
            IntArray selected = new IntArray(capacity);
            IntArray gatherMap = new IntArray(capacity);
            IntArray outInts = new IntArray(Math.max(1, layout.outIntSlots) * capacity);
            LongArray outLongs = new LongArray((long) Math.max(1, layout.outLongSlots) * capacity);
            DoubleArray outDoubles = new DoubleArray(Math.max(1, layout.outDoubleSlots * capacity));
            ByteArray outValid = new ByteArray((long) layout.fieldIds.length * capacity);
            IntArray stringOffsets = new IntArray(Math.max(1, strings * (capacity + 1)));
            ByteArray stringChars = new ByteArray(Math.max(1, strings * charsStride));
            IntArray spec = IntArray.fromArray(layout.gatherSpec);
            int groups = capacity / CompactKernels.GROUP;
            this.groupMin = new LongArray(groups);
            this.groupMax = new LongArray(groups);
            this.groupRetract = new IntArray(groups);
            this.edges = new LongArray(2L * layout.keyCount);
            KernelContext context = new KernelContext();

            TaskGraph graph =
                    new TaskGraph("paimon")
                            .transferToDevice(DataTransferMode.EVERY_EXECUTION, params, ties)
                            .transferToDevice(
                                    DataTransferMode.FIRST_EXECUTION,
                                    spec,
                                    ints,
                                    longs,
                                    doubles,
                                    valid,
                                    order,
                                    mask,
                                    selected,
                                    count,
                                    gatherMap,
                                    outInts,
                                    outLongs,
                                    outDoubles,
                                    outValid,
                                    stringOffsets,
                                    stringChars,
                                    groupMin,
                                    groupMax,
                                    groupRetract,
                                    edges)
                            .libraryTask(
                                    "read",
                                    Cudf::readParquetColumns,
                                    inputPath,
                                    0,
                                    0,
                                    layout.readColumns,
                                    layout.readTypes,
                                    rowsHolder,
                                    capacity,
                                    ints,
                                    longs,
                                    doubles,
                                    valid,
                                    true);
            if (strings > 0) {
                graph.libraryTask(
                        "readStrings",
                        Cudf::readParquetStringColumns,
                        inputPath,
                        layout.stringColumns,
                        rowsHolder,
                        capacity,
                        stringOffsets,
                        stringChars,
                        charsStride,
                        valid,
                        layout.readColumns.length);
            }
            graph.task("fold", CompactKernels::foldKind, context, params, longs, ints)
                    .libraryTask("sort", Cudf::sortedOrderLongs, sortSize, longs, capacity, order)
                    .task(
                            "mark",
                            CompactKernels::markSurvivors,
                            context,
                            params,
                            order,
                            longs,
                            ints,
                            mask,
                            ties)
                    .task("unfold", CompactKernels::unfoldKind, context, params, longs)
                    .libraryTask(
                            "select",
                            Cudf::selectedIndices,
                            capacity,
                            capacity,
                            mask,
                            selected,
                            count)
                    .task(
                            "gather",
                            CompactKernels::gatherRows,
                            context,
                            params,
                            count,
                            order,
                            selected,
                            spec,
                            ints,
                            longs,
                            doubles,
                            valid,
                            gatherMap,
                            outInts,
                            outLongs,
                            outDoubles,
                            outValid)
                    .libraryTask(
                            "write",
                            Cudf::writeParquetColumnsWithStrings,
                            outputPath,
                            layout.names,
                            layout.fieldIds,
                            layout.writeTypes,
                            layout.optional,
                            layout.details,
                            new long[] {-1},
                            count,
                            capacity,
                            outInts,
                            outLongs,
                            outDoubles,
                            outValid,
                            stringOffsets,
                            stringChars,
                            valid,
                            gatherMap,
                            settings)
                    .task(
                            "summarize",
                            CompactKernels::summarize,
                            context,
                            params,
                            count,
                            gatherMap,
                            longs,
                            ints,
                            groupMin,
                            groupMax,
                            groupRetract,
                            edges)
                    .transferToHost(
                            DataTransferMode.EVERY_EXECUTION,
                            count,
                            groupMin,
                            groupMax,
                            groupRetract,
                            edges,
                            ties);

            GridScheduler grids = new GridScheduler();
            for (String task : new String[] {"fold", "mark", "unfold", "gather", "summarize"}) {
                WorkerGrid1D grid = new WorkerGrid1D(capacity);
                grid.setLocalWork(CompactKernels.GROUP, 1, 1);
                grids.addWorkerGrid("paimon." + task, grid);
            }
            this.execution = new TornadoExecutionPlan(graph.snapshot()).withGridScheduler(grids);
        }

        Summary run(List<String> inputs, long rows, String output, boolean dropDelete, int codec) {
            inputPath.setLength(0);
            inputPath.append(String.join("\n", inputs));
            outputPath.setLength(0);
            outputPath.append(output);
            rowsHolder[0] = rows;
            sortSize[0] = (int) rows;
            sortSize[1] = layout.keyCount + 1;
            settings[0] = codec;
            settings[1] = 0;
            settings[2] = charsStride;
            settings[3] = rows;
            params.set(CompactKernels.P_ROWS, (int) rows);
            params.set(CompactKernels.P_KEYS, layout.keyCount);
            params.set(CompactKernels.P_CAPACITY, capacity);
            params.set(CompactKernels.P_DROP_DELETE, dropDelete ? 1 : 0);
            params.set(CompactKernels.P_OUTPUTS, layout.numericOutputs);
            ties.set(0, 0);
            execution.execute();

            int written = count.get(0);
            long min = Long.MAX_VALUE;
            long max = Long.MIN_VALUE;
            long retract = 0;
            int groups = (written + CompactKernels.GROUP - 1) / CompactKernels.GROUP;
            for (int g = 0; g < groups; g++) {
                min = Math.min(min, groupMin.get(g));
                max = Math.max(max, groupMax.get(g));
                retract += groupRetract.get(g);
            }
            long[] first = new long[layout.keyCount];
            long[] last = new long[layout.keyCount];
            for (int c = 0; c < layout.keyCount; c++) {
                first[c] = edges.get(c);
                last[c] = edges.get(layout.keyCount + c);
            }
            return new Summary(written, min, max, retract, first, last, ties.get(0) != 0);
        }

        @Override
        public void close() {
            try {
                execution.freeDeviceMemory();
                execution.close();
            } catch (Exception e) {
                throw new IllegalStateException("Cannot release the device plan", e);
            }
        }
    }
}
