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

package org.apache.paimon.globalindex.rtree;

import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.geo.BoundingBox;
import org.apache.paimon.globalindex.GlobalIndexSingleColumnWriter;
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;

import org.davidmoten.hilbert.HilbertCurve;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkState;

/** Buffers only one partition's envelopes, then writes a Hilbert-packed immutable tree. */
public class RTreeGlobalIndexWriter implements GlobalIndexSingleColumnWriter, Closeable {

    private final GlobalIndexFileWriter files;
    private final int pageSize;
    private final int partitionRows;
    private final List<RTreeIndexFile.Entry> entries = new ArrayList<>();
    private final List<Long> nulls = new ArrayList<>();
    private final List<Long> nonNulls = new ArrayList<>();
    private final List<ResultEntry> results = new ArrayList<>();
    private long firstRow = Long.MAX_VALUE;
    private long lastRow = -1;
    private boolean unorderedIds;
    private boolean finished;

    RTreeGlobalIndexWriter(GlobalIndexFileWriter files, int pageSize, int partitionRows) {
        this.files = files;
        this.pageSize = pageSize;
        this.partitionRows = partitionRows;
    }

    @Override
    public void write(@Nullable Object key, long relativeRowId) {
        checkState(!finished, "R-tree writer is already finished or closed.");
        checkArgument(
                relativeRowId >= 0 && relativeRowId < Long.MAX_VALUE,
                "R-tree relative row ID must be non-negative and less than Long.MAX_VALUE.");
        BoundingBox box = key == null ? null : BoundingBox.fromWkb((byte[]) key);
        if (nulls.size() + nonNulls.size() == partitionRows) {
            flush();
        }
        unorderedIds |= relativeRowId <= lastRow;
        firstRow = Math.min(firstRow, relativeRowId);
        lastRow = Math.max(lastRow, relativeRowId);
        if (box == null) {
            nulls.add(relativeRowId);
        } else {
            nonNulls.add(relativeRowId);
            if (!box.isEmpty()) {
                entries.add(new RTreeIndexFile.Entry(box, relativeRowId));
            }
        }
    }

    @Override
    public List<ResultEntry> finish() {
        checkState(!finished, "R-tree writer is already finished or closed.");
        finished = true;
        flush();
        return new ArrayList<>(results);
    }

    private void flush() {
        if (nulls.isEmpty() && nonNulls.isEmpty()) {
            return;
        }
        if (unorderedIds) {
            verifyUniqueIds();
        }
        BoundingBox bounds = BoundingBox.empty();
        for (RTreeIndexFile.Entry entry : entries) {
            bounds = bounds.union(entry.box);
        }
        final BoundingBox totalBounds = bounds;
        final HilbertCurve curve = HilbertCurve.bits(16).dimensions(2);
        // Compute keys once; comparisons should not recompute a Hilbert value.
        List<SortedEntry> sorted = new ArrayList<>(entries.size());
        for (RTreeIndexFile.Entry entry : entries) {
            BoundingBox box = entry.box;
            long x =
                    normalize(
                            box.minX() / 2 + box.maxX() / 2,
                            totalBounds.minX(),
                            totalBounds.maxX());
            long y =
                    normalize(
                            box.minY() / 2 + box.maxY() / 2,
                            totalBounds.minY(),
                            totalBounds.maxY());
            sorted.add(new SortedEntry(entry, curve.index(x, y).longValue()));
        }
        sorted.sort(
                Comparator.comparingLong((SortedEntry e) -> e.hilbert)
                        .thenComparingLong(e -> e.entry.id));
        entries.clear();
        for (SortedEntry entry : sorted) {
            entries.add(entry.entry);
        }

        RTreeIndexFile.Metadata meta =
                new RTreeIndexFile.Metadata(
                        pageSize,
                        entries.size(),
                        nulls.size(),
                        nonNulls.size(),
                        firstRow,
                        lastRow,
                        bounds);
        String name = files.newFileName("rtree");
        try (PositionOutputStream output = files.newOutputStream(name)) {
            output.write(meta.bytes);
            List<RTreeIndexFile.Entry> level = entries;
            int depth = 0;
            int pageId = 0;
            while (!level.isEmpty()) {
                List<RTreeIndexFile.Entry> parents = new ArrayList<>();
                for (int start = 0; start < level.size(); start += pageSize) {
                    int count = Math.min(pageSize, level.size() - start);
                    byte[] bytes = new byte[RTreeIndexFile.pageBytes(pageSize)];
                    ByteBuffer page = ByteBuffer.wrap(bytes);
                    page.putInt(count).putInt(depth);
                    BoundingBox pageBounds = BoundingBox.empty();
                    for (int i = start; i < start + count; i++) {
                        RTreeIndexFile.Entry entry = level.get(i);
                        RTreeIndexFile.putBox(page, entry.box);
                        page.putLong(entry.id);
                        pageBounds = pageBounds.union(entry.box);
                    }
                    RTreeIndexFile.checksum(bytes);
                    output.write(bytes);
                    parents.add(new RTreeIndexFile.Entry(pageBounds, pageId++));
                }
                if (parents.size() == 1) {
                    break;
                }
                level = parents;
                depth++;
            }
            writeRowBlock(output, nulls);
            writeRowBlock(output, nonNulls);
        } catch (IOException e) {
            finished = true;
            throw new UncheckedIOException("Failed to write R-tree index.", e);
        }
        results.add(new ResultEntry(name, meta.rowCount(), meta.bytes));
        entries.clear();
        nulls.clear();
        nonNulls.clear();
        firstRow = Long.MAX_VALUE;
        lastRow = -1;
        unorderedIds = false;
    }

    private void verifyUniqueIds() {
        long[] ids = new long[nulls.size() + nonNulls.size()];
        int pos = 0;
        for (long id : nulls) {
            ids[pos++] = id;
        }
        for (long id : nonNulls) {
            ids[pos++] = id;
        }
        Arrays.sort(ids);
        for (int i = 1; i < ids.length; i++) {
            if (ids[i - 1] == ids[i]) {
                finished = true;
                throw new IllegalArgumentException("Duplicate R-tree source row ID.");
            }
        }
    }

    private static void writeRowBlock(PositionOutputStream output, List<Long> ids)
            throws IOException {
        byte[] block = new byte[ids.size() * Long.BYTES + Integer.BYTES];
        ByteBuffer buffer = ByteBuffer.wrap(block);
        for (long id : ids) {
            buffer.putLong(id);
        }
        RTreeIndexFile.checksum(block);
        output.write(block);
    }

    private static long normalize(double center, double min, double max) {
        // Halving first avoids overflow when the extent spans most of the double domain.
        double extent = max / 2 - min / 2;
        if (extent == 0) {
            return 0;
        }
        return Math.max(0L, Math.min(65535L, (long) (65535 * ((center / 2 - min / 2) / extent))));
    }

    @Override
    public void close() {
        finished = true;
        entries.clear();
        nulls.clear();
        nonNulls.clear();
    }

    private static final class SortedEntry {
        final RTreeIndexFile.Entry entry;
        final long hilbert;

        private SortedEntry(RTreeIndexFile.Entry entry, long hilbert) {
            this.entry = entry;
            this.hilbert = hilbert;
        }
    }
}
