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

package org.apache.paimon.index.pkvector;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.function.LongPredicate;

import static org.apache.paimon.index.pkvector.RawVectorSidecarWriter.HEADER_SIZE;
import static org.apache.paimon.index.pkvector.RawVectorSidecarWriter.MAGIC;
import static org.apache.paimon.index.pkvector.RawVectorSidecarWriter.VERSION;
import static org.apache.paimon.index.pkvector.RawVectorSidecarWriter.recordSize;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Random-access reader for raw vector sidecars. This class is not thread-safe. */
public class RawVectorSidecarReader implements Closeable {

    private static final Comparator<Candidate> BEST_FIRST =
            (left, right) -> {
                int distance = Float.compare(left.distance, right.distance);
                return distance != 0 ? distance : Long.compare(left.rowPosition, right.rowPosition);
            };

    private final SeekableInputStream input;
    private final DataInputStream dataInput;
    private final int dimension;
    private final int recordSize;
    private final long rowCount;
    private long nextSequentialPosition;

    public RawVectorSidecarReader(FileIO fileIO, Path path) throws IOException {
        long fileSize = fileIO.getFileSize(path);
        checkArgument(
                fileSize >= HEADER_SIZE, "Raw vector sidecar %s is shorter than its header.", path);
        this.input = fileIO.newInputStream(path);
        this.dataInput = new DataInputStream(input);
        int magic = dataInput.readInt();
        checkArgument(magic == MAGIC, "File %s is not a raw vector sidecar.", path);
        int version = dataInput.readInt();
        checkArgument(version == VERSION, "Unsupported raw vector sidecar version: %s.", version);
        this.dimension = dataInput.readInt();
        this.recordSize = dataInput.readInt();
        checkArgument(dimension > 0, "Raw vector sidecar dimension must be positive.");
        checkArgument(
                recordSize == recordSize(dimension),
                "Raw vector sidecar record size %s does not match dimension %s.",
                recordSize,
                dimension);
        long payloadSize = fileSize - HEADER_SIZE;
        checkArgument(
                payloadSize % recordSize == 0,
                "Raw vector sidecar %s has a truncated record.",
                path);
        this.rowCount = payloadSize / recordSize;
        this.nextSequentialPosition = 0;
    }

    public int dimension() {
        return dimension;
    }

    public long rowCount() {
        return rowCount;
    }

    /** Raw sidecars use row positions directly as segment ordinals. */
    public long rowPositionForOrdinal(long ordinal) {
        checkRowPosition(ordinal);
        return ordinal;
    }

    public float[] readVector(long rowPosition) throws IOException {
        checkRowPosition(rowPosition);
        nextSequentialPosition = -1;
        input.seek(HEADER_SIZE + rowPosition * recordSize);
        if (!dataInput.readBoolean()) {
            return null;
        }
        float[] vector = new float[dimension];
        for (int i = 0; i < dimension; i++) {
            vector[i] = dataInput.readFloat();
        }
        return vector;
    }

    /** Positions this reader for a sequential pass over all row-position records. */
    public void rewind() throws IOException {
        input.seek(HEADER_SIZE);
        nextSequentialPosition = 0;
    }

    /**
     * Reads the next record into a caller-owned reusable buffer and returns whether it is non-null.
     */
    public boolean readNextVector(float[] reuse) throws IOException {
        checkArgument(
                reuse.length == dimension,
                "Reusable vector buffer dimension must be %s, but is %s.",
                dimension,
                reuse.length);
        checkArgument(
                nextSequentialPosition >= 0,
                "Raw vector sequential read requires rewind after random access.");
        checkArgument(
                nextSequentialPosition < rowCount,
                "No raw vector remains after row position %s.",
                nextSequentialPosition);
        boolean present = dataInput.readBoolean();
        for (int i = 0; i < dimension; i++) {
            reuse[i] = dataInput.readFloat();
        }
        nextSequentialPosition++;
        return present;
    }

    /** Performs an exact top-k scan and excludes positions deleted in the selected snapshot. */
    public List<Candidate> search(
            float[] query, String metric, int limit, LongPredicate excludedPosition)
            throws IOException {
        checkArgument(query.length == dimension, "Query vector dimension must be %s.", dimension);
        checkArgument(limit > 0, "Vector search limit must be positive: %s.", limit);
        checkArgument(
                "l2".equals(metric) || "cosine".equals(metric) || "inner_product".equals(metric),
                "Unsupported raw vector distance metric: %s.",
                metric);
        for (int i = 0; i < query.length; i++) {
            checkArgument(
                    !Float.isNaN(query[i]) && !Float.isInfinite(query[i]),
                    "Query vector element at index %s must be finite.",
                    i);
        }

        PriorityQueue<Candidate> nearest = new PriorityQueue<>(limit, BEST_FIRST.reversed());
        float[] vector = new float[dimension];
        nextSequentialPosition = -1;
        input.seek(HEADER_SIZE);
        for (long rowPosition = 0; rowPosition < rowCount; rowPosition++) {
            boolean present = dataInput.readBoolean();
            for (int i = 0; i < dimension; i++) {
                vector[i] = dataInput.readFloat();
            }
            if (!present || excludedPosition.test(rowPosition)) {
                continue;
            }

            Candidate candidate = new Candidate(rowPosition, distance(query, vector, metric));
            if (nearest.size() < limit) {
                nearest.add(candidate);
            } else if (BEST_FIRST.compare(candidate, nearest.peek()) < 0) {
                nearest.poll();
                nearest.add(candidate);
            }
        }

        List<Candidate> result = new ArrayList<>(nearest);
        Collections.sort(result, BEST_FIRST);
        return result;
    }

    @Override
    public void close() throws IOException {
        dataInput.close();
    }

    private void checkRowPosition(long rowPosition) {
        checkArgument(
                rowPosition >= 0 && rowPosition < rowCount,
                "Raw vector row position %s is outside [0, %s).",
                rowPosition,
                rowCount);
    }

    private float distance(float[] query, float[] vector, String metric) {
        if ("l2".equals(metric)) {
            double squaredDistance = 0;
            for (int i = 0; i < dimension; i++) {
                double delta = vector[i] - query[i];
                squaredDistance += delta * delta;
            }
            return (float) squaredDistance;
        }

        double dot = 0;
        double queryNorm = 0;
        double vectorNorm = 0;
        for (int i = 0; i < dimension; i++) {
            dot += query[i] * vector[i];
            queryNorm += query[i] * query[i];
            vectorNorm += vector[i] * vector[i];
        }
        if ("inner_product".equals(metric)) {
            return (float) -dot;
        }
        if (queryNorm == 0 || vectorNorm == 0) {
            return 1;
        }
        double similarity = dot / Math.sqrt(queryNorm * vectorNorm);
        similarity = Math.max(-1, Math.min(1, similarity));
        return (float) (1 - similarity);
    }

    /** One exact raw-vector candidate. Lower distance is better. */
    public static class Candidate {

        private final long rowPosition;
        private final float distance;

        private Candidate(long rowPosition, float distance) {
            this.rowPosition = rowPosition;
            this.distance = distance;
        }

        public long rowPosition() {
            return rowPosition;
        }

        public float distance() {
            return distance;
        }
    }
}
