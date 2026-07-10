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

import org.apache.paimon.data.InternalArray;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.IOException;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkState;

/** Streaming writer for a row-position-addressable raw float-vector sidecar. */
public class RawVectorSidecarWriter implements Closeable {

    static final int MAGIC = 0x50565231;
    static final int VERSION = 1;
    static final int HEADER_SIZE = 16;

    private final FileIO fileIO;
    private final Path path;
    private final int dimension;
    private final DataOutputStream output;
    private final float[] vectorBuffer;

    private long rowCount;
    private long liveVectorCount;
    private boolean closed;

    public RawVectorSidecarWriter(FileIO fileIO, Path path, int dimension) throws IOException {
        checkArgument(dimension > 0, "Raw vector dimension must be positive: %s.", dimension);
        checkArgument(
                dimension <= (Integer.MAX_VALUE - 1) / Float.BYTES,
                "Raw vector dimension is too large: %s.",
                dimension);
        this.fileIO = fileIO;
        this.path = path;
        this.dimension = dimension;
        this.vectorBuffer = new float[dimension];
        PositionOutputStream stream = fileIO.newOutputStream(path, false);
        this.output = new DataOutputStream(stream);
        this.output.writeInt(MAGIC);
        this.output.writeInt(VERSION);
        this.output.writeInt(dimension);
        this.output.writeInt(recordSize(dimension));
    }

    public void write(@Nullable Object vector) throws IOException {
        checkState(!closed, "Raw vector sidecar writer is already closed.");
        if (vector == null) {
            output.writeBoolean(false);
            for (int i = 0; i < dimension; i++) {
                output.writeFloat(0);
            }
            rowCount++;
            return;
        }

        float[] values = materializeAndValidate(vector);
        output.writeBoolean(true);
        for (float value : values) {
            output.writeFloat(value);
        }
        rowCount++;
        liveVectorCount++;
    }

    public long rowCount() {
        return rowCount;
    }

    public long liveVectorCount() {
        return liveVectorCount;
    }

    public Path path() {
        return path;
    }

    @Override
    public void close() throws IOException {
        if (!closed) {
            closed = true;
            output.close();
        }
    }

    /** Best-effort cleanup used when the owning data-file writer fails or is aborted. */
    public void abort() {
        try {
            close();
        } catch (IOException ignored) {
            // Keep abort best-effort and always try to remove the incomplete sidecar.
        }
        fileIO.deleteQuietly(path);
    }

    static int recordSize(int dimension) {
        return 1 + dimension * Float.BYTES;
    }

    private void checkDimension(int actualDimension) {
        checkArgument(
                actualDimension == dimension,
                "Raw vector dimension must be %s, but was %s.",
                dimension,
                actualDimension);
    }

    private float[] materializeAndValidate(Object vector) {
        if (vector instanceof float[]) {
            float[] values = (float[]) vector;
            checkDimension(values.length);
            for (int i = 0; i < dimension; i++) {
                checkFinite(values[i], i);
            }
            return values;
        }
        if (vector instanceof InternalArray) {
            InternalArray values = (InternalArray) vector;
            checkDimension(values.size());
            for (int i = 0; i < dimension; i++) {
                checkArgument(!values.isNullAt(i), "Vector element at index %s is null.", i);
                float value = values.getFloat(i);
                checkFinite(value, i);
                vectorBuffer[i] = value;
            }
            return vectorBuffer;
        }
        throw new IllegalArgumentException(
                "Unsupported raw vector value type: " + vector.getClass().getName());
    }

    private static void checkFinite(float value, int index) {
        checkArgument(
                !Float.isNaN(value) && !Float.isInfinite(value),
                "Vector element at index %s must be finite, but was %s.",
                index,
                value);
    }
}
