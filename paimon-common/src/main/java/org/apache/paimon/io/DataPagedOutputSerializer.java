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

package org.apache.paimon.io;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.SimpleCollectingOutputView;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.utils.MathUtils;

import java.io.IOException;
import java.util.ArrayList;

import static org.apache.paimon.data.serializer.BinaryRowSerializer.getSerializedRowLength;
import static org.apache.paimon.data.serializer.BinaryRowSerializer.serializeWithoutLength;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * A paged output serializer that efficiently handles serialization of rows using memory pages.
 *
 * <p>This serializer uses a two-phase approach:
 *
 * <ol>
 *   <li><strong>Initial phase:</strong> Writes data to an initial buffer until it reaches the
 *       specified page size.
 *   <li><strong>Paged phase:</strong> Once the initial buffer exceeds the page size, switches to
 *       using {@link SimpleCollectingOutputView} with allocated memory segments for efficient
 *       memory management.
 * </ol>
 *
 * <p>The design ensures optimal performance for both small datasets (using single buffer) and large
 * datasets (using paged memory allocation).
 */
public class DataPagedOutputSerializer {

    private final InternalRowSerializer serializer;
    private final DataOutputSerializer initialOut;
    private final int pageSize;

    private SimpleCollectingOutputView pagedOut;

    /**
     * Constructs a new DataPagedOutputSerializer with the specified parameters.
     *
     * @param serializer the internal row serializer used for converting rows to binary format
     * @param startSize the initial buffer size for storing serialized data
     * @param pageSize the maximum size of each memory page before switching to paged mode
     */
    public DataPagedOutputSerializer(
            InternalRowSerializer serializer, int startSize, int pageSize) {
        this.serializer = serializer;
        this.initialOut = new DataOutputSerializer(startSize);
        this.pageSize = pageSize;
    }

    @VisibleForTesting
    SimpleCollectingOutputView pagedOut() {
        return pagedOut;
    }

    /**
     * Serializes a binary row to the output.
     *
     * <p>Depending on the current state and available space, this method will either:
     *
     * <ul>
     *   <li>Write directly to the initial buffer if there's sufficient space remaining
     *   <li>Switch to paged mode and write to memory segments once the initial buffer fills up
     * </ul>
     *
     * @param row the binary row to serialize
     * @throws IOException if an I/O error occurs during serialization
     */
    public void write(InternalRow row) throws IOException {
        if (pagedOut != null) {
            serializer.serializeToPages(row, pagedOut);
        } else {
            BinaryRow binaryRow = serializer.toBinaryRow(row);
            int serializedSize = getSerializedRowLength(binaryRow);
            if (initialOut.length() + serializedSize > pageSize) {
                pagedOut = toPagedOutput(initialOut, pageSize);
                serializer.serializeToPages(row, pagedOut);
            } else {
                initialOut.writeInt(binaryRow.getSizeInBytes());
                serializeWithoutLength(binaryRow, initialOut);
            }
        }
    }

    private static SimpleCollectingOutputView toPagedOutput(
            DataOutputSerializer output, int pageSize) throws IOException {
        checkArgument(output.length() <= pageSize);
        SimpleCollectingOutputView pagedOut =
                new SimpleCollectingOutputView(
                        new ArrayList<>(),
                        () -> MemorySegment.allocateHeapMemory(pageSize),
                        pageSize);
        pagedOut.write(output.getSharedBuffer(), 0, output.length());
        return pagedOut;
    }

    public SimpleCollectingOutputView result() throws IOException {
        if (pagedOut != null) {
            return pagedOut;
        }

        int pageSize = MathUtils.roundUpToPowerOf2(initialOut.length());
        return toPagedOutput(initialOut, pageSize);
    }
}
