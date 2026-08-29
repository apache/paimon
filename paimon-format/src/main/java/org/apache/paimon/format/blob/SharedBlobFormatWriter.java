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

package org.apache.paimon.format.blob;

import org.apache.paimon.data.Blob;
import org.apache.paimon.data.BlobDescriptor;
import org.apache.paimon.data.BlobFetchMetricReporter;
import org.apache.paimon.data.BlobPlaceholder;
import org.apache.paimon.data.BlobRef;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FileAwareFormatWriter;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.DeltaVarintCompressor;
import org.apache.paimon.utils.LongArrayList;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.StreamUtils.intToLittleEndian;

/**
 * {@link FormatWriter} for a shared blob file.
 *
 * <p>The data region stores every exact {@link BlobDescriptor} once. A second index maps each
 * logical row to a physical blob ordinal, allowing many rows to reference the same payload without
 * copying it again.
 */
public class SharedBlobFormatWriter implements FileAwareFormatWriter {

    static final byte VERSION = 1;
    static final int MAGIC_NUMBER = 0x4C424853; // "SHBL" in little endian
    static final long NULL_REFERENCE = -1L;
    static final long PLACEHOLDER_REFERENCE = -2L;

    private final PositionOutputStream out;
    private final BlobElementSerializer.Writer elementWriter;
    private final LongArrayList physicalBlobLengths;
    private final LongArrayList rowReferences;
    private final Map<BlobDescriptor, Integer> physicalBlobs;

    public SharedBlobFormatWriter(
            PositionOutputStream out,
            RowType type,
            boolean writeNullOnMissingFile,
            boolean writeNullOnFetchFailure,
            BlobFetchMetricReporter blobFetchMetricReporter,
            int copyBufferSize) {
        checkArgument(type.getFieldCount() == 1, "SharedBlobFormatWriter only supports one field.");
        this.out = out;
        this.elementWriter =
                new RawBlobElementSerializer()
                        .createWriter(
                                out,
                                type.getFieldNames().get(0),
                                null,
                                writeNullOnMissingFile,
                                writeNullOnFetchFailure,
                                blobFetchMetricReporter,
                                copyBufferSize);
        this.physicalBlobLengths = new LongArrayList(16);
        this.rowReferences = new LongArrayList(16);
        this.physicalBlobs = new HashMap<>();
    }

    @Override
    public void setFile(Path file) {
        elementWriter.setFile(file);
    }

    @Override
    public boolean deleteFileUponAbort() {
        return true;
    }

    @Override
    public void addElement(InternalRow element) throws IOException {
        checkArgument(
                element.getFieldCount() == 1, "SharedBlobFormatWriter only supports one field.");
        if (element.isNullAt(0)) {
            rowReferences.add(NULL_REFERENCE);
            return;
        }

        Blob blob = element.getBlob(0);
        if (blob == BlobPlaceholder.INSTANCE) {
            rowReferences.add(PLACEHOLDER_REFERENCE);
            return;
        }
        checkArgument(
                blob != null && blob.getClass() == BlobRef.class,
                "Shared blob fields require an exact BlobRef with a stable descriptor; "
                        + "inline BlobData and custom Blob implementations are not supported.");

        BlobDescriptor descriptor = blob.toDescriptor();
        Integer physicalBlob = physicalBlobs.get(descriptor);
        if (physicalBlob != null) {
            rowReferences.add(physicalBlob);
            return;
        }

        long length = elementWriter.write(element);
        if (length == BlobFormatWriter.NULL_LENGTH) {
            rowReferences.add(NULL_REFERENCE);
            return;
        }

        int ordinal = physicalBlobLengths.size();
        physicalBlobLengths.add(length);
        physicalBlobs.put(descriptor, ordinal);
        rowReferences.add(ordinal);
    }

    @Override
    public boolean reachTargetSize(boolean suggestedCheck, long targetSize) throws IOException {
        return out.getPos() >= targetSize;
    }

    @Override
    public void close() throws IOException {
        Throwable primary = null;
        try {
            byte[] physicalIndex = DeltaVarintCompressor.compressLongArrayList(physicalBlobLengths);
            byte[] rowIndex = DeltaVarintCompressor.compressLongArrayList(rowReferences);
            out.write(physicalIndex);
            out.write(rowIndex);
            out.write(intToLittleEndian(physicalIndex.length));
            out.write(intToLittleEndian(rowIndex.length));
            out.write(intToLittleEndian(MAGIC_NUMBER));
            out.write(VERSION);
        } catch (RuntimeException | Error | IOException e) {
            primary = e;
            throw e;
        } finally {
            if (primary == null) {
                elementWriter.close();
            } else {
                try {
                    elementWriter.close();
                } catch (RuntimeException | Error | IOException suppressed) {
                    primary.addSuppressed(suppressed);
                }
            }
        }
    }
}
