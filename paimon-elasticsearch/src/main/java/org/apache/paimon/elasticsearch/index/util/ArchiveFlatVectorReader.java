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

package org.apache.paimon.elasticsearch.index.util;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.hnsw.FlatVectorScorerUtil;
import org.apache.lucene.codecs.lucene103.Lucene103Codec;
import org.apache.lucene.codecs.lucene99.Lucene99FlatVectorsReader;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.elasticsearch.vectorindex.api.RawVectorProvider;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;

/**
 * Reads raw float32 vectors from a Lucene 99 {@code FlatVectorsFormat} {@code .vec} file packed
 * inside the paimon-elasticsearch archive.
 *
 * <p>Strategy: wrap the archive's file slice map in an {@link ArchiveDirectory}, parse {@code
 * _0.si} + {@code _0.fnm} via the standard Lucene codec, then build a real {@link
 * Lucene99FlatVectorsReader} on top — every codec header / per-field offset detail is delegated to
 * Lucene itself.
 */
public class ArchiveFlatVectorReader implements RawVectorProvider, Closeable {

    private static final int SEGMENT_ID_LENGTH = 16;

    private final ArchiveDirectory directory;
    private final Lucene99FlatVectorsReader delegate;
    private final String fieldName;
    private final FloatVectorValues values;

    public ArchiveFlatVectorReader(
            ArchiveByteRangeReader archive, Map<String, long[]> fileOffsets, String fieldName)
            throws IOException {
        this.fieldName = fieldName;
        this.directory = new ArchiveDirectory(fileOffsets, archive);

        // 1. Find the .si file and derive the segment name (e.g. "_0.si" -> "_0").
        String siFile =
                findFileWithSuffix(fileOffsets, ".si")
                        .orElseThrow(() -> new IOException(".si not found in archive"));
        String segmentName = siFile.substring(0, siFile.length() - ".si".length());

        // 2. Parse the segmentID (16 raw bytes) from the .si codec index header so that the
        //    standard SegmentInfoFormat.read() — which validates this ID via CodecUtil —
        //    succeeds.
        byte[] segmentId;
        try (IndexInput in = directory.openInput(siFile, IOContext.DEFAULT)) {
            segmentId = readSegmentIdFromCodecHeader(in);
        }

        // 3. Use a Lucene 103 codec to read SegmentInfo + FieldInfos. DiskBBQCodec extends
        //    FilterCodec(Lucene103Codec) and only overrides knnVectorsFormat, so the standard
        //    103 codec is functionally equivalent for .si and .fnm.
        Codec codec = new Lucene103Codec();
        SegmentInfo segmentInfo =
                codec.segmentInfoFormat()
                        .read(directory, segmentName, segmentId, IOContext.DEFAULT);
        FieldInfos fieldInfos =
                codec.fieldInfosFormat().read(directory, segmentInfo, "", IOContext.DEFAULT);

        // 4. Construct the Lucene99FlatVectorsReader directly. This bypasses DiskBBQ's IVF
        //    layer (which would also load .cenivf/.clivf/.mivf — already in heap on the
        //    paimon-elasticsearch side) and reads only .vec/.vemf for raw vector lookup.
        SegmentReadState state =
                new SegmentReadState(directory, segmentInfo, fieldInfos, IOContext.DEFAULT, "");
        this.delegate =
                new Lucene99FlatVectorsReader(
                        state, FlatVectorScorerUtil.getLucene99FlatVectorsScorer());
        this.values = delegate.getFloatVectorValues(fieldName);
        if (values == null) {
            delegate.close();
            directory.close();
            throw new IOException("Field '" + fieldName + "' has no FloatVectorValues");
        }
    }

    public int numVectors() {
        return values.size();
    }

    @Override
    public synchronized float[] getVector(long ordinal) throws IOException {
        if (ordinal < 0 || ordinal >= values.size()) {
            return null;
        }
        // Lucene 10's FloatVectorValues exposes random access by ordinal directly.
        return values.vectorValue((int) ordinal).clone();
    }

    @Override
    public void close() throws IOException {
        try {
            delegate.close();
        } finally {
            directory.close();
        }
    }

    private static Optional<String> findFileWithSuffix(
            Map<String, long[]> fileOffsets, String suffix) {
        for (String name : fileOffsets.keySet()) {
            if (name.endsWith(suffix)) {
                return Optional.of(name);
            }
        }
        return Optional.empty();
    }

    /**
     * Read just the segmentID (16 raw bytes) from a Lucene codec index header, leaving the stream
     * pointer wherever it ended up — caller closes the input. Layout per {@code
     * CodecUtil.writeIndexHeader}:
     *
     * <pre>
     *   [4 bytes]  magic = CODEC_MAGIC (big-endian)
     *   VInt+UTF   codec name
     *   [4 bytes]  version (big-endian)
     *   [16 bytes] segment ID (raw)
     *   VInt+UTF   suffix
     * </pre>
     */
    private static byte[] readSegmentIdFromCodecHeader(IndexInput in) throws IOException {
        in.readInt(); // magic
        in.readString(); // codec name (Lucene IndexInput readString = VInt length + UTF-8)
        in.readInt(); // version
        byte[] id = new byte[SEGMENT_ID_LENGTH];
        in.readBytes(id, 0, SEGMENT_ID_LENGTH);
        return id;
    }

    /** Callback for reading a byte range from the archive file. */
    @FunctionalInterface
    public interface ArchiveByteRangeReader {
        byte[] readRange(long offset, int length) throws IOException;
    }
}
