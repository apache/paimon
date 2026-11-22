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

package org.apache.paimon.globalindex.bitmap;

import org.apache.paimon.fs.CloseShieldOutputStream;
import org.apache.paimon.fs.OffsetSeekableInputStream;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.KeySerializer;
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.globalindex.io.GlobalIndexFileReader;
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Equal;
import org.apache.paimon.predicate.IsNotNull;
import org.apache.paimon.predicate.IsNull;
import org.apache.paimon.predicate.LeafPredicate;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.RoaringNavigableMap64;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkState;

/** Bitmap sections in the order of their manifest's extra field IDs. */
public final class CentroidBitmapIndex {

    private static final int BITMAP = 1;
    private static final int ENTRY_HEADER_LENGTH = Byte.BYTES + Long.BYTES + Integer.BYTES;

    private CentroidBitmapIndex() {}

    public static long schemaId(byte[] metadata) {
        return metadataPayload(metadata).getLong();
    }

    private static ByteBuffer metadataPayload(byte[] metadata) {
        checkArgument(
                metadata != null && metadata.length >= Long.BYTES,
                "Truncated centroid bitmap metadata.");
        return ByteBuffer.wrap(metadata);
    }

    public static RoaringNavigableMap64 match(
            GlobalIndexFileReader files,
            GlobalIndexIOMeta bitmap,
            int[] extraFieldIds,
            RowType rowType,
            RowType buildType,
            List<LeafPredicate> predicates)
            throws IOException {
        checkArgument(!predicates.isEmpty(), "Centroid bitmap lookup requires a predicate.");
        ByteBuffer metadata = metadataPayload(bitmap.metadata());
        metadata.getLong(); // build schema ID
        Map<Integer, Section> sections = readDirectory(metadata, extraFieldIds, bitmap.fileSize());
        try (SeekableInputStream input = files.getInputStream(bitmap)) {
            RoaringNavigableMap64 result = null;
            for (LeafPredicate predicate : predicates) {
                int fieldId = rowType.getField(predicate.fieldRefOptional().get().name()).id();
                Section section = sections.get(fieldId);
                checkState(section != null, "Missing field %s in %s.", fieldId, bitmap.filePath());
                checkArgument(
                        section.indexType == BITMAP,
                        "Unsupported centroid scalar index type %s for field %s.",
                        section.indexType,
                        fieldId);
                KeySerializer serializer = KeySerializer.create(buildType.getField(fieldId).type());
                GlobalIndexIOMeta sectionMeta =
                        new GlobalIndexIOMeta(
                                bitmap.filePath(),
                                section.length,
                                bitmap.rowCount(),
                                section.metadata);
                try (BitmapIndexReader reader =
                        new BitmapIndexReader(
                                serializer, ignored -> section.open(input), sectionMeta)) {
                    RoaringNavigableMap64 rows;
                    if (Equal.INSTANCE.equals(predicate.function())) {
                        rows = reader.visitEqual(predicate.literals().get(0)).get().results();
                    } else if (IsNull.INSTANCE.equals(predicate.function())) {
                        rows = reader.visitIsNull().get().results();
                    } else if (IsNotNull.INSTANCE.equals(predicate.function())) {
                        rows = reader.visitIsNotNull().get().results();
                    } else {
                        rows = reader.visitIn(predicate.literals()).get().results();
                    }
                    result = result == null ? rows : RoaringNavigableMap64.and(result, rows);
                    if (result.isEmpty()) {
                        break;
                    }
                }
            }
            return result;
        }
    }

    private static Map<Integer, Section> readDirectory(
            ByteBuffer directory, int[] extraFieldIds, long sectionsLength) {
        int fieldCount = extraFieldIds.length;
        checkArgument(
                fieldCount > 0 && fieldCount <= directory.remaining() / ENTRY_HEADER_LENGTH,
                "Invalid centroid bitmap field count.");
        Map<Integer, Section> sections = new HashMap<>();
        long offset = 0;
        for (int i = 0; i < fieldCount; i++) {
            int fieldId = extraFieldIds[i];
            int indexType = Byte.toUnsignedInt(directory.get());
            long length = directory.getLong();
            int metaLength = directory.getInt();
            checkArgument(
                    length > 0
                            && length <= sectionsLength - offset
                            && metaLength >= 0
                            && metaLength
                                    <= directory.remaining()
                                            - (fieldCount - i - 1) * ENTRY_HEADER_LENGTH,
                    "Invalid centroid bitmap section for field %s.",
                    fieldId);
            byte[] metadata = new byte[metaLength];
            directory.get(metadata);
            checkArgument(
                    sections.put(fieldId, new Section(indexType, offset, length, metadata)) == null,
                    "Duplicate centroid bitmap field %s.",
                    fieldId);
            offset += length;
        }
        checkArgument(
                offset == sectionsLength && !directory.hasRemaining(),
                "Invalid centroid bitmap directory.");
        return sections;
    }

    /** Writes one complete Bitmap per column to the same output stream. */
    public static final class Writer implements AutoCloseable {

        private final String name;
        private final int fieldCount;
        private final long rowCount;
        private final PositionOutputStream output;
        private final ByteArrayOutputStream directoryBytes = new ByteArrayOutputStream();
        private final DataOutputStream directory = new DataOutputStream(directoryBytes);
        private int written;

        public Writer(GlobalIndexFileWriter files, long schemaId, int fieldCount, long rowCount)
                throws IOException {
            this.name = files.newFileName("centroid-bitmap");
            this.fieldCount = fieldCount;
            this.rowCount = rowCount;
            directory.writeLong(schemaId);
            this.output = files.newOutputStream(name);
        }

        public void writeColumn(DataField field, Options options, ColumnRows rows)
                throws IOException {
            long offset = output.getPos();
            GlobalIndexFileWriter sectionWriter =
                    new GlobalIndexFileWriter() {
                        @Override
                        public String newFileName(String prefix) {
                            return name;
                        }

                        @Override
                        public PositionOutputStream newOutputStream(String fileName) {
                            return new CloseShieldOutputStream(output) {
                                @Override
                                public long getPos() throws IOException {
                                    return super.getPos() - offset;
                                }
                            };
                        }
                    };
            ResultEntry section;
            try (BitmapGlobalIndexWriter writer =
                    new BitmapGlobalIndexer(field, options).createWriter(sectionWriter)) {
                rows.writeTo(writer);
                section = writer.finish().get(0);
            }
            checkState(
                    section.rowCount() == rowCount,
                    "Centroid bitmap and vector row counts differ.");
            directory.writeByte(BITMAP);
            directory.writeLong(output.getPos() - offset);
            directory.writeInt(section.meta().length);
            directory.write(section.meta());
            written++;
        }

        public ResultEntry finish() {
            checkState(written == fieldCount, "Incomplete centroid bitmap file.");
            return new ResultEntry(name, rowCount, directoryBytes.toByteArray());
        }

        @Override
        public void close() throws IOException {
            output.close();
        }
    }

    /** Sorted rows of one extra field. */
    @FunctionalInterface
    public interface ColumnRows {
        void writeTo(BitmapGlobalIndexWriter writer) throws IOException;
    }

    private static final class Section {
        private final int indexType;
        private final long offset;
        private final long length;
        private final byte[] metadata;

        private Section(int indexType, long offset, long length, byte[] metadata) {
            this.indexType = indexType;
            this.offset = offset;
            this.length = length;
            this.metadata = metadata;
        }

        private SeekableInputStream open(SeekableInputStream input) throws IOException {
            if (offset == 0) {
                input.seek(0);
            }
            return new OffsetSeekableInputStream(input, offset, length) {
                @Override
                public void seek(long desired) throws IOException {
                    if (desired < 0 || desired > length) {
                        throw new IOException("Seek outside centroid bitmap section.");
                    }
                    super.seek(desired);
                }

                @Override
                public void close() {
                    // The caller owns the shared input stream.
                }
            };
        }
    }
}
