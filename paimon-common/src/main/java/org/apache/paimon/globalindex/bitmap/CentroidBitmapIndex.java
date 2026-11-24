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
import org.apache.paimon.predicate.And;
import org.apache.paimon.predicate.CompoundPredicate;
import org.apache.paimon.predicate.Equal;
import org.apache.paimon.predicate.In;
import org.apache.paimon.predicate.IsNotNull;
import org.apache.paimon.predicate.IsNull;
import org.apache.paimon.predicate.LeafPredicate;
import org.apache.paimon.predicate.Or;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.IOFunction;
import org.apache.paimon.utils.RoaringNavigableMap64;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkState;

/**
 * Bitmap sections ordered by the manifest's extra field IDs, with offsets for sections 2 onward.
 */
public final class CentroidBitmapIndex {

    private CentroidBitmapIndex() {}

    public static RoaringNavigableMap64 match(
            GlobalIndexFileReader files,
            GlobalIndexIOMeta bitmap,
            int[] extraFieldIds,
            RowType rowType,
            RowType buildType,
            Predicate predicate)
            throws IOException {
        Map<Integer, Section> sections =
                readDirectory(bitmap.metadata(), extraFieldIds, bitmap.fileSize());
        try (SeekableInputStream input = files.getInputStream(bitmap)) {
            return evaluate(
                    predicate,
                    fieldName -> {
                        int fieldId = rowType.getField(fieldName).id();
                        Section section = sections.get(fieldId);
                        checkState(
                                section != null,
                                "Missing field %s in %s.",
                                fieldId,
                                bitmap.filePath());
                        KeySerializer serializer =
                                KeySerializer.create(buildType.getField(fieldId).type());
                        GlobalIndexIOMeta sectionMeta =
                                new GlobalIndexIOMeta(
                                        bitmap.filePath(), section.length, bitmap.rowCount(), null);
                        return new BitmapIndexReader(
                                serializer, ignored -> section.open(input), sectionMeta);
                    });
        }
    }

    private static RoaringNavigableMap64 evaluate(
            Predicate predicate, IOFunction<String, BitmapIndexReader> openReader)
            throws IOException {
        if (predicate instanceof LeafPredicate) {
            LeafPredicate leaf = (LeafPredicate) predicate;
            try (BitmapIndexReader reader =
                    openReader.apply(leaf.fieldRefOptional().get().name())) {
                if (Equal.INSTANCE.equals(leaf.function())) {
                    return reader.visitEqual(leaf.literals().get(0)).get().results();
                }
                if (IsNull.INSTANCE.equals(leaf.function())) {
                    return reader.visitIsNull().get().results();
                }
                if (IsNotNull.INSTANCE.equals(leaf.function())) {
                    return reader.visitIsNotNull().get().results();
                }
                checkArgument(
                        In.INSTANCE.equals(leaf.function()),
                        "Unsupported centroid bitmap predicate %s.",
                        leaf.function());
                return reader.visitIn(leaf.literals()).get().results();
            }
        }

        checkArgument(
                predicate instanceof CompoundPredicate, "Unsupported centroid bitmap predicate.");
        CompoundPredicate compound = (CompoundPredicate) predicate;
        boolean conjunction = And.INSTANCE.equals(compound.function());
        checkArgument(
                conjunction || Or.INSTANCE.equals(compound.function()),
                "Unsupported centroid bitmap predicate %s.",
                compound.function());
        Iterator<Predicate> children = compound.children().iterator();
        checkArgument(children.hasNext(), "Centroid bitmap lookup requires a predicate.");
        RoaringNavigableMap64 rows = evaluate(children.next(), openReader);
        while (children.hasNext() && (!conjunction || !rows.isEmpty())) {
            RoaringNavigableMap64 childRows = evaluate(children.next(), openReader);
            if (conjunction) {
                rows.and(childRows);
            } else {
                rows.or(childRows);
            }
        }
        return rows;
    }

    private static Map<Integer, Section> readDirectory(
            @Nullable byte[] metadata, int[] extraFieldIds, long fileSize) {
        ByteBuffer directory =
                metadata == null ? ByteBuffer.allocate(0) : ByteBuffer.wrap(metadata);
        int fieldCount = extraFieldIds.length;
        checkArgument(
                fieldCount > 0 && directory.remaining() == (fieldCount - 1L) * Long.BYTES,
                "Invalid centroid bitmap directory length.");
        Map<Integer, Section> sections = new HashMap<>();
        long offset = 0;
        for (int i = 0; i < fieldCount; i++) {
            int fieldId = extraFieldIds[i];
            long end = i == fieldCount - 1 ? fileSize : directory.getLong();
            checkArgument(
                    end > offset && end <= fileSize,
                    "Invalid centroid bitmap section for field %s.",
                    fieldId);
            checkArgument(
                    sections.put(fieldId, new Section(offset, end - offset)) == null,
                    "Duplicate centroid bitmap field %s.",
                    fieldId);
            offset = end;
        }
        return sections;
    }

    /** Writes one complete Bitmap per column to the same output stream. */
    public static final class Writer implements AutoCloseable {

        private final String filePath;
        private final int fieldCount;
        private final long rowCount;
        private final PositionOutputStream output;
        private final ByteBuffer directory;
        private int written;

        public Writer(GlobalIndexFileWriter files, int fieldCount, long rowCount)
                throws IOException {
            checkArgument(fieldCount > 0 && rowCount > 0, "Empty centroid bitmap file.");
            this.filePath = files.newFilePath("centroid-bitmap");
            this.fieldCount = fieldCount;
            this.rowCount = rowCount;
            this.directory = ByteBuffer.allocate(Math.multiplyExact(fieldCount - 1, Long.BYTES));
            this.output = files.newOutputStream(filePath);
        }

        public void writeColumn(DataField field, Options options, ColumnRows rows)
                throws IOException {
            checkState(written < fieldCount, "Too many centroid bitmap sections.");
            long offset = output.getPos();
            GlobalIndexFileWriter sectionWriter =
                    new GlobalIndexFileWriter() {
                        @Override
                        public String newFilePath(String prefix) {
                            return filePath;
                        }

                        @Override
                        public PositionOutputStream newOutputStream(String filePath) {
                            return new CloseShieldOutputStream(output) {
                                @Override
                                public long getPos() throws IOException {
                                    return super.getPos() - offset;
                                }
                            };
                        }
                    };
            long sectionRows;
            try (BitmapGlobalIndexWriter writer =
                    new BitmapGlobalIndexer(field, options).createWriter(sectionWriter)) {
                rows.writeTo(writer);
                sectionRows = writer.finishSection();
            }
            checkState(sectionRows == rowCount, "Centroid bitmap and vector row counts differ.");
            if (written > 0) {
                directory.putLong(offset);
            }
            written++;
        }

        public ResultEntry finish() {
            checkState(written == fieldCount, "Incomplete centroid bitmap file.");
            return new ResultEntry(filePath, rowCount, fieldCount == 1 ? null : directory.array());
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
        private final long offset;
        private final long length;

        private Section(long offset, long length) {
            this.offset = offset;
            this.length = length;
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
