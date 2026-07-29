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

package org.apache.paimon.manifest;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.utils.Range;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.InternalRowUtils.fromStringArrayData;
import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkState;

/** A simple {@link FileEntry} only contains identifier and min max key. */
public class SimpleFileEntry implements FileEntry {

    private final FileKind kind;
    private final EntryData data;

    public SimpleFileEntry(
            FileKind kind,
            BinaryRow partition,
            int bucket,
            int totalBuckets,
            int level,
            String fileName,
            List<String> extraFiles,
            @Nullable byte[] embeddedIndex,
            BinaryRow minKey,
            BinaryRow maxKey,
            @Nullable String externalPath,
            long rowCount,
            @Nullable Long firstRowId) {
        this(
                kind,
                new PojoEntryData(
                        partition,
                        bucket,
                        totalBuckets,
                        level,
                        fileName,
                        extraFiles,
                        embeddedIndex,
                        minKey,
                        maxKey,
                        externalPath,
                        rowCount,
                        firstRowId));
    }

    private SimpleFileEntry(FileKind kind, EntryData data) {
        this.kind = kind;
        this.data = data;
    }

    /** Creates a lightweight wrapper which shares the immutable entry data. */
    protected SimpleFileEntry(SimpleFileEntry entry) {
        this(entry.kind, entry.data);
    }

    public static SimpleFileEntry from(ManifestEntry entry) {
        return new SimpleFileEntry(
                entry.kind(),
                entry.partition(),
                entry.bucket(),
                entry.totalBuckets(),
                entry.level(),
                entry.fileName(),
                entry.file().extraFiles(),
                entry.file().embeddedIndex(),
                entry.minKey(),
                entry.maxKey(),
                entry.externalPath(),
                entry.file().rowCount(),
                entry.firstRowId());
    }

    /** Copies a compact projected binary entry into independently owned memory. */
    public static SimpleFileEntry fromCompact(BinaryManifestEntry entry) {
        checkArgument(
                entry.usesProjection(BinaryManifestEntry.SIMPLE_FILE_ENTRY_PROJECTION),
                "Binary entry does not use the simple-file-entry projection.");
        return new SimpleFileEntry(entry.kind(), new BinaryEntryData(entry.copyRow()));
    }

    public SimpleFileEntry toDelete() {
        return new SimpleFileEntry(FileKind.DELETE, data);
    }

    public static List<SimpleFileEntry> from(List<ManifestEntry> entries) {
        return entries.stream().map(SimpleFileEntry::from).collect(Collectors.toList());
    }

    @Override
    public FileKind kind() {
        return kind;
    }

    @Override
    public BinaryRow partition() {
        return data.partition();
    }

    @Override
    public int bucket() {
        return data.bucket();
    }

    @Override
    public int totalBuckets() {
        return data.totalBuckets();
    }

    @Override
    public int level() {
        return data.level();
    }

    @Override
    public String fileName() {
        return data.fileName();
    }

    @Nullable
    public byte[] embeddedIndex() {
        return data.embeddedIndex();
    }

    @Nullable
    @Override
    public String externalPath() {
        return data.externalPath();
    }

    @Override
    public Identifier identifier() {
        return new Identifier(
                partition(),
                bucket(),
                level(),
                fileName(),
                extraFiles(),
                embeddedIndex(),
                externalPath());
    }

    @Override
    public BinaryRow minKey() {
        return data.minKey();
    }

    @Override
    public BinaryRow maxKey() {
        return data.maxKey();
    }

    @Override
    public List<String> extraFiles() {
        return data.extraFiles();
    }

    @Override
    public long rowCount() {
        return data.rowCount();
    }

    @Override
    public @Nullable Long firstRowId() {
        return data.firstRowId();
    }

    public long nonNullFirstRowId() {
        Long firstRowId = firstRowId();
        checkArgument(firstRowId != null, "First row id of '%s' should not be null.", fileName());
        return firstRowId;
    }

    public Range nonNullRowIdRange() {
        long firstRowId = nonNullFirstRowId();
        return new Range(firstRowId, firstRowId + rowCount() - 1);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SimpleFileEntry that = (SimpleFileEntry) o;
        return bucket() == that.bucket()
                && totalBuckets() == that.totalBuckets()
                && level() == that.level()
                && kind == that.kind()
                && Objects.equals(partition(), that.partition())
                && Objects.equals(fileName(), that.fileName())
                && Objects.equals(extraFiles(), that.extraFiles())
                && Objects.equals(minKey(), that.minKey())
                && Objects.equals(maxKey(), that.maxKey())
                && Objects.equals(externalPath(), that.externalPath())
                && rowCount() == that.rowCount()
                && Objects.equals(firstRowId(), that.firstRowId());
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                kind,
                partition(),
                bucket(),
                totalBuckets(),
                level(),
                fileName(),
                extraFiles(),
                minKey(),
                maxKey(),
                externalPath(),
                rowCount(),
                firstRowId());
    }

    @Override
    public String toString() {
        return "{"
                + "kind="
                + kind
                + ", partition="
                + partition()
                + ", bucket="
                + bucket()
                + ", totalBuckets="
                + totalBuckets()
                + ", level="
                + level()
                + ", fileName="
                + fileName()
                + ", extraFiles="
                + extraFiles()
                + ", minKey="
                + minKey()
                + ", maxKey="
                + maxKey()
                + ", externalPath="
                + externalPath()
                + ", rowCount="
                + rowCount()
                + ", firstRowId="
                + firstRowId()
                + '}';
    }

    private interface EntryData {

        BinaryRow partition();

        int bucket();

        int totalBuckets();

        int level();

        String fileName();

        List<String> extraFiles();

        @Nullable
        byte[] embeddedIndex();

        BinaryRow minKey();

        BinaryRow maxKey();

        @Nullable
        String externalPath();

        long rowCount();

        @Nullable
        Long firstRowId();
    }

    private static final class PojoEntryData implements EntryData {

        private final BinaryRow partition;
        private final int bucket;
        private final int totalBuckets;
        private final int level;
        private final String fileName;
        private final List<String> extraFiles;
        private final @Nullable byte[] embeddedIndex;
        private final BinaryRow minKey;
        private final BinaryRow maxKey;
        private final @Nullable String externalPath;
        private final long rowCount;
        private final @Nullable Long firstRowId;

        private PojoEntryData(
                BinaryRow partition,
                int bucket,
                int totalBuckets,
                int level,
                String fileName,
                List<String> extraFiles,
                @Nullable byte[] embeddedIndex,
                BinaryRow minKey,
                BinaryRow maxKey,
                @Nullable String externalPath,
                long rowCount,
                @Nullable Long firstRowId) {
            this.partition = partition;
            this.bucket = bucket;
            this.totalBuckets = totalBuckets;
            this.level = level;
            this.fileName = fileName;
            this.extraFiles = extraFiles;
            this.embeddedIndex = embeddedIndex;
            this.minKey = minKey;
            this.maxKey = maxKey;
            this.externalPath = externalPath;
            this.rowCount = rowCount;
            this.firstRowId = firstRowId;
        }

        @Override
        public BinaryRow partition() {
            return partition;
        }

        @Override
        public int bucket() {
            return bucket;
        }

        @Override
        public int totalBuckets() {
            return totalBuckets;
        }

        @Override
        public int level() {
            return level;
        }

        @Override
        public String fileName() {
            return fileName;
        }

        @Override
        public List<String> extraFiles() {
            return extraFiles;
        }

        @Override
        public byte[] embeddedIndex() {
            return embeddedIndex;
        }

        @Override
        public BinaryRow minKey() {
            return minKey;
        }

        @Override
        public BinaryRow maxKey() {
            return maxKey;
        }

        @Override
        public String externalPath() {
            return externalPath;
        }

        @Override
        public long rowCount() {
            return rowCount;
        }

        @Override
        public Long firstRowId() {
            return firstRowId;
        }
    }

    private static final class BinaryEntryData implements EntryData {

        private static final int PARTITION = 1;
        private static final int BUCKET = 2;
        private static final int TOTAL_BUCKETS = 3;
        private static final int FILE = 4;

        private static final int FILE_NAME = 0;
        private static final int ROW_COUNT = 1;
        private static final int MIN_KEY = 2;
        private static final int MAX_KEY = 3;
        private static final int LEVEL = 4;
        private static final int EXTRA_FILES = 5;
        private static final int EMBEDDED_INDEX = 6;
        private static final int EXTERNAL_PATH = 7;
        private static final int FIRST_ROW_ID = 8;
        private static final int FILE_FIELD_COUNT = 9;

        private final BinaryRow row;

        private BinaryEntryData(BinaryRow row) {
            this.row = row;
        }

        @Override
        public BinaryRow partition() {
            return binaryRow(row, PARTITION);
        }

        @Override
        public int bucket() {
            return row.getInt(BUCKET);
        }

        @Override
        public int totalBuckets() {
            return row.getInt(TOTAL_BUCKETS);
        }

        @Override
        public int level() {
            return fileRow().getInt(LEVEL);
        }

        @Override
        public String fileName() {
            BinaryString fileName = fileRow().getString(FILE_NAME);
            checkState(fileName != null, "Data file name cannot be null.");
            return fileName.toString();
        }

        @Override
        public List<String> extraFiles() {
            InternalArray extraFiles = fileRow().getArray(EXTRA_FILES);
            checkState(extraFiles != null, "Data file extra files cannot be null.");
            return Collections.unmodifiableList(fromStringArrayData(extraFiles));
        }

        @Override
        public byte[] embeddedIndex() {
            InternalRow file = fileRow();
            return file.isNullAt(EMBEDDED_INDEX) ? null : file.getBinary(EMBEDDED_INDEX);
        }

        @Override
        public BinaryRow minKey() {
            return binaryRow(fileRow(), MIN_KEY);
        }

        @Override
        public BinaryRow maxKey() {
            return binaryRow(fileRow(), MAX_KEY);
        }

        @Override
        public String externalPath() {
            InternalRow file = fileRow();
            return file.isNullAt(EXTERNAL_PATH) ? null : file.getString(EXTERNAL_PATH).toString();
        }

        @Override
        public long rowCount() {
            return fileRow().getLong(ROW_COUNT);
        }

        @Override
        public Long firstRowId() {
            InternalRow file = fileRow();
            return file.isNullAt(FIRST_ROW_ID) ? null : file.getLong(FIRST_ROW_ID);
        }

        private InternalRow fileRow() {
            InternalRow file = row.getRow(FILE, FILE_FIELD_COUNT);
            checkState(file != null, "Manifest data file metadata cannot be null.");
            return file;
        }

        private static BinaryRow binaryRow(InternalRow source, int position) {
            BinaryString serialized = source.getString(position);
            checkState(serialized != null, "Serialized binary row cannot be null.");
            checkState(
                    serialized.getSizeInBytes() >= Integer.BYTES,
                    "Serialized binary row is too short.");
            int arity =
                    ((serialized.byteAt(0) & 0xff) << 24)
                            | ((serialized.byteAt(1) & 0xff) << 16)
                            | ((serialized.byteAt(2) & 0xff) << 8)
                            | (serialized.byteAt(3) & 0xff);
            BinaryRow result = new BinaryRow(arity);
            result.pointTo(
                    serialized.getSegments(),
                    serialized.getOffset() + Integer.BYTES,
                    serialized.getSizeInBytes() - Integer.BYTES);
            return result;
        }
    }
}
