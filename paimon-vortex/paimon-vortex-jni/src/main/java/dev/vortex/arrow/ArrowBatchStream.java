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

package dev.vortex.arrow;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.ArrowFormats;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.c.NativeUtil;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.util.MemoryUtil;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.BitVectorHelper;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Reads the record batches of an Arrow C stream from the native scan. Each batch is a new root that
 * the caller owns and must close.
 *
 * <p>Vortex exports strings and binaries as {@code Utf8View} and {@code BinaryView}. Arrow Java 15
 * cannot import view types, and newer Arrow Java versions need Java 11. So this stream imports view
 * columns as {@code Utf8} and {@code Binary}: it copies their values into new vectors. All other
 * columns are moved into Java without a copy.
 */
public final class ArrowBatchStream implements AutoCloseable {

    private static final long FLAG_NULLABLE = 2;
    private static final int VIEW_SIZE = 16;
    private static final int MAX_INLINE_VIEW_LENGTH = 12;

    private final BufferAllocator allocator;
    private final ArrowArrayStream stream;
    private final Schema schema;
    private final Layout layout;

    private ArrowBatchStream(
            BufferAllocator allocator, ArrowArrayStream stream, Schema schema, Layout layout) {
        this.allocator = allocator;
        this.stream = stream;
        this.schema = schema;
        this.layout = layout;
    }

    /** Takes ownership of {@code stream}, which must be a stream of struct arrays. */
    public static ArrowBatchStream open(BufferAllocator allocator, ArrowArrayStream stream)
            throws IOException {
        try (ArrowSchema cSchema = ArrowSchema.allocateNew(allocator)) {
            stream.getSchema(cSchema);
            try {
                long address = cSchema.memoryAddress();
                Field root = field(address);
                return new ArrowBatchStream(
                        allocator, stream, new Schema(root.getChildren()), Layout.of(address));
            } finally {
                cSchema.release();
            }
        } catch (Throwable t) {
            stream.release();
            stream.close();
            throw t;
        }
    }

    /** The schema of every batch, with view types replaced by {@code Utf8} and {@code Binary}. */
    public Schema schema() {
        return schema;
    }

    /** Returns the next batch, or null at the end of the stream. */
    public VectorSchemaRoot next() throws IOException {
        ArrowArray array = ArrowArray.allocateNew(allocator);
        boolean moved = false;
        try {
            stream.getNext(array);
            ArrowArray.Snapshot snapshot = array.snapshot();
            if (snapshot.release == 0) {
                return null;
            }

            VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
            try {
                if (!layout.hasView) {
                    // Moves the whole batch, then releases and closes the array struct.
                    moved = true;
                    Data.importIntoVectorSchemaRoot(allocator, array, root, null);
                    return root;
                }

                checkZeroOffset(snapshot);
                long[] children =
                        NativeUtil.toJavaArray(snapshot.children, (int) snapshot.n_children);
                for (int i = 0; i < children.length; i++) {
                    importColumn(children[i], layout.children[i], root.getVector(i));
                }
                root.setRowCount((int) snapshot.length);
                return root;
            } catch (Throwable t) {
                root.close();
                throw t;
            }
        } finally {
            if (!moved) {
                // Releases the columns that were copied rather than moved.
                if (array.snapshot().release != 0) {
                    array.release();
                }
                array.close();
            }
        }
    }

    @Override
    public void close() {
        if (stream.snapshot().release != 0) {
            stream.release();
        }
        stream.close();
    }

    private void importColumn(long arrayAddress, Layout layout, FieldVector vector) {
        if (!layout.hasView) {
            // Moves the column, and marks it as released so its parent does not release it.
            Data.importIntoVector(allocator, ArrowArray.wrap(arrayAddress), vector, null);
            return;
        }

        ArrowArray.Snapshot snapshot = ArrowArray.wrap(arrayAddress).snapshot();
        switch (layout.kind) {
            case VIEW:
                copyViews(snapshot, (BaseVariableWidthVector) vector);
                return;
            case STRUCT:
                importStruct(snapshot, layout, (StructVector) vector);
                return;
            case LIST:
                importList(snapshot, layout, (ListVector) vector);
                return;
            default:
                throw new UnsupportedOperationException(
                        "Cannot import a view type nested in " + vector.getField());
        }
    }

    private void importStruct(ArrowArray.Snapshot snapshot, Layout layout, StructVector vector) {
        checkZeroOffset(snapshot);
        int length = (int) snapshot.length;
        long validity = buffer(snapshot, 0);

        long[] children = NativeUtil.toJavaArray(snapshot.children, (int) snapshot.n_children);
        for (int i = 0; i < children.length; i++) {
            importColumn(
                    children[i], layout.children[i], (FieldVector) vector.getChildByOrdinal(i));
        }

        for (int i = 0; i < length; i++) {
            if (isValid(validity, i)) {
                vector.setIndexDefined(i);
            } else {
                vector.setNull(i);
            }
        }
        vector.setValueCount(length);
    }

    private void importList(ArrowArray.Snapshot snapshot, Layout layout, ListVector vector) {
        checkZeroOffset(snapshot);
        int length = (int) snapshot.length;
        long validity = buffer(snapshot, 0);
        long offsets = buffer(snapshot, 1);
        if (length > 0 && MemoryUtil.UNSAFE.getInt(offsets) != 0) {
            throw new UnsupportedOperationException(
                    "Cannot import a list whose offsets do not start at 0");
        }

        long[] children = NativeUtil.toJavaArray(snapshot.children, (int) snapshot.n_children);
        importColumn(children[0], layout.children[0], vector.getDataVector());

        for (int i = 0; i < length; i++) {
            if (isValid(validity, i)) {
                vector.startNewValue(i);
                int size =
                        MemoryUtil.UNSAFE.getInt(offsets + 4L * (i + 1))
                                - MemoryUtil.UNSAFE.getInt(offsets + 4L * i);
                vector.endValue(i, size);
            } else {
                vector.setNull(i);
            }
        }
        vector.setValueCount(length);
    }

    /**
     * Copies a {@code Utf8View} or {@code BinaryView} array into an offset-based vector. A view of
     * at most 12 bytes holds its bytes inline. A longer view points into one of the variadic data
     * buffers, which follow the validity and view buffers.
     */
    private static void copyViews(ArrowArray.Snapshot snapshot, BaseVariableWidthVector vector) {
        int length = (int) snapshot.length;
        long offset = snapshot.offset;
        long[] buffers = NativeUtil.toJavaArray(snapshot.buffers, (int) snapshot.n_buffers);
        long validity = buffers[0];
        long views = buffers[1];

        long totalBytes = 0;
        for (int i = 0; i < length; i++) {
            if (isValid(validity, offset + i)) {
                totalBytes += MemoryUtil.UNSAFE.getInt(views + VIEW_SIZE * (offset + i));
            }
        }

        vector.allocateNew(totalBytes, length);
        ArrowBuf offsets = vector.getOffsetBuffer();
        ArrowBuf validityOut = vector.getValidityBuffer();
        long data = vector.getDataBuffer().memoryAddress();

        long position = 0;
        offsets.setInt(0, 0);
        for (int i = 0; i < length; i++) {
            long view = views + VIEW_SIZE * (offset + i);
            if (isValid(validity, offset + i)) {
                int size = MemoryUtil.UNSAFE.getInt(view);
                long source;
                if (size <= MAX_INLINE_VIEW_LENGTH) {
                    source = view + 4;
                } else {
                    int bufferIndex = MemoryUtil.UNSAFE.getInt(view + 8);
                    int bufferOffset = MemoryUtil.UNSAFE.getInt(view + 12);
                    source = buffers[2 + bufferIndex] + bufferOffset;
                }
                MemoryUtil.UNSAFE.copyMemory(source, data + position, size);
                position += size;
                BitVectorHelper.setBit(validityOut, i);
            }
            offsets.setInt(4L * (i + 1), (int) position);
        }

        vector.setLastSet(length - 1);
        vector.setValueCount(length);
    }

    private static long buffer(ArrowArray.Snapshot snapshot, int index) {
        return NativeUtil.toJavaArray(snapshot.buffers, (int) snapshot.n_buffers)[index];
    }

    private static boolean isValid(long validity, long index) {
        if (validity == 0) {
            return true;
        }
        byte bits = MemoryUtil.UNSAFE.getByte(validity + (index >> 3));
        return ((bits >> (index & 7)) & 1) != 0;
    }

    private static void checkZeroOffset(ArrowArray.Snapshot snapshot) {
        if (snapshot.offset != 0) {
            throw new UnsupportedOperationException(
                    "Cannot import a nested array with offset " + snapshot.offset);
        }
    }

    private static Field field(long schemaAddress) {
        ArrowSchema.Snapshot snapshot = ArrowSchema.wrap(schemaAddress).snapshot();
        if (snapshot.dictionary != 0) {
            throw new UnsupportedOperationException("Cannot import a dictionary-encoded field");
        }

        String format = NativeUtil.toJavaString(snapshot.format);
        String name = snapshot.name == 0 ? "" : NativeUtil.toJavaString(snapshot.name);
        boolean nullable = (snapshot.flags & FLAG_NULLABLE) != 0;

        ArrowType type;
        if ("vu".equals(format)) {
            type = ArrowType.Utf8.INSTANCE;
        } else if ("vz".equals(format)) {
            type = ArrowType.Binary.INSTANCE;
        } else {
            type = ArrowFormats.asType(format, snapshot.flags);
        }

        List<Field> children = new ArrayList<>();
        for (long child : NativeUtil.toJavaArray(snapshot.children, (int) snapshot.n_children)) {
            children.add(field(child));
        }
        return new Field(name, new FieldType(nullable, type, null), children);
    }

    /** Where the view columns are in a schema. */
    private static final class Layout {

        private final Kind kind;
        private final Layout[] children;
        private final boolean hasView;

        private Layout(Kind kind, Layout[] children) {
            this.kind = kind;
            this.children = children;
            boolean view = kind == Kind.VIEW;
            for (Layout child : children) {
                view |= child.hasView;
            }
            this.hasView = view;
        }

        private static Layout of(long schemaAddress) {
            ArrowSchema.Snapshot snapshot = ArrowSchema.wrap(schemaAddress).snapshot();
            String format = NativeUtil.toJavaString(snapshot.format);

            long[] childAddresses =
                    NativeUtil.toJavaArray(snapshot.children, (int) snapshot.n_children);
            Layout[] children = new Layout[childAddresses.length];
            for (int i = 0; i < children.length; i++) {
                children[i] = of(childAddresses[i]);
            }

            Kind kind;
            if ("vu".equals(format) || "vz".equals(format)) {
                kind = Kind.VIEW;
            } else if ("+s".equals(format)) {
                kind = Kind.STRUCT;
            } else if ("+l".equals(format)) {
                kind = Kind.LIST;
            } else {
                kind = Kind.OTHER;
            }
            return new Layout(kind, children);
        }
    }

    private enum Kind {
        VIEW,
        STRUCT,
        LIST,
        OTHER
    }
}
