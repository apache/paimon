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

package org.apache.paimon.blob;

import org.apache.paimon.data.Blob;
import org.apache.paimon.data.BlobDescriptor;
import org.apache.paimon.data.BlobFetchMetricReporter;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.blob.BlobFormatWriter;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.IOUtils;
import org.apache.paimon.utils.InternalRowUtils;
import org.apache.paimon.utils.RowDataToObjectArrayConverter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.apache.paimon.types.BlobType.isBlobFileField;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Externalizes primary-key BLOB values before they enter the MergeTree write buffer. */
public class PrimaryKeyBlobExternalizer {

    private static final RowType BLOB_ROW_TYPE = RowType.of(DataTypes.BLOB());

    private final FileIO fileIO;
    private final RowDataToObjectArrayConverter rowConverter;
    private final ManagedBlobField[] blobFields;
    private final List<Path> uncommittedPacks;

    public PrimaryKeyBlobExternalizer(
            FileIO fileIO,
            RowType valueType,
            Set<String> managedBlobFields,
            DataFilePathFactory pathFactory,
            long targetFileSize) {
        this(
                fileIO,
                valueType,
                managedBlobFields,
                pathFactory,
                targetFileSize,
                BlobFormatWriter.DEFAULT_COPY_BUFFER_SIZE);
    }

    public PrimaryKeyBlobExternalizer(
            FileIO fileIO,
            RowType valueType,
            Set<String> managedBlobFields,
            DataFilePathFactory pathFactory,
            long targetFileSize,
            int copyBufferSize) {
        checkArgument(targetFileSize > 0, "Managed BLOB target file size must be positive.");
        this.fileIO = fileIO;
        this.rowConverter = new RowDataToObjectArrayConverter(valueType);
        this.uncommittedPacks = new ArrayList<>();

        List<ManagedBlobField> blobFields = new ArrayList<>();
        Set<String> unknownFields = new TreeSet<>(managedBlobFields);
        for (int i = 0; i < valueType.getFieldCount(); i++) {
            DataField field = valueType.getFields().get(i);
            if (!managedBlobFields.contains(field.name())) {
                continue;
            }
            unknownFields.remove(field.name());
            boolean blob = field.type().getTypeRoot() == DataTypeRoot.BLOB;
            boolean blobArray =
                    field.type().getTypeRoot() == DataTypeRoot.ARRAY
                            && ((ArrayType) field.type()).getElementType().getTypeRoot()
                                    == DataTypeRoot.BLOB;
            boolean blobMap =
                    field.type().getTypeRoot() == DataTypeRoot.MAP && isBlobFileField(field.type());
            checkArgument(
                    blob || blobArray || blobMap,
                    "Managed BLOB field '%s' must be BLOB, ARRAY<BLOB> or MAP<X, BLOB>, but was %s.",
                    field.name(),
                    field.type());
            blobFields.add(
                    new ManagedBlobField(
                            i,
                            field.type(),
                            blobMap
                                    ? InternalArray.createElementGetter(
                                            ((MapType) field.type()).getKeyType())
                                    : null,
                            new ManagedBlobPackWriter(
                                    fileIO,
                                    pathFactory,
                                    targetFileSize,
                                    uncommittedPacks,
                                    copyBufferSize)));
        }
        checkArgument(
                unknownFields.isEmpty(),
                "Managed BLOB fields do not exist in value type: %s.",
                unknownFields);
        this.blobFields = blobFields.toArray(new ManagedBlobField[0]);
    }

    public boolean enabled() {
        return blobFields.length > 0;
    }

    public InternalRow externalize(RowKind valueKind, InternalRow value) throws IOException {
        if (!enabled()) {
            return value;
        }

        GenericRow result = null;
        try {
            for (ManagedBlobField blobField : blobFields) {
                int fieldIndex = blobField.index;
                if (value.isNullAt(fieldIndex)) {
                    continue;
                }

                if (valueKind.isRetract()) {
                    if (result == null) {
                        result = copy(value);
                    }
                    result.setField(fieldIndex, null);
                } else if (blobField.type.getTypeRoot() == DataTypeRoot.ARRAY) {
                    InternalArray array = value.getArray(fieldIndex);
                    GenericArray externalized = externalizeArray(array, blobField.packWriter);
                    if (externalized != null) {
                        if (result == null) {
                            result = copy(value);
                        }
                        result.setField(fieldIndex, externalized);
                    }
                } else if (blobField.type.getTypeRoot() == DataTypeRoot.MAP) {
                    InternalMap map = value.getMap(fieldIndex);
                    MapType mapType = (MapType) blobField.type;
                    GenericMap externalized =
                            externalizeMap(
                                    map,
                                    mapType.getKeyType(),
                                    blobField.mapKeyGetter,
                                    blobField.packWriter);
                    if (externalized != null) {
                        if (result == null) {
                            result = copy(value);
                        }
                        result.setField(fieldIndex, externalized);
                    }
                } else {
                    Blob blob = value.getBlob(fieldIndex);
                    if (result == null) {
                        result = copy(value);
                    }
                    BlobDescriptor descriptor = blobField.packWriter.write(blob);
                    result.setField(
                            fieldIndex,
                            Blob.fromFile(
                                    fileIO,
                                    descriptor.uri(),
                                    descriptor.offset(),
                                    descriptor.length()));
                }
            }
        } catch (IOException | RuntimeException e) {
            abort();
            throw e;
        }
        return result == null ? value : result;
    }

    private GenericArray externalizeArray(InternalArray array, ManagedBlobPackWriter packWriter)
            throws IOException {
        Object[] elements = null;
        for (int i = 0; i < array.size(); i++) {
            if (array.isNullAt(i)) {
                continue;
            }

            Blob blob = array.getBlob(i);

            if (elements == null) {
                elements = new Object[array.size()];
                for (int j = 0; j < array.size(); j++) {
                    elements[j] = array.isNullAt(j) ? null : array.getBlob(j);
                }
            }
            BlobDescriptor descriptor = packWriter.write(blob);
            elements[i] =
                    Blob.fromFile(
                            fileIO, descriptor.uri(), descriptor.offset(), descriptor.length());
        }
        return elements == null ? null : new GenericArray(elements);
    }

    private GenericMap externalizeMap(
            InternalMap map,
            DataType keyType,
            InternalArray.ElementGetter keyGetter,
            ManagedBlobPackWriter packWriter)
            throws IOException {
        InternalArray keys = map.keyArray();
        InternalArray values = map.valueArray();
        checkArgument(
                keys.size() == map.size() && values.size() == map.size(),
                "MAP<X, BLOB> key/value array size does not match map size.");

        Map<Object, Blob> blobs = new LinkedHashMap<>();
        for (int i = 0; i < map.size(); i++) {
            Object key = InternalRowUtils.copy(keyGetter.getElementOrNull(keys, i), keyType);
            blobs.put(key, values.isNullAt(i) ? null : values.getBlob(i));
        }

        boolean hasBlob = false;
        for (Blob blob : blobs.values()) {
            if (blob != null) {
                hasBlob = true;
                break;
            }
        }
        if (!hasBlob) {
            return blobs.size() == map.size() ? null : new GenericMap(blobs);
        }

        Map<Object, Object> externalized = new LinkedHashMap<>();
        for (Map.Entry<Object, Blob> entry : blobs.entrySet()) {
            Blob blob = entry.getValue();
            if (blob == null) {
                externalized.put(entry.getKey(), null);
                continue;
            }
            BlobDescriptor descriptor = packWriter.write(blob);
            externalized.put(
                    entry.getKey(),
                    Blob.fromFile(
                            fileIO, descriptor.uri(), descriptor.offset(), descriptor.length()));
        }
        return new GenericMap(externalized);
    }

    public void prepareCommit() throws IOException {
        try {
            for (ManagedBlobField blobField : blobFields) {
                blobField.packWriter.closeCurrent();
            }
            uncommittedPacks.clear();
        } catch (IOException e) {
            abort();
            throw e;
        }
    }

    public void abort() {
        for (ManagedBlobField blobField : blobFields) {
            blobField.packWriter.abortCurrent();
        }
        for (Path path : uncommittedPacks) {
            fileIO.deleteQuietly(path);
        }
        uncommittedPacks.clear();
    }

    private GenericRow copy(InternalRow value) {
        GenericRow row = rowConverter.toGenericRow(value);
        row.setRowKind(value.getRowKind());
        return row;
    }

    private static class ManagedBlobField {

        private final int index;
        private final DataType type;
        private final InternalArray.ElementGetter mapKeyGetter;
        private final ManagedBlobPackWriter packWriter;

        private ManagedBlobField(
                int index,
                DataType type,
                InternalArray.ElementGetter mapKeyGetter,
                ManagedBlobPackWriter packWriter) {
            this.index = index;
            this.type = type;
            this.mapKeyGetter = mapKeyGetter;
            this.packWriter = packWriter;
        }
    }

    private static class ManagedBlobPackWriter {

        private final FileIO fileIO;
        private final DataFilePathFactory pathFactory;
        private final long targetFileSize;
        private final List<Path> uncommittedPacks;
        private final int copyBufferSize;

        private Path currentPath;
        private PositionOutputStream out;
        private BlobFormatWriter writer;
        private BlobDescriptor lastDescriptor;

        private ManagedBlobPackWriter(
                FileIO fileIO,
                DataFilePathFactory pathFactory,
                long targetFileSize,
                List<Path> uncommittedPacks,
                int copyBufferSize) {
            this.fileIO = fileIO;
            this.pathFactory = pathFactory;
            this.targetFileSize = targetFileSize;
            this.uncommittedPacks = uncommittedPacks;
            this.copyBufferSize = copyBufferSize;
        }

        private BlobDescriptor write(Blob blob) throws IOException {
            if (writer == null) {
                openCurrent();
            }

            lastDescriptor = null;
            writer.addElement(GenericRow.of(blob));
            BlobDescriptor descriptor = lastDescriptor;
            if (descriptor == null) {
                throw new IOException("Managed BLOB writer did not produce a descriptor.");
            }
            if (writer.reachTargetSize(true, targetFileSize)) {
                closeCurrent();
            }
            return descriptor;
        }

        private void openCurrent() throws IOException {
            currentPath =
                    pathFactory.newPathFromExtension(ManagedBlobReferenceFile.MANAGED_BLOB_SUFFIX);
            uncommittedPacks.add(currentPath);
            out = fileIO.newOutputStream(currentPath, false);
            writer =
                    new BlobFormatWriter(
                            out,
                            (fieldName, descriptor) -> {
                                lastDescriptor = descriptor;
                                return false;
                            },
                            BLOB_ROW_TYPE,
                            false,
                            false,
                            BlobFetchMetricReporter.NOOP,
                            copyBufferSize);
            writer.setFile(currentPath);
        }

        private void closeCurrent() throws IOException {
            if (writer == null) {
                return;
            }
            PositionOutputStream currentOut = out;
            try (PositionOutputStream ignored = currentOut) {
                writer.close();
                currentOut.flush();
            } finally {
                writer = null;
                out = null;
                currentPath = null;
            }
        }

        private void abortCurrent() {
            IOUtils.closeQuietly(writer);
            IOUtils.closeQuietly(out);
            writer = null;
            out = null;
            currentPath = null;
        }
    }
}
