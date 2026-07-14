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
import org.apache.paimon.data.BlobRef;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.blob.BlobFormatWriter;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.IOUtils;
import org.apache.paimon.utils.RowDataToObjectArrayConverter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Externalizes primary-key BLOB values before they enter the MergeTree write buffer. */
public class PrimaryKeyBlobExternalizer {

    private final FileIO fileIO;
    private final RowDataToObjectArrayConverter rowConverter;
    private final int[] blobFieldIndexes;
    private final ManagedBlobPackWriter[] packWriters;
    private final List<Path> uncommittedPacks;

    public PrimaryKeyBlobExternalizer(
            FileIO fileIO,
            RowType valueType,
            DataFilePathFactory pathFactory,
            long targetFileSize) {
        checkArgument(targetFileSize > 0, "Managed BLOB target file size must be positive.");
        this.fileIO = fileIO;
        this.rowConverter = new RowDataToObjectArrayConverter(valueType);
        this.uncommittedPacks = new ArrayList<>();

        List<Integer> indexes = new ArrayList<>();
        List<ManagedBlobPackWriter> writers = new ArrayList<>();
        for (int i = 0; i < valueType.getFieldCount(); i++) {
            DataField field = valueType.getFields().get(i);
            if (field.type().getTypeRoot() == DataTypeRoot.BLOB) {
                indexes.add(i);
                writers.add(
                        new ManagedBlobPackWriter(
                                fileIO,
                                new RowType(Collections.singletonList(field)),
                                pathFactory,
                                targetFileSize,
                                uncommittedPacks));
            }
        }
        this.blobFieldIndexes = indexes.stream().mapToInt(Integer::intValue).toArray();
        this.packWriters = writers.toArray(new ManagedBlobPackWriter[0]);
    }

    public boolean enabled() {
        return blobFieldIndexes.length > 0;
    }

    public InternalRow externalize(RowKind valueKind, InternalRow value) throws IOException {
        if (!enabled()) {
            return value;
        }

        GenericRow result = null;
        try {
            for (int i = 0; i < blobFieldIndexes.length; i++) {
                int fieldIndex = blobFieldIndexes[i];
                if (value.isNullAt(fieldIndex)) {
                    continue;
                }

                Blob blob = value.getBlob(fieldIndex);
                if (valueKind.isRetract()) {
                    if (result == null) {
                        result = copy(value);
                    }
                    result.setField(fieldIndex, null);
                } else if (!(blob instanceof BlobRef)) {
                    if (result == null) {
                        result = copy(value);
                    }
                    BlobDescriptor descriptor = packWriters[i].write(blob);
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

    public void prepareCommit() throws IOException {
        try {
            for (ManagedBlobPackWriter packWriter : packWriters) {
                packWriter.closeCurrent();
            }
            uncommittedPacks.clear();
        } catch (IOException e) {
            abort();
            throw e;
        }
    }

    public void abort() {
        for (ManagedBlobPackWriter packWriter : packWriters) {
            packWriter.abortCurrent();
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

    private static class ManagedBlobPackWriter {

        private final FileIO fileIO;
        private final RowType blobType;
        private final DataFilePathFactory pathFactory;
        private final long targetFileSize;
        private final List<Path> uncommittedPacks;

        private Path currentPath;
        private PositionOutputStream out;
        private BlobFormatWriter writer;
        private BlobDescriptor lastDescriptor;

        private ManagedBlobPackWriter(
                FileIO fileIO,
                RowType blobType,
                DataFilePathFactory pathFactory,
                long targetFileSize,
                List<Path> uncommittedPacks) {
            this.fileIO = fileIO;
            this.blobType = blobType;
            this.pathFactory = pathFactory;
            this.targetFileSize = targetFileSize;
            this.uncommittedPacks = uncommittedPacks;
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
                            blobType);
            writer.setFile(currentPath);
        }

        private void closeCurrent() throws IOException {
            if (writer == null) {
                return;
            }
            try {
                writer.close();
                out.flush();
                out.close();
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
