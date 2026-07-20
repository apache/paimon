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

import org.apache.paimon.KeyValue;
import org.apache.paimon.data.Blob;
import org.apache.paimon.data.BlobRef;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.RowType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.paimon.types.BlobType.isBlobFileField;
import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkState;

/** Collects exact managed BLOB dependencies from the final rows of one data file. */
public class ManagedBlobReferenceCollector {

    private final FileIO fileIO;
    private final Path sidecar;
    private final int[] blobFieldIndexes;
    private final boolean[] blobArrayFields;
    private final Set<String> descriptorUris;

    private boolean closed;

    public ManagedBlobReferenceCollector(
            FileIO fileIO, Path dataFile, RowType valueType, Set<String> managedBlobFields) {
        this.fileIO = fileIO;
        this.sidecar = ManagedBlobReferenceFile.sidecarPath(dataFile);
        List<Integer> indexes = new ArrayList<>();
        List<Boolean> arrayFields = new ArrayList<>();
        for (int i = 0; i < valueType.getFieldCount(); i++) {
            if (!managedBlobFields.contains(valueType.getFieldNames().get(i))) {
                continue;
            }
            DataTypeRoot typeRoot = valueType.getTypeAt(i).getTypeRoot();
            checkArgument(
                    typeRoot != DataTypeRoot.MAP,
                    "Primary-key managed MAP<X, BLOB> field '%s' is not supported.",
                    valueType.getFieldNames().get(i));
            if (isBlobFileField(valueType.getTypeAt(i))) {
                indexes.add(i);
                arrayFields.add(typeRoot == DataTypeRoot.ARRAY);
            }
        }
        this.blobFieldIndexes = indexes.stream().mapToInt(Integer::intValue).toArray();
        this.blobArrayFields = new boolean[arrayFields.size()];
        for (int i = 0; i < arrayFields.size(); i++) {
            blobArrayFields[i] = arrayFields.get(i);
        }
        this.descriptorUris = new HashSet<>();
    }

    public void write(KeyValue keyValue) {
        checkState(!closed, "Managed BLOB reference collector is already closed.");
        if (keyValue.valueKind().isRetract()) {
            return;
        }

        for (int i = 0; i < blobFieldIndexes.length; i++) {
            int fieldIndex = blobFieldIndexes[i];
            if (keyValue.value().isNullAt(fieldIndex)) {
                continue;
            }
            if (blobArrayFields[i]) {
                InternalArray array = keyValue.value().getArray(fieldIndex);
                for (int j = 0; j < array.size(); j++) {
                    if (!array.isNullAt(j)) {
                        collect(array.getBlob(j));
                    }
                }
            } else {
                collect(keyValue.value().getBlob(fieldIndex));
            }
        }
    }

    private void collect(Blob blob) {
        if (blob instanceof BlobRef) {
            descriptorUris.add(blob.toDescriptor().uri());
        }
    }

    public void close() throws IOException {
        if (closed) {
            return;
        }
        try {
            List<ManagedBlobReferenceFile.Reference> references =
                    new ArrayList<>(descriptorUris.size());
            for (String descriptorUri : descriptorUris) {
                ManagedBlobReferenceFile.fromDescriptorUri(descriptorUri)
                        .ifPresent(references::add);
            }
            descriptorUris.clear();
            ManagedBlobReferenceFile.write(fileIO, sidecar, references);
            closed = true;
        } catch (IOException e) {
            abort();
            throw e;
        }
    }

    public void abort() {
        fileIO.deleteQuietly(sidecar);
        closed = true;
    }

    public String result() {
        checkState(closed, "Managed BLOB reference collector must be closed before result.");
        return sidecar.getName();
    }
}
