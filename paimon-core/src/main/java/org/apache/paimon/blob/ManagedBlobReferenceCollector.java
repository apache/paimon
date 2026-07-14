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
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.RowType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.paimon.utils.Preconditions.checkState;

/** Collects exact managed BLOB dependencies from the final rows of one data file. */
public class ManagedBlobReferenceCollector {

    private final FileIO fileIO;
    private final Path dataFile;
    private final Path sidecar;
    private final int[] blobFieldIndexes;
    private final Set<ManagedBlobReferenceFile.Reference> references;

    private boolean closed;

    public ManagedBlobReferenceCollector(FileIO fileIO, Path dataFile, RowType valueType) {
        this.fileIO = fileIO;
        this.dataFile = dataFile;
        this.sidecar = ManagedBlobReferenceFile.sidecarPath(dataFile);
        List<Integer> indexes = new ArrayList<>();
        for (int i = 0; i < valueType.getFieldCount(); i++) {
            if (valueType.getTypeAt(i).getTypeRoot() == DataTypeRoot.BLOB) {
                indexes.add(i);
            }
        }
        this.blobFieldIndexes = indexes.stream().mapToInt(Integer::intValue).toArray();
        this.references = new HashSet<>();
    }

    public void write(KeyValue keyValue) {
        checkState(!closed, "Managed BLOB reference collector is already closed.");
        if (keyValue.valueKind().isRetract()) {
            return;
        }

        for (int fieldIndex : blobFieldIndexes) {
            if (keyValue.value().isNullAt(fieldIndex)) {
                continue;
            }
            Blob blob = keyValue.value().getBlob(fieldIndex);
            if (!(blob instanceof BlobRef)) {
                continue;
            }
            ManagedBlobReferenceFile.fromDescriptorUri(dataFile, blob.toDescriptor().uri())
                    .ifPresent(references::add);
        }
    }

    public void close() throws IOException {
        if (closed) {
            return;
        }
        try {
            ManagedBlobReferenceFile.write(fileIO, sidecar, new ArrayList<>(references));
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
