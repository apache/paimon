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

package org.apache.paimon.index.pksorted;

import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.pk.PrimaryKeyIndexSourceFile;
import org.apache.paimon.index.pk.PrimaryKeyIndexSourceMeta;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/** All rotated payloads that index the same ordered source data files. */
public final class PkSortedIndexGroup {

    private final List<PrimaryKeyIndexSourceFile> sourceFiles;
    private final List<IndexFileMeta> payloads;

    PkSortedIndexGroup(List<PrimaryKeyIndexSourceFile> sourceFiles, List<IndexFileMeta> payloads) {
        this.sourceFiles = Collections.unmodifiableList(new ArrayList<>(sourceFiles));
        this.payloads = Collections.unmodifiableList(new ArrayList<>(payloads));
    }

    static Optional<PkSortedIndexGroup> create(
            int fieldId,
            String indexType,
            PrimaryKeyIndexSourceFile sourceFile,
            List<IndexFileMeta> payloads) {
        return create(fieldId, indexType, Collections.singletonList(sourceFile), payloads);
    }

    static Optional<PkSortedIndexGroup> create(
            int fieldId,
            String indexType,
            List<PrimaryKeyIndexSourceFile> sourceFiles,
            List<IndexFileMeta> payloads) {
        long sourceRowCount = 0;
        Set<String> sourceNames = new HashSet<>();
        for (PrimaryKeyIndexSourceFile sourceFile : sourceFiles) {
            if (!sourceNames.add(sourceFile.fileName())) {
                return Optional.empty();
            }
            try {
                sourceRowCount = Math.addExact(sourceRowCount, sourceFile.rowCount());
            } catch (ArithmeticException e) {
                return Optional.empty();
            }
        }
        if (sourceFiles.isEmpty()) {
            return Optional.empty();
        }

        long payloadRowCount = 0;
        Set<String> payloadNames = new HashSet<>();
        for (IndexFileMeta payload : payloads) {
            GlobalIndexMeta meta = payload.globalIndexMeta();
            List<PrimaryKeyIndexSourceFile> payloadSources =
                    PrimaryKeyIndexSourceMeta.fromIndexFile(payload).sourceFiles();
            if (!payloadNames.add(payload.fileName())
                    || !sourceFiles.equals(payloadSources)
                    || !indexType.equals(payload.indexType())
                    || meta == null
                    || meta.indexFieldId() != fieldId
                    || meta.rowRangeStart() != 0
                    || meta.rowRangeEnd() != sourceRowCount - 1) {
                return Optional.empty();
            }
            try {
                payloadRowCount = Math.addExact(payloadRowCount, payload.rowCount());
            } catch (ArithmeticException e) {
                return Optional.empty();
            }
        }
        if (payloadRowCount != sourceRowCount) {
            return Optional.empty();
        }
        return Optional.of(new PkSortedIndexGroup(sourceFiles, payloads));
    }

    public PrimaryKeyIndexSourceFile sourceFile() {
        if (sourceFiles.size() != 1) {
            throw new IllegalStateException(
                    String.format(
                            "Expected exactly one source file, but found %s.", sourceFiles.size()));
        }
        return sourceFiles.get(0);
    }

    public List<PrimaryKeyIndexSourceFile> sourceFiles() {
        return sourceFiles;
    }

    public List<IndexFileMeta> payloads() {
        return payloads;
    }

    public String identity() {
        List<String> names = new ArrayList<>();
        for (IndexFileMeta payload : payloads) {
            names.add(payload.fileName());
        }
        Collections.sort(names);
        StringBuilder identity = new StringBuilder();
        for (String name : names) {
            identity.append(name.length()).append(':').append(name);
        }
        return identity.toString();
    }
}
