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

/** All rotated payloads that index one physical source data file. */
public final class PkSortedIndexGroup {

    private final PrimaryKeyIndexSourceFile sourceFile;
    private final List<IndexFileMeta> payloads;

    PkSortedIndexGroup(PrimaryKeyIndexSourceFile sourceFile, List<IndexFileMeta> payloads) {
        this.sourceFile = sourceFile;
        this.payloads = Collections.unmodifiableList(new ArrayList<>(payloads));
    }

    static Optional<PkSortedIndexGroup> create(
            int fieldId,
            String indexType,
            PrimaryKeyIndexSourceFile sourceFile,
            List<IndexFileMeta> payloads) {
        long payloadRowCount = 0;
        Set<String> payloadNames = new HashSet<>();
        for (IndexFileMeta payload : payloads) {
            GlobalIndexMeta meta = payload.globalIndexMeta();
            PrimaryKeyIndexSourceFile payloadSource =
                    PrimaryKeyIndexSourceMeta.fromIndexFile(payload).sourceFile();
            if (!payloadNames.add(payload.fileName())
                    || !sourceFile.equals(payloadSource)
                    || !indexType.equals(payload.indexType())
                    || meta == null
                    || meta.indexFieldId() != fieldId
                    || meta.rowRangeStart() != 0
                    || meta.rowRangeEnd() != sourceFile.rowCount() - 1) {
                return Optional.empty();
            }
            payloadRowCount += payload.rowCount();
        }
        if (payloadRowCount != sourceFile.rowCount()) {
            return Optional.empty();
        }
        return Optional.of(new PkSortedIndexGroup(sourceFile, payloads));
    }

    public PrimaryKeyIndexSourceFile sourceFile() {
        return sourceFile;
    }

    public List<IndexFileMeta> payloads() {
        return payloads;
    }
}
