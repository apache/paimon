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

package org.apache.paimon.index.pkvector;

import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.pk.PrimaryKeyIndexSourceFile;
import org.apache.paimon.index.pk.PrimaryKeyIndexSourceMeta;
import org.apache.paimon.index.pk.PrimaryKeyIndexSourcePolicy;
import org.apache.paimon.io.DataFileMeta;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Immutable primary-key vector-index state derived from one bucket's active payload metadata. */
public final class PkVectorBucketIndexState {

    private final int vectorFieldId;
    private final String indexType;
    private final List<IndexFileMeta> annSegments;
    private final List<IndexFileMeta> staleSegments;
    private final Map<String, IndexFileMeta> sourceFileToAnnSegment;

    public static PkVectorBucketIndexState fromActiveDataFiles(
            int vectorFieldId,
            String indexType,
            List<DataFileMeta> activeDataFiles,
            List<IndexFileMeta> activePayloads) {
        Map<Integer, List<PrimaryKeyIndexSourceFile>> sourcesByLevel = new TreeMap<>();
        for (DataFileMeta dataFile : activeDataFiles) {
            if (PrimaryKeyIndexSourcePolicy.shouldRead(dataFile)) {
                sourcesByLevel
                        .computeIfAbsent(dataFile.level(), ignored -> new ArrayList<>())
                        .add(
                                new PrimaryKeyIndexSourceFile(
                                        dataFile.fileName(), dataFile.rowCount()));
            }
        }
        for (List<PrimaryKeyIndexSourceFile> sources : sourcesByLevel.values()) {
            sources.sort(Comparator.comparing(PrimaryKeyIndexSourceFile::fileName));
        }

        Map<Integer, List<IndexFileMeta>> segmentsByLevel = new TreeMap<>();
        List<IndexFileMeta> stale = new ArrayList<>();
        for (IndexFileMeta segment : activePayloads) {
            try {
                PkVectorBucketIndexState singleton =
                        new PkVectorBucketIndexState(
                                vectorFieldId, indexType, Collections.singletonList(segment));
                PrimaryKeyIndexSourceMeta sourceMeta =
                        PrimaryKeyIndexSourceMeta.fromIndexFile(segment);
                List<PrimaryKeyIndexSourceFile> desired =
                        sourcesByLevel.get(sourceMeta.dataLevel());
                if (singleton.annSegments().size() != 1
                        || desired == null
                        || !desired.equals(sourceMeta.sourceFiles())) {
                    stale.add(segment);
                } else {
                    segmentsByLevel
                            .computeIfAbsent(sourceMeta.dataLevel(), ignored -> new ArrayList<>())
                            .add(segment);
                }
            } catch (RuntimeException ignored) {
                stale.add(segment);
            }
        }

        List<IndexFileMeta> current = new ArrayList<>();
        for (List<IndexFileMeta> levelSegments : segmentsByLevel.values()) {
            if (levelSegments.size() == 1) {
                current.add(levelSegments.get(0));
            } else {
                stale.addAll(levelSegments);
            }
        }
        PkVectorBucketIndexState state =
                new PkVectorBucketIndexState(vectorFieldId, indexType, current);
        return new PkVectorBucketIndexState(
                vectorFieldId, indexType, state.annSegments, stale, state.sourceFileToAnnSegment);
    }

    public PkVectorBucketIndexState(
            int vectorFieldId, String indexType, List<IndexFileMeta> activePayloads) {
        this.vectorFieldId = vectorFieldId;
        this.indexType = indexType;

        Map<String, IndexFileMeta> payloadsByName = new LinkedHashMap<>();
        Map<String, IndexFileMeta> annBySource = new LinkedHashMap<>();
        for (IndexFileMeta payload : activePayloads) {
            checkArgument(
                    payloadsByName.put(payload.fileName(), payload) == null,
                    "Active vector payload %s appears more than once in the index manifest.",
                    payload.fileName());
            PrimaryKeyIndexSourceMeta sourceMeta = validatedSourceMeta(payload);
            for (PrimaryKeyIndexSourceFile sourceFile : sourceMeta.sourceFiles()) {
                IndexFileMeta previous = annBySource.put(sourceFile.fileName(), payload);
                checkArgument(
                        previous == null,
                        "Source data file %s is covered by both ANN segments %s and %s.",
                        sourceFile.fileName(),
                        previous == null ? "" : previous.fileName(),
                        payload.fileName());
            }
        }

        this.annSegments = Collections.unmodifiableList(new java.util.ArrayList<>(activePayloads));
        this.staleSegments = Collections.emptyList();
        this.sourceFileToAnnSegment = Collections.unmodifiableMap(annBySource);
    }

    private PkVectorBucketIndexState(
            int vectorFieldId,
            String indexType,
            List<IndexFileMeta> annSegments,
            List<IndexFileMeta> staleSegments,
            Map<String, IndexFileMeta> sourceFileToAnnSegment) {
        this.vectorFieldId = vectorFieldId;
        this.indexType = indexType;
        this.annSegments = Collections.unmodifiableList(new ArrayList<>(annSegments));
        this.staleSegments = Collections.unmodifiableList(new ArrayList<>(staleSegments));
        this.sourceFileToAnnSegment =
                Collections.unmodifiableMap(new LinkedHashMap<>(sourceFileToAnnSegment));
    }

    public int vectorFieldId() {
        return vectorFieldId;
    }

    public String indexType() {
        return indexType;
    }

    public List<IndexFileMeta> annSegments() {
        return annSegments;
    }

    public List<IndexFileMeta> staleSegments() {
        return staleSegments;
    }

    public Map<String, IndexFileMeta> sourceFileToAnnSegment() {
        return sourceFileToAnnSegment;
    }

    private PrimaryKeyIndexSourceMeta validatedSourceMeta(IndexFileMeta payload) {
        validateIdentity(payload);
        return PrimaryKeyIndexSourceMeta.fromIndexFile(payload);
    }

    private void validateIdentity(IndexFileMeta payload) {
        checkArgument(
                indexType.equals(payload.indexType()),
                "Vector payload %s has a different index type.",
                payload.fileName());
        checkArgument(
                payload.globalIndexMeta() != null
                        && vectorFieldId == payload.globalIndexMeta().indexFieldId(),
                "Vector payload %s has a different vector field.",
                payload.fileName());
    }
}
