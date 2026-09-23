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

package org.apache.paimon.index.pkfulltext;

import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.pk.PrimaryKeyIndexSourceFile;
import org.apache.paimon.index.pk.PrimaryKeyIndexSourceMeta;
import org.apache.paimon.index.pk.PrimaryKeyIndexSourcePolicy;
import org.apache.paimon.io.DataFileMeta;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Immutable source-aligned primary-key full-text state for one bucket and definition. */
public final class PkFullTextBucketIndexState {

    private final int textFieldId;
    private final List<IndexFileMeta> currentPayloads;
    private final List<IndexFileMeta> stalePayloads;
    private final Map<String, IndexFileMeta> payloadBySourceFile;

    public static PkFullTextBucketIndexState fromActiveDataFiles(
            int textFieldId,
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

        Map<Integer, List<IndexFileMeta>> payloadsByLevel = new TreeMap<>();
        List<IndexFileMeta> stale = new ArrayList<>();
        for (IndexFileMeta payload : activePayloads) {
            GlobalIndexMeta globalMeta = payload.globalIndexMeta();
            if (!PkFullTextIndexFile.INDEX_TYPE.equals(payload.indexType()) || globalMeta == null) {
                continue;
            }
            if (globalMeta.indexFieldId() != textFieldId) {
                if (globalMeta.sourceMeta() != null) {
                    stale.add(payload);
                }
                continue;
            }
            try {
                PrimaryKeyIndexSourceMeta sourceMeta =
                        PrimaryKeyIndexSourceMeta.fromIndexFile(payload);
                List<PrimaryKeyIndexSourceFile> desired =
                        sourcesByLevel.get(sourceMeta.dataLevel());
                PkFullTextBucketIndexState singleton =
                        new PkFullTextBucketIndexState(
                                textFieldId, Collections.singletonList(payload));
                if (desired == null
                        || !desired.equals(sourceMeta.sourceFiles())
                        || singleton.currentPayloads().size() != 1) {
                    stale.add(payload);
                } else {
                    payloadsByLevel
                            .computeIfAbsent(sourceMeta.dataLevel(), ignored -> new ArrayList<>())
                            .add(payload);
                }
            } catch (RuntimeException ignored) {
                stale.add(payload);
            }
        }

        List<IndexFileMeta> current = new ArrayList<>();
        for (List<IndexFileMeta> levelPayloads : payloadsByLevel.values()) {
            if (levelPayloads.size() == 1) {
                current.add(levelPayloads.get(0));
            } else {
                stale.addAll(levelPayloads);
            }
        }
        PkFullTextBucketIndexState state = new PkFullTextBucketIndexState(textFieldId, current);
        return new PkFullTextBucketIndexState(
                textFieldId, state.currentPayloads, stale, state.payloadBySourceFile);
    }

    private PkFullTextBucketIndexState(
            int textFieldId,
            List<IndexFileMeta> currentPayloads,
            List<IndexFileMeta> stalePayloads,
            Map<String, IndexFileMeta> payloadBySourceFile) {
        this.textFieldId = textFieldId;
        this.currentPayloads = Collections.unmodifiableList(new ArrayList<>(currentPayloads));
        this.stalePayloads = Collections.unmodifiableList(new ArrayList<>(stalePayloads));
        this.payloadBySourceFile =
                Collections.unmodifiableMap(new LinkedHashMap<>(payloadBySourceFile));
    }

    public PkFullTextBucketIndexState(int textFieldId, List<IndexFileMeta> activePayloads) {
        this.textFieldId = textFieldId;

        List<IndexFileMeta> current = new ArrayList<>();
        List<IndexFileMeta> stale = new ArrayList<>();
        Map<String, IndexFileMeta> bySource = new LinkedHashMap<>();
        Set<String> payloadNames = new HashSet<>();
        for (IndexFileMeta payload : activePayloads) {
            GlobalIndexMeta globalMeta = payload.globalIndexMeta();
            if (!PkFullTextIndexFile.INDEX_TYPE.equals(payload.indexType()) || globalMeta == null) {
                continue;
            }
            if (globalMeta.indexFieldId() != textFieldId) {
                if (globalMeta.sourceMeta() != null) {
                    stale.add(payload);
                }
                continue;
            }
            checkArgument(
                    payloadNames.add(payload.fileName()),
                    "Active full-text payload %s appears more than once in the index manifest.",
                    payload.fileName());
            PrimaryKeyIndexSourceMeta sourceMeta = PrimaryKeyIndexSourceMeta.fromIndexFile(payload);
            long sourceRowCount = 0;
            for (PrimaryKeyIndexSourceFile source : sourceMeta.sourceFiles()) {
                sourceRowCount = Math.addExact(sourceRowCount, source.rowCount());
            }
            checkArgument(
                    payload.rowCount() == sourceRowCount
                            && globalMeta.rowRangeStart() == 0
                            && globalMeta.rowRangeEnd() == sourceRowCount - 1,
                    "Full-text archive %s row metadata does not match its source files.",
                    payload.fileName());
            for (PrimaryKeyIndexSourceFile source : sourceMeta.sourceFiles()) {
                IndexFileMeta previous = bySource.put(source.fileName(), payload);
                checkArgument(
                        previous == null,
                        "Source data file %s is covered by both full-text archives %s and %s.",
                        source.fileName(),
                        previous == null ? "" : previous.fileName(),
                        payload.fileName());
            }
            current.add(payload);
        }
        this.currentPayloads = Collections.unmodifiableList(current);
        this.stalePayloads = Collections.unmodifiableList(stale);
        this.payloadBySourceFile = Collections.unmodifiableMap(bySource);
    }

    public int textFieldId() {
        return textFieldId;
    }

    public List<IndexFileMeta> currentPayloads() {
        return currentPayloads;
    }

    public List<IndexFileMeta> stalePayloads() {
        return stalePayloads;
    }

    public Map<String, IndexFileMeta> payloadBySourceFile() {
        return payloadBySourceFile;
    }
}
