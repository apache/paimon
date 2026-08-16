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

package org.apache.paimon.operation;

import org.apache.paimon.Snapshot;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.ManifestEntry;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;

/** Helpers for persisting generated sequence state in snapshot properties. */
class SequenceSnapshotProperties {

    static final String MAX_SEQUENCE_NUMBER = "sequence.generation.max-sequence-number";

    private SequenceSnapshotProperties() {}

    static OptionalLong maxSequenceNumber(@Nullable Snapshot snapshot) {
        if (snapshot == null || snapshot.properties() == null) {
            return OptionalLong.empty();
        }

        String value = snapshot.properties().get(MAX_SEQUENCE_NUMBER);
        if (value == null) {
            return OptionalLong.empty();
        }

        return OptionalLong.of(Long.parseLong(value));
    }

    static Map<String, String> mergeMaxSequenceNumber(
            @Nullable Map<String, String> properties,
            OptionalLong latestMax,
            List<ManifestEntry> deltaFiles) {
        OptionalLong currentMax =
                deltaFiles.stream()
                        .filter(entry -> entry.kind() == FileKind.ADD)
                        .mapToLong(entry -> entry.file().maxSequenceNumber())
                        .max();

        if (!latestMax.isPresent() && !currentMax.isPresent()) {
            return properties == null ? new HashMap<>() : properties;
        }

        long maxSequenceNumber =
                Math.max(latestMax.orElse(Long.MIN_VALUE), currentMax.orElse(Long.MIN_VALUE));
        Map<String, String> merged =
                properties == null ? new HashMap<>() : new HashMap<>(properties);
        merged.put(MAX_SEQUENCE_NUMBER, String.valueOf(maxSequenceNumber));
        return merged;
    }
}
