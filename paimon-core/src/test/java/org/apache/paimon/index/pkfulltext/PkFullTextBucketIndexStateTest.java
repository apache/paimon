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

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests restored file-aligned primary-key full-text state. */
class PkFullTextBucketIndexStateTest {

    @Test
    void testClassifiesMatchingFieldAndRetiresOtherFields() {
        IndexFileMeta current = payload("current", "data", 7);
        IndexFileMeta alsoCurrent = payload("also-current", "other-data", 7);
        IndexFileMeta otherField = payload("other", "third-data", 8);

        PkFullTextBucketIndexState state =
                PkFullTextBucketIndexState.fromActivePayloads(
                        7, Arrays.asList(current, alsoCurrent, otherField));

        assertThat(state.currentPayloads()).containsExactly(current, alsoCurrent);
        assertThat(state.stalePayloads()).containsExactly(otherField);
        assertThat(state.payloadBySourceFile()).containsEntry("data", current);
    }

    @Test
    void testRejectsDuplicateCurrentCoverage() {
        assertThatThrownBy(
                        () ->
                                PkFullTextBucketIndexState.fromActivePayloads(
                                        7,
                                        Arrays.asList(
                                                payload("first", "data", 7),
                                                payload("second", "data", 7))))
                .hasMessageContaining("data")
                .hasMessageContaining("covered by both full-text archives");
    }

    @Test
    void testMapsEverySourceOfMultiSourceArchive() {
        PrimaryKeyIndexSourceMeta sourceMeta =
                new PrimaryKeyIndexSourceMeta(
                        Arrays.asList(
                                new PrimaryKeyIndexSourceFile("data-1", 1),
                                new PrimaryKeyIndexSourceFile("data-2", 1)));
        IndexFileMeta payload = payload("multi", 7, 2, sourceMeta);

        PkFullTextBucketIndexState state =
                PkFullTextBucketIndexState.fromActivePayloads(
                        7, Collections.singletonList(payload));

        assertThat(state.currentPayloads()).containsExactly(payload);
        assertThat(state.payloadBySourceFile())
                .containsEntry("data-1", payload)
                .containsEntry("data-2", payload);
    }

    private static IndexFileMeta payload(String payloadName, String sourceName, int fieldId) {
        return payload(
                payloadName,
                fieldId,
                1,
                new PrimaryKeyIndexSourceMeta(new PrimaryKeyIndexSourceFile(sourceName, 1)));
    }

    private static IndexFileMeta payload(
            String payloadName, int fieldId, long rowCount, PrimaryKeyIndexSourceMeta sourceMeta) {
        return new IndexFileMeta(
                "full-text",
                payloadName,
                100,
                rowCount,
                new GlobalIndexMeta(0, rowCount - 1, fieldId, null, null, sourceMeta.serialize()),
                null);
    }
}
