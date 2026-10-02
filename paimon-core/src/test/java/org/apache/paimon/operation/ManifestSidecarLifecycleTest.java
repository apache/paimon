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

import org.apache.paimon.manifest.IndexManifestFile;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.operation.commit.CommitCleaner;
import org.apache.paimon.utils.Pair;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/** Tests ownership of manifest extra files during expiration and aborted commits. */
class ManifestSidecarLifecycleTest {

    @Test
    void retainedManifestProtectsSharedExtraFiles() {
        ManifestFileMeta retained = manifest("retained", "shared.avro.sidecar", "retained-extra");
        ManifestFileMeta expired = manifest("expired", "shared.avro.sidecar", "expired-extra");
        FileDeletionBase<?> deletion = mock(FileDeletionBase.class, CALLS_REAL_METHODS);
        doReturn(Arrays.asList(retained, expired)).when(deletion).tryReadManifestList("old-list");
        Set<String> skipping = new HashSet<>();
        FileDeletionBase.addManifestToSkippingSet(skipping, retained);
        Set<String> removed = new HashSet<>();
        deletion.collectUnusedManifestList("old-list", skipping, removed);
        assertThat(removed).containsExactlyInAnyOrder("expired", "expired-extra", "old-list");
    }

    @Test
    void failedCommitDeletesOnlyNewManifestsWithTheirMetadata() {
        ManifestList lists = mock(ManifestList.class);
        ManifestFile files = mock(ManifestFile.class);
        IndexManifestFile indexes = mock(IndexManifestFile.class);
        ManifestFileMeta reused = manifest("reused", "reused.avro.sidecar");
        ManifestFileMeta added = manifest("added", "added.avro.sidecar");
        CommitCleaner cleaner = new CommitCleaner(lists, files, indexes);
        cleaner.cleanUpNoReuseTmpManifests(
                Pair.of("new-base-list", 1L),
                Collections.singletonList(reused),
                Arrays.asList(reused, added));
        verify(files).delete(added);
        verify(files, never()).delete(reused);
        verify(lists).delete("new-base-list");
        verifyNoInteractions(indexes);
    }

    @Test
    void failedCommitDeletesDeltaAndChangelogManifestExtras() {
        ManifestList lists = mock(ManifestList.class);
        ManifestFile files = mock(ManifestFile.class);
        IndexManifestFile indexes = mock(IndexManifestFile.class);
        ManifestFileMeta delta = manifest("delta", "delta.avro.sidecar");
        ManifestFileMeta changelog = manifest("changelog", "changelog.avro.sidecar");
        when(lists.read("delta-list")).thenReturn(Collections.singletonList(delta));
        when(lists.read("changelog-list")).thenReturn(Collections.singletonList(changelog));
        new CommitCleaner(lists, files, indexes)
                .cleanUpReuseTmpManifests(
                        Pair.of("delta-list", 1L),
                        Pair.of("changelog-list", 1L),
                        "same-index",
                        "same-index");
        verify(files).delete(delta);
        verify(files).delete(changelog);
        verify(lists).delete("delta-list");
        verify(lists).delete("changelog-list");
        verifyNoInteractions(indexes);
    }

    private static ManifestFileMeta manifest(String name, String... extras) {
        ManifestFileMeta meta = mock(ManifestFileMeta.class);
        when(meta.fileName()).thenReturn(name);
        when(meta.extraFiles()).thenReturn(Arrays.asList(extras));
        return meta;
    }
}
