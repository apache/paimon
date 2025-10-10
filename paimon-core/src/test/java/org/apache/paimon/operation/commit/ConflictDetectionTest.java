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

package org.apache.paimon.operation.commit;

import org.apache.paimon.index.DeletionVectorMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.manifest.FileEntry;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.SimpleFileEntry;
import org.apache.paimon.manifest.SimpleFileEntryWithDV;

import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;

import static org.apache.paimon.data.BinaryRow.EMPTY_ROW;
import static org.apache.paimon.deletionvectors.DeletionVectorsIndexFile.DELETION_VECTORS_INDEX;
import static org.apache.paimon.manifest.FileKind.ADD;
import static org.apache.paimon.manifest.FileKind.DELETE;
import static org.apache.paimon.operation.commit.ConflictDetection.buildBaseEntriesWithDV;
import static org.apache.paimon.operation.commit.ConflictDetection.buildDeltaEntriesWithDV;
import static org.assertj.core.api.Assertions.assertThat;

class ConflictDetectionTest {

    @Test
    public void testBuildBaseEntriesWithDV() {
        {
            // Scene 1
            List<SimpleFileEntry> baseEntries = new ArrayList<>();
            baseEntries.add(createFileEntry("f1", ADD));
            baseEntries.add(createFileEntry("f2", ADD));

            List<IndexManifestEntry> deltaIndexEntries = new ArrayList<>();
            deltaIndexEntries.add(createDvIndexEntry("dv1", ADD, Arrays.asList("f2")));

            assertThat(buildBaseEntriesWithDV(baseEntries, deltaIndexEntries))
                    .containsExactlyInAnyOrder(
                            createFileEntryWithDV("f1", ADD, null),
                            createFileEntryWithDV("f2", ADD, "dv1"));
        }

        {
            // Scene 2: skip delete dv
            List<SimpleFileEntry> baseEntries = new ArrayList<>();
            baseEntries.add(createFileEntry("f1", ADD));
            baseEntries.add(createFileEntry("f2", ADD));

            List<IndexManifestEntry> deltaIndexEntries = new ArrayList<>();
            deltaIndexEntries.add(createDvIndexEntry("dv1", DELETE, Arrays.asList("f2")));

            assertThat(buildBaseEntriesWithDV(baseEntries, deltaIndexEntries))
                    .containsExactlyInAnyOrder(
                            createFileEntryWithDV("f1", ADD, null),
                            createFileEntryWithDV("f2", ADD, null));
        }
    }

    @Test
    public void testBuildDeltaEntriesWithDV() {
        {
            // Scene 1: update f2's dv
            List<SimpleFileEntry> baseEntries = new ArrayList<>();
            baseEntries.add(createFileEntryWithDV("f1", ADD, "dv1"));
            baseEntries.add(createFileEntryWithDV("f2", ADD, null));

            List<SimpleFileEntry> deltaEntries = new ArrayList<>();
            deltaEntries.add(createFileEntry("f2", DELETE));
            deltaEntries.add(createFileEntry("f2_new", ADD));

            List<IndexManifestEntry> deltaIndexEntries = new ArrayList<>();
            deltaIndexEntries.add(createDvIndexEntry("dv2", ADD, Arrays.asList("f2_new")));

            assertThat(buildDeltaEntriesWithDV(baseEntries, deltaEntries, deltaIndexEntries))
                    .containsExactlyInAnyOrder(
                            createFileEntryWithDV("f2", DELETE, null),
                            createFileEntryWithDV("f2_new", ADD, "dv2"));
        }

        {
            // Scene 2: update f2 and merge f1's dv
            List<SimpleFileEntry> baseEntries = new ArrayList<>();
            baseEntries.add(createFileEntryWithDV("f1", ADD, "dv1"));
            baseEntries.add(createFileEntryWithDV("f2", ADD, null));

            List<SimpleFileEntry> deltaEntries = new ArrayList<>();
            deltaEntries.add(createFileEntry("f2", DELETE));
            deltaEntries.add(createFileEntry("f2_new", ADD));
            deltaEntries.add(createFileEntry("f3", ADD));

            List<IndexManifestEntry> deltaIndexEntries = new ArrayList<>();
            deltaIndexEntries.add(createDvIndexEntry("dv1", DELETE, Arrays.asList("f1")));
            deltaIndexEntries.add(createDvIndexEntry("dv2", ADD, Arrays.asList("f1", "f2_new")));

            assertThat(buildDeltaEntriesWithDV(baseEntries, deltaEntries, deltaIndexEntries))
                    .containsExactlyInAnyOrder(
                            createFileEntryWithDV("f1", DELETE, "dv1"),
                            createFileEntryWithDV("f1", ADD, "dv2"),
                            createFileEntryWithDV("f2", DELETE, null),
                            createFileEntryWithDV("f2_new", ADD, "dv2"),
                            createFileEntryWithDV("f3", ADD, null));
        }

        {
            // Scene 3: update f2 (with dv) and merge f1's dv
            List<SimpleFileEntry> baseEntries = new ArrayList<>();
            baseEntries.add(createFileEntryWithDV("f1", ADD, "dv1"));
            baseEntries.add(createFileEntryWithDV("f2", ADD, "dv2"));

            List<SimpleFileEntry> deltaEntries = new ArrayList<>();
            deltaEntries.add(createFileEntry("f2", DELETE));
            deltaEntries.add(createFileEntry("f2_new", ADD));
            deltaEntries.add(createFileEntry("f3", ADD));

            List<IndexManifestEntry> deltaIndexEntries = new ArrayList<>();
            deltaIndexEntries.add(createDvIndexEntry("dv1", DELETE, Arrays.asList("f1")));
            deltaIndexEntries.add(createDvIndexEntry("dv2", DELETE, Arrays.asList("f2")));
            deltaIndexEntries.add(createDvIndexEntry("dv3", ADD, Arrays.asList("f1", "f2_new")));

            assertThat(buildDeltaEntriesWithDV(baseEntries, deltaEntries, deltaIndexEntries))
                    .containsExactlyInAnyOrder(
                            createFileEntryWithDV("f1", DELETE, "dv1"),
                            createFileEntryWithDV("f1", ADD, "dv3"),
                            createFileEntryWithDV("f2", DELETE, "dv2"),
                            createFileEntryWithDV("f2_new", ADD, "dv3"),
                            createFileEntryWithDV("f3", ADD, null));
        }

        {
            // Scene 4: full compact
            List<SimpleFileEntry> baseEntries = new ArrayList<>();
            baseEntries.add(createFileEntryWithDV("f1", ADD, null));
            baseEntries.add(createFileEntryWithDV("f2", ADD, "dv1"));
            baseEntries.add(createFileEntryWithDV("f3", ADD, "dv1"));
            baseEntries.add(createFileEntryWithDV("f4", ADD, "dv2"));

            List<SimpleFileEntry> deltaEntries = new ArrayList<>();
            deltaEntries.add(createFileEntry("f1", DELETE));
            deltaEntries.add(createFileEntry("f2", DELETE));
            deltaEntries.add(createFileEntry("f3", DELETE));
            deltaEntries.add(createFileEntry("f4", DELETE));
            deltaEntries.add(createFileEntry("f5_compact", ADD));

            List<IndexManifestEntry> deltaIndexEntries = new ArrayList<>();
            deltaIndexEntries.add(createDvIndexEntry("dv1", DELETE, Arrays.asList("f2", "f3")));
            deltaIndexEntries.add(createDvIndexEntry("dv2", DELETE, Arrays.asList("f4")));

            assertThat(buildDeltaEntriesWithDV(baseEntries, deltaEntries, deltaIndexEntries))
                    .containsExactlyInAnyOrder(
                            createFileEntryWithDV("f1", DELETE, null),
                            createFileEntryWithDV("f2", DELETE, "dv1"),
                            createFileEntryWithDV("f3", DELETE, "dv1"),
                            createFileEntryWithDV("f4", DELETE, "dv2"),
                            createFileEntryWithDV("f5_compact", ADD, null));
        }

        {
            // Scene 5: merge into with update, delete and insert
            List<SimpleFileEntry> baseEntries = new ArrayList<>();
            baseEntries.add(createFileEntryWithDV("f1", ADD, null));
            baseEntries.add(createFileEntryWithDV("f2", ADD, null));
            baseEntries.add(createFileEntryWithDV("f3", ADD, "dv1"));
            baseEntries.add(createFileEntryWithDV("f4", ADD, "dv1"));
            baseEntries.add(createFileEntryWithDV("f5", ADD, "dv2"));

            List<SimpleFileEntry> deltaEntries = new ArrayList<>();
            deltaEntries.add(createFileEntry("f2", DELETE));
            deltaEntries.add(createFileEntry("f3", DELETE));
            deltaEntries.add(createFileEntry("f3_new", ADD));
            deltaEntries.add(createFileEntry("f7", ADD));

            List<IndexManifestEntry> deltaIndexEntries = new ArrayList<>();
            deltaIndexEntries.add(createDvIndexEntry("dv1", DELETE, Arrays.asList("f3", "f4")));
            deltaIndexEntries.add(createDvIndexEntry("dv2", DELETE, Arrays.asList("f5")));
            deltaIndexEntries.add(createDvIndexEntry("dv3", ADD, Arrays.asList("f1", "f4", "f5")));

            assertThat(buildDeltaEntriesWithDV(baseEntries, deltaEntries, deltaIndexEntries))
                    .containsExactlyInAnyOrder(
                            createFileEntryWithDV("f1", DELETE, null),
                            createFileEntryWithDV("f1", ADD, "dv3"),
                            createFileEntryWithDV("f2", DELETE, null),
                            createFileEntryWithDV("f3", DELETE, "dv1"),
                            createFileEntryWithDV("f3_new", ADD, null),
                            createFileEntryWithDV("f4", DELETE, "dv1"),
                            createFileEntryWithDV("f4", ADD, "dv3"),
                            createFileEntryWithDV("f5", DELETE, "dv2"),
                            createFileEntryWithDV("f5", ADD, "dv3"),
                            createFileEntryWithDV("f7", ADD, null));
        }
    }

    @Test
    public void testConflictDeletionWithDV() {
        {
            // Scene 1: base -------------> update2 (conflict)
            //           f1          ^         <f1, +dv2>
            //                       |
            //                  update1 (finished)
            //                    <f1, +dv1>
            List<SimpleFileEntry> update1Entries = new ArrayList<>();
            update1Entries.add(createFileEntryWithDV("f1", ADD, "dv1"));

            List<SimpleFileEntry> update2DeltaEntries = new ArrayList<>();

            List<IndexManifestEntry> update2DeltaIndexEntries = new ArrayList<>();
            update2DeltaIndexEntries.add(createDvIndexEntry("dv2", ADD, Arrays.asList("f1")));

            List<SimpleFileEntry> update2DeltaEntriesWithDV =
                    buildDeltaEntriesWithDV(
                            update1Entries, update2DeltaEntries, update2DeltaIndexEntries);
            assertThat(update2DeltaEntriesWithDV)
                    .containsExactlyInAnyOrder(
                            createFileEntryWithDV("f1", DELETE, null),
                            createFileEntryWithDV("f1", ADD, "dv2"));
            assertConflict(update1Entries, update2DeltaEntriesWithDV);
        }

        {
            // Scene 2: base -------------> update2 (conflict)
            //         <f1, dv0>     ^        <f1, +dv2>
            //                       |
            //                  update1 (finished)
            //                    <f1, +dv1>
            List<SimpleFileEntry> update1Entries = new ArrayList<>();
            update1Entries.add(createFileEntryWithDV("f1", ADD, "dv1"));

            List<SimpleFileEntry> update2DeltaEntries = new ArrayList<>();

            List<IndexManifestEntry> update2DeltaIndexEntries = new ArrayList<>();
            update2DeltaIndexEntries.add(createDvIndexEntry("dv0", DELETE, Arrays.asList("f1")));
            update2DeltaIndexEntries.add(createDvIndexEntry("dv2", ADD, Arrays.asList("f1")));

            List<SimpleFileEntry> update2DeltaEntriesWithDV =
                    buildDeltaEntriesWithDV(
                            update1Entries, update2DeltaEntries, update2DeltaIndexEntries);
            assertThat(update2DeltaEntriesWithDV)
                    .containsExactlyInAnyOrder(
                            createFileEntryWithDV("f1", DELETE, "dv0"),
                            createFileEntryWithDV("f1", ADD, "dv2"));
            assertConflict(update1Entries, update2DeltaEntriesWithDV);
        }

        {
            // Scene 3: base -------------> update2 (conflict)
            //         <f1, dv0>      ^     <-f1, -dv0>, <+f3, null>
            //                        |
            //                  update1 (finished)
            //                 <-f1, -dv0>, <+f2, dv1>
            List<SimpleFileEntry> update1Entries = new ArrayList<>();
            update1Entries.add(createFileEntryWithDV("f2", ADD, "dv1"));

            List<SimpleFileEntry> update2DeltaEntries = new ArrayList<>();
            update2DeltaEntries.add(createFileEntry("f1", DELETE));
            update2DeltaEntries.add(createFileEntry("f3", ADD));

            List<IndexManifestEntry> update2DeltaIndexEntries = new ArrayList<>();
            update2DeltaIndexEntries.add(createDvIndexEntry("dv0", DELETE, Arrays.asList("f1")));

            List<SimpleFileEntry> update2DeltaEntriesWithDV =
                    buildDeltaEntriesWithDV(
                            update1Entries, update2DeltaEntries, update2DeltaIndexEntries);
            assertThat(update2DeltaEntriesWithDV)
                    .containsExactlyInAnyOrder(
                            createFileEntryWithDV("f1", DELETE, "dv0"),
                            createFileEntryWithDV("f3", ADD, null));
            assertConflict(update1Entries, update2DeltaEntriesWithDV);
        }
    }

    private SimpleFileEntry createFileEntry(String fileName, FileKind kind) {
        return new SimpleFileEntry(
                kind,
                EMPTY_ROW,
                0,
                1,
                0,
                fileName,
                Collections.emptyList(),
                null,
                EMPTY_ROW,
                EMPTY_ROW,
                null);
    }

    private SimpleFileEntryWithDV createFileEntryWithDV(
            String fileName, FileKind kind, @Nullable String dvFileName) {
        return new SimpleFileEntryWithDV(createFileEntry(fileName, kind), dvFileName);
    }

    private IndexManifestEntry createDvIndexEntry(
            String fileName, FileKind kind, List<String> fileNames) {
        LinkedHashMap<String, DeletionVectorMeta> dvRanges = new LinkedHashMap<>();
        for (String name : fileNames) {
            dvRanges.put(name, new DeletionVectorMeta(name, 1, 1, 1L));
        }
        return new IndexManifestEntry(
                kind,
                EMPTY_ROW,
                0,
                new IndexFileMeta(
                        DELETION_VECTORS_INDEX, fileName, 11, dvRanges.size(), dvRanges, null));
    }

    private void assertConflict(
            List<SimpleFileEntry> baseEntries, List<SimpleFileEntry> deltaEntries) {
        ArrayList<SimpleFileEntry> simpleFileEntryWithDVS = new ArrayList<>(baseEntries);
        simpleFileEntryWithDVS.addAll(deltaEntries);
        Collection<SimpleFileEntry> merged = FileEntry.mergeEntries(simpleFileEntryWithDVS);
        int deleteCount = 0;
        for (SimpleFileEntry simpleFileEntryWithDV : merged) {
            if (simpleFileEntryWithDV.kind().equals(FileKind.DELETE)) {
                deleteCount++;
            }
        }
        assert (deleteCount > 0);
    }
}
