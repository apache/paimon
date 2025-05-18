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

package org.apache.paimon.manifest;

import org.apache.paimon.operation.ManifestFileMerger;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Test {@link ManifestFile}. for table without partition */
public class NoPartitionManifestFileMetaTest extends ManifestFileMetaTestBase {
    private final RowType noPartitionType = RowType.of();

    @TempDir java.nio.file.Path tempDir;
    private ManifestFile manifestFile;

    @BeforeEach
    public void beforeEach() {
        manifestFile = createManifestFile(tempDir.toString());
    }

    @Test
    public void testMerge() {
        List<ManifestFileMeta> input = createBaseManifestFileMetas(false);
        addDeltaManifests(input, false);

        List<ManifestFileMeta> merged =
                ManifestFileMerger.merge(
                        input, manifestFile, 500, 3, 200, getPartitionType(), null);
        assertEquivalentEntries(input, merged);

        // the first one is not deleted, it should not be merged
        assertThat(merged.get(0)).isSameAs(input.get(0));
    }

    @Test
    public void testMergeFullCompactionWithoutDeleteFile() {
        // entries are All ADD.
        List<ManifestFileMeta> input = new ArrayList<>();
        // base
        for (int j = 0; j < 6; j++) {
            List<ManifestEntry> entrys = new ArrayList<>();
            for (int i = 1; i < 50; i++) {
                entrys.add(makeEntry(true, String.format(manifestFileNameTemplate, j, i), null));
            }
            input.add(makeManifest(entrys.toArray(new ManifestEntry[0])));
        }
        // The base file all meet the manifest file size.
        long threshold = input.stream().mapToLong(ManifestFileMeta::fileSize).min().getAsLong();
        Set<String> baseFiles =
                input.stream().map(ManifestFileMeta::fileName).collect(Collectors.toSet());

        // assert base manifest are not accessed
        for (String baseFile : baseFiles) {
            manifestFile.delete(baseFile);
        }

        // delta
        input.add(makeManifest(makeEntry(true, "A", null)));
        input.add(makeManifest(makeEntry(true, "B", null)));
        input.add(makeManifest(makeEntry(true, "C", null)));
        input.add(makeManifest(makeEntry(true, "D", null)));
        input.add(makeManifest(makeEntry(true, "E", null)));
        input.add(makeManifest(makeEntry(true, "F", null)));
        input.add(makeManifest(makeEntry(true, "G", null)));

        List<ManifestFileMeta> merged =
                ManifestFileMerger.merge(
                        input, manifestFile, threshold, 3, 200, getPartitionType(), null);
        assertEquivalentEntries(
                input.stream()
                        .filter(f -> !baseFiles.contains(f.fileName()))
                        .collect(Collectors.toList()),
                merged.stream()
                        .filter(f -> !baseFiles.contains(f.fileName()))
                        .collect(Collectors.toList()));
    }

    @Override
    public ManifestFile getManifestFile() {
        return manifestFile;
    }

    @Override
    public RowType getPartitionType() {
        return noPartitionType;
    }
}
