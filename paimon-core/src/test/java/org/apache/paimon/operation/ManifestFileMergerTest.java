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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.ManifestFileMetaTestBase;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ManifestFileMerger}. */
public class ManifestFileMergerTest extends ManifestFileMetaTestBase {

    private static final RowType NO_PARTITION_TYPE = RowType.of();

    @TempDir java.nio.file.Path tempDir;
    private ManifestFile manifestFile;

    @BeforeEach
    public void beforeEach() {
        manifestFile = createManifestFile(tempDir.toString());
    }

    @Test
    public void testManifestSortFallsBackToForcedLegacyMergeWithoutRowId() {
        List<ManifestFileMeta> input =
                Arrays.asList(
                        makeManifest(makeEntry(true, "base", null)),
                        makeManifest(
                                makeEntry(false, "base", null),
                                makeEntry(true, "replacement", null)));

        Options options = new Options();
        options.set(CoreOptions.MANIFEST_SORT_ENABLED, true);
        options.set(CoreOptions.DATA_EVOLUTION_ENABLED, true);
        options.set(CoreOptions.MANIFEST_MERGE_MIN_COUNT, 100);
        options.set(CoreOptions.MANIFEST_FULL_COMPACTION_FILE_SIZE.key(), Long.MAX_VALUE + "B");
        CoreOptions tableOptions = new CoreOptions(options);

        assertThat(ManifestFileMerger.canUseManifestSort(input, NO_PARTITION_TYPE, tableOptions))
                .isFalse();

        CoreOptions compactOptions =
                FileStoreCommitImpl.manifestCompactionOptions(
                        tableOptions, input, NO_PARTITION_TYPE);
        assertThat(compactOptions.manifestMergeMinCount()).isEqualTo(1);
        assertThat(compactOptions.manifestFullCompactionThresholdSize().getBytes()).isEqualTo(1);

        List<ManifestFileMeta> merged =
                ManifestFileMerger.merge(input, manifestFile, NO_PARTITION_TYPE, compactOptions);
        List<ManifestEntry> mergedEntries =
                merged.stream()
                        .flatMap(
                                meta ->
                                        manifestFile.read(meta.fileName(), meta.fileSize())
                                                .stream())
                        .collect(Collectors.toList());
        assertThat(mergedEntries).noneMatch(entry -> entry.kind() == FileKind.DELETE);
        assertThat(mergedEntries)
                .extracting(entry -> entry.file().fileName())
                .containsExactly("replacement");
    }

    @Override
    public ManifestFile getManifestFile() {
        return manifestFile;
    }

    @Override
    public RowType getPartitionType() {
        return NO_PARTITION_TYPE;
    }
}
