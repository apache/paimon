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

package org.apache.paimon.utils;

import org.apache.paimon.fs.Path;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link CompactedChangelogPathResolver}. */
public class CompactedChangelogPathResolverTest {

    @Test
    public void testIsCompactedChangelogPath() {
        // Test non-compacted changelog file
        Path regularFile =
                new Path(
                        "/path/to/table/bucket-0/changelog-25b05ab0-6f90-4865-a984-8d9629bac735-1426.parquet");
        assertThat(CompactedChangelogPathResolver.isCompactedChangelogPath(regularFile)).isFalse();

        // Test compacted changelog file
        Path compactedFile =
                new Path(
                        "/path/to/table/bucket-0/compacted-changelog-8e049c65-5ce4-4ce7-b1b0-78ce694ab351$0-39253.cc-parquet");
        assertThat(CompactedChangelogPathResolver.isCompactedChangelogPath(compactedFile)).isTrue();

        // Test regular data file
        Path dataFile = new Path("/path/to/table/bucket-0/data-file-1.parquet");
        assertThat(CompactedChangelogPathResolver.isCompactedChangelogPath(dataFile)).isFalse();
    }

    @Test
    public void testResolveNonCompactedChangelogFile() {
        // Test regular changelog file - should return unchanged
        Path regularFile =
                new Path(
                        "/path/to/table/bucket-0/changelog-25b05ab0-6f90-4865-a984-8d9629bac735-1426.parquet");
        Path resolved = CompactedChangelogPathResolver.resolveCompactedChangelogPath(regularFile);
        assertThat(resolved).isEqualTo(regularFile);
    }

    @Test
    public void testResolveRealCompactedChangelogFile() {
        // Test real compacted changelog file - should return unchanged
        Path realFile =
                new Path(
                        "/path/to/table/bucket-0/compacted-changelog-8e049c65-5ce4-4ce7-b1b0-78ce694ab351$0-39253.cc-parquet");
        Path resolved = CompactedChangelogPathResolver.resolveCompactedChangelogPath(realFile);
        assertThat(resolved).isEqualTo(realFile);
    }

    @Test
    public void testResolveFakeCompactedChangelogFile() {
        // Test fake compacted changelog file - should resolve to real path
        Path fakeFile =
                new Path(
                        "/path/to/table/bucket-1/compacted-changelog-8e049c65-5ce4-4ce7-b1b0-78ce694ab351$0-39253-39253-35699.cc-parquet");
        Path resolved = CompactedChangelogPathResolver.resolveCompactedChangelogPath(fakeFile);

        Path expectedRealFile =
                new Path(
                        "/path/to/table/bucket-0/compacted-changelog-8e049c65-5ce4-4ce7-b1b0-78ce694ab351$0-39253.cc-parquet");
        assertThat(resolved).isEqualTo(expectedRealFile);
    }

    @Test
    public void testResolveWithDifferentFormats() {
        // Test with different file formats
        Path fakeOrcFile =
                new Path(
                        "/path/to/table/bucket-2/compacted-changelog-8e049c65-5ce4-4ce7-b1b0-78ce694ab351$0-1024-1024-512.cc-orc");
        Path resolvedOrc =
                CompactedChangelogPathResolver.resolveCompactedChangelogPath(fakeOrcFile);
        Path expectedOrcFile =
                new Path(
                        "/path/to/table/bucket-0/compacted-changelog-8e049c65-5ce4-4ce7-b1b0-78ce694ab351$0-1024.cc-orc");
        assertThat(resolvedOrc).isEqualTo(expectedOrcFile);

        Path fakeAvroFile =
                new Path(
                        "/path/to/table/bucket-5/compacted-changelog-8e049c65-5ce4-4ce7-b1b0-78ce694ab351$2-2048-2048-1024.cc-avro");
        Path resolvedAvro =
                CompactedChangelogPathResolver.resolveCompactedChangelogPath(fakeAvroFile);
        Path expectedAvroFile =
                new Path(
                        "/path/to/table/bucket-2/compacted-changelog-8e049c65-5ce4-4ce7-b1b0-78ce694ab351$2-2048.cc-avro");
        assertThat(resolvedAvro).isEqualTo(expectedAvroFile);
    }

    @Test
    public void testResolveFileWithoutExtension() {
        // Test file without file extension - should return unchanged
        Path fileWithoutExt = new Path("/path/to/table/file");
        Path resolved =
                CompactedChangelogPathResolver.resolveCompactedChangelogPath(fileWithoutExt);
        assertThat(resolved).isEqualTo(fileWithoutExt);
    }
}
