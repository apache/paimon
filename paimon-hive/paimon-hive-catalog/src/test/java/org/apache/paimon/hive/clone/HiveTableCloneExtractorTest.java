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

package org.apache.paimon.hive.clone;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;

/** Tests for {@link HiveTableCloneExtractor}. */
class HiveTableCloneExtractorTest {

    /** U+0130, whose lower case under {@link Locale#ROOT} is the two characters below. */
    private static final String DOTTED_CAPITAL_I = "İ";

    private static final String LOWER_CASE_DOTTED_I = "i̇";

    @Test
    void prefixIsMatchedCaseInsensitively() {
        Map<String, String> options = new HashMap<>();
        options.put("ORC.compression", "zstd");
        options.put("avro.codec", "snappy");

        assertThat(HiveTableCloneExtractor.getIdentifierPrefixOptions("orc", options))
                .containsExactly(entry("orc.compression", "zstd"));
    }

    @Test
    void anIdentifierThatLowerCasesLongerKeepsItsOptions() {
        assertThat(DOTTED_CAPITAL_I.toLowerCase(Locale.ROOT)).isEqualTo(LOWER_CASE_DOTTED_I);

        Map<String, String> options = new HashMap<>();
        options.put(DOTTED_CAPITAL_I + ".compression", "zstd");

        // slicing the key at the length of the lower-cased prefix would drop the "c"
        assertThat(HiveTableCloneExtractor.getIdentifierPrefixOptions(DOTTED_CAPITAL_I, options))
                .containsExactly(entry(LOWER_CASE_DOTTED_I + ".compression", "zstd"));
    }

    @Test
    void aKeyThatIsOnlyThePrefixYieldsAnEmptySuffix() {
        Map<String, String> options = new HashMap<>();
        options.put(DOTTED_CAPITAL_I + ".", "zstd");

        // this key is shorter than the lower-cased prefix, so slicing at its length overruns it
        assertThat(HiveTableCloneExtractor.getIdentifierPrefixOptions(DOTTED_CAPITAL_I, options))
                .containsExactly(entry(LOWER_CASE_DOTTED_I + ".", "zstd"));
    }

    @Test
    void recursiveListingSkipsHiddenFilesAndDirectories(@TempDir java.nio.file.Path tmp)
            throws IOException {
        // hidden directories above the partition root, such as a hidden warehouse, hide nothing
        java.nio.file.Path root = tmp.resolve(".warehouse/_db/t");
        String[] files = {
            "a.parquet",
            "_SUCCESS",
            ".a.parquet.crc",
            "sub/b.parquet",
            "sub/deep/c.parquet",
            "_temporary/0/d.parquet",
            ".hive-staging_1/e.parquet",
            "sub/_tmp/f.parquet",
            "sub/deep/.stage/g.parquet",
        };
        for (String file : files) {
            java.nio.file.Path path = root.resolve(file);
            Files.createDirectories(path.getParent());
            Files.createFile(path);
        }

        assertThat(recursivelyListedFiles(root.toUri().toString()))
                .containsExactlyInAnyOrder("a.parquet", "sub/b.parquet", "sub/deep/c.parquet");
    }

    @Test
    void aFileListedByTwoPartitionsIsRejected() {
        Identifier identifier = Identifier.create("db", "t");
        Path outer = new Path("s3://bucket/t/dt=1/part-0.parquet");
        Path inner = new Path("s3://bucket/t/dt=1/dt=2/part-0.parquet");
        HivePartitionFiles first =
                new HivePartitionFiles(
                        BinaryRow.EMPTY_ROW,
                        Arrays.asList(outer, inner),
                        Arrays.asList(1L, 1L),
                        "parquet");
        HivePartitionFiles second =
                new HivePartitionFiles(
                        BinaryRow.EMPTY_ROW,
                        Collections.singletonList(inner),
                        Collections.singletonList(1L),
                        "parquet");

        HiveTableCloneExtractor.checkNoSharedFiles(identifier, Collections.singletonList(first));
        assertThatThrownBy(
                        () ->
                                HiveTableCloneExtractor.checkNoSharedFiles(
                                        identifier, Arrays.asList(first, second)))
                .hasMessageContaining(inner.toString());
    }

    private static List<String> recursivelyListedFiles(String location) throws IOException {
        String root = new Path(location).toUri().getPath();
        return HiveTableCloneExtractor.listFiles(
                        LocalFileIO.create(), location, BinaryRow.EMPTY_ROW, "parquet", true)
                .paths().stream()
                .map(p -> p.toUri().getPath().substring(root.length()).replaceAll("^/", ""))
                .collect(Collectors.toList());
    }
}
