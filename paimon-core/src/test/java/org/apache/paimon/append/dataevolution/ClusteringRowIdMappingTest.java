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

package org.apache.paimon.append.dataevolution;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests the output-file ordinal mapping and rejects incomplete or corrupt side files. */
class ClusteringRowIdMappingTest {

    private static final int MAGIC = 0x43524944;
    private final FileIO fileIO = LocalFileIO.create();

    @TempDir java.nio.file.Path tempPath;

    @Test
    void testRoundTripKeepsPhysicalOrder() throws Exception {
        Path path = path("round-trip");
        try (ClusteringRowIdMapping.Writer writer =
                ClusteringRowIdMapping.createWriter(fileIO, path)) {
            writer.write(10);
            writer.write(0);
            writer.write(Long.MAX_VALUE);
        }
        List<Long> ordinals = new ArrayList<>();
        List<Long> oldRowIds = new ArrayList<>();
        ClusteringRowIdMapping.read(
                fileIO,
                path,
                3,
                (ordinal, oldRowId) -> {
                    ordinals.add(ordinal);
                    oldRowIds.add(oldRowId);
                });
        assertThat(ordinals).containsExactly(0L, 1L, 2L);
        assertThat(oldRowIds).containsExactly(10L, 0L, Long.MAX_VALUE);
    }

    @Test
    void testRejectsTruncatedAndTrailingRows() throws Exception {
        Path path = raw("row-count", MAGIC, 1, 10L);
        assertThatThrownBy(
                        () -> ClusteringRowIdMapping.read(fileIO, path, 2, (ordinal, oldId) -> {}))
                .isInstanceOf(EOFException.class);
        assertThatThrownBy(
                        () -> ClusteringRowIdMapping.read(fileIO, path, 0, (ordinal, oldId) -> {}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unexpected trailing clustering row IDs");
    }

    @Test
    void testRejectsInvalidHeaderAndOldRowIds() throws Exception {
        Path magic = raw("magic", 0, 1);
        assertThatThrownBy(
                        () -> ClusteringRowIdMapping.read(fileIO, magic, 0, (ordinal, oldId) -> {}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid clustering row ID mapping");
        Path version = raw("version", MAGIC, 2);
        assertThatThrownBy(
                        () ->
                                ClusteringRowIdMapping.read(
                                        fileIO, version, 0, (ordinal, oldId) -> {}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unsupported clustering row ID mapping");
        Path negative = raw("negative", MAGIC, 1, -1);
        assertThatThrownBy(
                        () ->
                                ClusteringRowIdMapping.read(
                                        fileIO, negative, 1, (ordinal, oldId) -> {}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid old row ID");
        assertThatThrownBy(
                        () ->
                                ClusteringRowIdMapping.read(
                                        fileIO, negative, -1, (ordinal, oldId) -> {}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid clustering row count");
    }

    @Test
    void testWriterRejectsNegativeRowIds() throws Exception {
        Path path = path("invalid-write");
        try (ClusteringRowIdMapping.Writer writer =
                ClusteringRowIdMapping.createWriter(fileIO, path)) {
            assertThatThrownBy(() -> writer.write(-1))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Invalid old row ID");
            writer.write(3);
        }
        List<Long> rows = new ArrayList<>();
        ClusteringRowIdMapping.read(fileIO, path, 1, (ordinal, oldId) -> rows.add(oldId));
        assertThat(rows).containsExactly(3L);
    }

    private Path raw(String name, int magic, int version, long... rows) throws IOException {
        Path path = path(name);
        try (DataOutputStream out = new DataOutputStream(fileIO.newOutputStream(path, false))) {
            out.writeInt(magic);
            out.writeInt(version);
            for (long row : rows) {
                out.writeLong(row);
            }
        }
        return path;
    }

    private Path path(String name) {
        return new Path(tempPath.resolve(name).toUri());
    }
}
