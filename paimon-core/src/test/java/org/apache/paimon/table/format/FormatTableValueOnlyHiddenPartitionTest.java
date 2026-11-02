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

package org.apache.paimon.table.format;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.table.FormatTable;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for writing hidden-name partitions into a value-only format table layout.
 *
 * <p>Scans skip any value-only directory whose name starts with {@code '_'} or {@code '.'} (see
 * {@link org.apache.paimon.utils.PartitionPathUtils}), so a partition written under such a value
 * used to vanish silently from every read. The write path must now fail loudly instead. The
 * configured default partition name stays writable because scans deliberately un-hide exactly it.
 */
class FormatTableValueOnlyHiddenPartitionTest {

    private static final String DEFAULT_PART_NAME = "__null__";

    private static final RowType TABLE_TYPE =
            RowType.builder()
                    .field("value", DataTypes.STRING())
                    .field("pt", DataTypes.STRING())
                    .build();

    @TempDir java.nio.file.Path tempDir;

    @ParameterizedTest
    @ValueSource(strings = {"_x", ".tmp", "__hidden"})
    void testHiddenValueOnlyPartitionFailsLoudlyAtWrite(String pt) {
        // Before the fix this write succeeded and the partition silently disappeared on read.
        assertThatThrownBy(() -> writeOneRow(pt))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(pt)
                .hasMessageContaining("value-only");
    }

    @Test
    void testVisibleValueOnlyPartitionStillWrites() throws Exception {
        List<CommitMessage> messages = writeOneRow("2025");
        assertThat(messages).isNotEmpty();
    }

    @Test
    void testDefaultPartitionNameIsWritableInValueOnlyLayout() throws Exception {
        // A null partition value is substituted with the default name, which starts with '_' but is
        // exempted because scans un-hide exactly that name.
        List<CommitMessage> messages = writeOneRow(null);
        assertThat(messages).isNotEmpty();
        assertThat(tempDir.resolve(DEFAULT_PART_NAME)).exists();
    }

    private List<CommitMessage> writeOneRow(String pt) throws Exception {
        FormatTable table = valueOnlyTable();
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = writeBuilder.newWrite()) {
            write.write(
                    GenericRow.of(
                            BinaryString.fromString("row"),
                            pt == null ? null : BinaryString.fromString(pt)));
            return write.prepareCommit();
        }
    }

    private FormatTable valueOnlyTable() {
        Path tablePath = new Path(tempDir.toUri());
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.PATH.key(), tablePath.toString());
        options.put(CoreOptions.FILE_FORMAT.key(), "csv");
        options.put(
                CoreOptions.FORMAT_TABLE_PARTITION_ONLY_VALUE_IN_PATH.key(),
                Boolean.TRUE.toString());
        options.put(CoreOptions.PARTITION_DEFAULT_NAME.key(), DEFAULT_PART_NAME);
        return FormatTable.builder()
                .fileIO(LocalFileIO.create())
                .identifier(Identifier.create("test_db", "value_only"))
                .rowType(TABLE_TYPE)
                .partitionKeys(Collections.singletonList("pt"))
                .location(tablePath.toString())
                .format(FormatTable.Format.CSV)
                .options(options)
                .build();
    }
}
