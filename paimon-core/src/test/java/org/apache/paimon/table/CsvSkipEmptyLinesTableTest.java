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

package org.apache.paimon.table;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.csv.CsvOptions;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.testutils.assertj.PaimonAssertions.anyCauseMatches;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests that {@link CsvOptions#SKIP_EMPTY_LINES} is only accepted for format tables. A file-store
 * table records the number of written rows in its file metadata, which COUNT(*) push-down reads, so
 * skipping rows while scanning would make the two disagree.
 */
class CsvSkipEmptyLinesTableTest extends TableTestBase {

    private static final String ERROR = "csv.skip-empty-lines is only supported for format tables";

    @Test
    void testRejectedWhenCreatingFileStoreTable() {
        assertThatThrownBy(
                        () ->
                                catalog.createTable(
                                        identifier("t_create"),
                                        csvTableSchema(
                                                Collections.singletonMap(
                                                        CsvOptions.SKIP_EMPTY_LINES.key(), "true")),
                                        false))
                .satisfies(anyCauseMatches(IllegalArgumentException.class, ERROR));
    }

    @Test
    void testRejectedWhenAlteringFileStoreTable() throws Exception {
        Identifier identifier = identifier("t_alter");
        catalog.createTable(identifier, csvTableSchema(Collections.emptyMap()), false);

        assertThatThrownBy(
                        () ->
                                catalog.alterTable(
                                        identifier,
                                        SchemaChange.setOption(
                                                CsvOptions.SKIP_EMPTY_LINES.key(), "true"),
                                        false))
                .satisfies(anyCauseMatches(IllegalArgumentException.class, ERROR));
    }

    @Test
    void testRejectedAsDynamicOptionOfFileStoreTable() throws Exception {
        Identifier identifier = identifier("t_dynamic");
        catalog.createTable(identifier, csvTableSchema(Collections.emptyMap()), false);
        Table table = catalog.getTable(identifier);
        // A single NULL column is written as an empty line, so this is the row that would be
        // skipped while the file metadata still counts it.
        write(table, GenericRow.of((Object) null), GenericRow.of(1));

        assertThatThrownBy(
                        () ->
                                table.copy(
                                        Collections.singletonMap(
                                                CsvOptions.SKIP_EMPTY_LINES.key(), "true")))
                .satisfies(anyCauseMatches(IllegalArgumentException.class, ERROR));

        // Without the option, the scan and the row count in the metadata agree.
        assertThat(read(table)).hasSize(2);
        long metadataRowCount = 0;
        for (Split split : table.newReadBuilder().newScan().plan().splits()) {
            metadataRowCount += split.mergedRowCount().getAsLong();
        }
        assertThat(metadataRowCount).isEqualTo(2);
    }

    @Test
    void testFormatTableSkipsEmptyLinesWithoutRowCount() throws Exception {
        java.nio.file.Path location = tempPath.resolve("format_table");
        Files.createDirectories(location);
        Files.write(location.resolve("data.csv"), "1\n\n \n2\n".getBytes(StandardCharsets.UTF_8));

        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.FILE_FORMAT.key(), "csv");
        options.put(CsvOptions.SKIP_EMPTY_LINES.key(), "true");
        FormatTable table =
                FormatTable.builder()
                        .fileIO(LocalFileIO.create())
                        .identifier(identifier("t_format"))
                        .rowType(RowType.of(DataTypes.INT()))
                        .partitionKeys(Collections.emptyList())
                        .location(location.toString())
                        .format(FormatTable.Format.CSV)
                        .options(options)
                        .build();

        TableScan.Plan plan = table.newReadBuilder().newScan().plan();
        // Format table splits carry no row count, so COUNT(*) can never be answered from
        // metadata and always scans the rows, which skips the empty lines.
        for (Split split : plan.splits()) {
            assertThat(split.mergedRowCount()).isEmpty();
        }
        List<Integer> ids = new ArrayList<>();
        try (RecordReader<InternalRow> reader =
                table.newReadBuilder().newRead().createReader(plan)) {
            reader.forEachRemaining(row -> ids.add(row.getInt(0)));
        }
        assertThat(ids).containsExactly(1, 2);
    }

    private static Schema csvTableSchema(Map<String, String> extraOptions) {
        Schema.Builder builder =
                Schema.newBuilder()
                        .column("v", DataTypes.INT())
                        .option(CoreOptions.FILE_FORMAT.key(), "csv")
                        .option(CoreOptions.FILE_COMPRESSION.key(), "gzip")
                        .option(CoreOptions.BUCKET.key(), "-1");
        extraOptions.forEach(builder::option);
        return builder.build();
    }
}
