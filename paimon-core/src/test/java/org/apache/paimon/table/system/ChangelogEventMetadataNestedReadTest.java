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

package org.apache.paimon.table.system;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.InnerTableRead;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.StreamTableScan;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests reading nested-pruned changelog event metadata. */
class ChangelogEventMetadataNestedReadTest extends TableTestBase {

    @ParameterizedTest(name = "file.format = {0}")
    @ValueSource(strings = {"parquet", "avro"})
    void testPrunedMetadataWithoutSourceField(String format) throws Exception {
        createAndWrite(format);
        assertBatchReads("id", "__internal__payload.b").containsExactly("+I[1, [44]]");
    }

    @ParameterizedTest(name = "file.format = {0}")
    @ValueSource(strings = {"parquet", "avro"})
    void testMetadataAndSourceFieldRequestDifferentChildren(String format) throws Exception {
        createAndWrite(format);
        assertBatchReads("id", "payload.a", "__internal__payload.b")
                .containsExactly("+I[1, [33], [44]]");
    }

    @ParameterizedTest(name = "file.format = {0}")
    @ValueSource(strings = {"parquet", "avro", "orc"})
    void testFullMetadataWithPrunedSourceField(String format) throws Exception {
        createAndWrite(format);
        assertBatchReads("id", "payload.a", "__internal__payload")
                .containsExactly("+I[1, [33], [33, 44]]");
    }

    @ParameterizedTest(name = "file.format = {0}")
    @ValueSource(strings = {"parquet", "avro"})
    void testPrunedMetadataWithFullSourceField(String format) throws Exception {
        createAndWrite(format);
        assertBatchReads("id", "payload", "__internal__payload.b")
                .containsExactly("+I[1, [33, 44], [44]]");
    }

    @ParameterizedTest(name = "file.format = {0}")
    @ValueSource(strings = {"parquet", "avro"})
    void testPrunedMetadataFromChangelogFiles(String format) throws Exception {
        createAndWrite(format);
        ChangelogEventMetadataTable table = new ChangelogEventMetadataTable(getTableDefault());
        RowType readType =
                table.rowType().projectByPaths(Arrays.asList("id", "__internal__payload.b"));

        ReadBuilder readBuilder = table.newReadBuilder().withReadType(readType);
        StreamTableScan scan = readBuilder.newStreamScan();
        scan.restore(1L);
        List<String> result = new ArrayList<>();
        long latestSnapshotId = table.snapshotManager().latestSnapshotId();
        for (int i = 0;
                i < 2 * latestSnapshotId
                        && (scan.checkpoint() == null || scan.checkpoint() <= latestSnapshotId);
                i++) {
            result.addAll(read(readBuilder.newRead(), scan.plan(), readType));
        }

        // -U carries the physical before-image but the metadata of the updating event.
        assertThat(result).containsExactly("+I[1, [22]]", "-U[1, [44]]", "+U[1, [44]]");
    }

    @ParameterizedTest(name = "file.format = {0}")
    @ValueSource(strings = {"parquet", "avro"})
    void testPrunedMetadataInIncrementalDiffRead(String format) throws Exception {
        createAndWrite(format);
        FileStoreTable table = getTableDefault();
        Map<String, String> options = new HashMap<>();
        options.put(
                CoreOptions.INCREMENTAL_BETWEEN.key(),
                "1," + table.snapshotManager().latestSnapshotId());
        options.put(CoreOptions.INCREMENTAL_BETWEEN_SCAN_MODE.key(), "diff");
        ChangelogEventMetadataTable diffTable =
                new ChangelogEventMetadataTable(table.copy(options));

        // The diff read merges each snapshot through the merge reader and compares the results,
        // so metadata dependencies must be resolved there as well.
        RowType readType =
                diffTable
                        .rowType()
                        .projectByPaths(Arrays.asList("id", "payload.a", "__internal__payload.b"));
        TableScan.Plan plan = diffTable.newReadBuilder().newScan().plan();
        assertThat(read(diffTable.newRead().withReadType(readType), plan, readType))
                .containsExactly("+I[1, [33], [44]]");
    }

    private void createAndWrite(String format) throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT().notNull())
                        .column(
                                "payload",
                                DataTypes.ROW(
                                        DataTypes.FIELD(2, "a", DataTypes.INT()),
                                        DataTypes.FIELD(3, "b", DataTypes.INT())))
                        .primaryKey("id")
                        .option(CoreOptions.BUCKET.key(), "1")
                        .option(CoreOptions.FILE_FORMAT.key(), format)
                        .option(CoreOptions.CHANGELOG_PRODUCER.key(), "lookup")
                        .option(
                                CoreOptions.CHANGELOG_PRODUCER_EVENT_METADATA_FIELDS.key(),
                                "payload")
                        .build();
        catalog.createTable(identifier(), schema, false);

        FileStoreTable table = getTableDefault();
        try (StreamTableWrite write = table.newWrite(commitUser).withIOManager(ioManager);
                StreamTableCommit commit = table.newCommit(commitUser)) {
            write.write(GenericRow.of(1, GenericRow.of(11, 22)));
            commit.commit(0, write.prepareCommit(true, 0));
            write.write(GenericRow.of(1, GenericRow.of(33, 44)));
            commit.commit(1, write.prepareCommit(true, 1));
            // Compact into a single file so batch splits can also be read raw.
            write.compact(BinaryRow.EMPTY_ROW, 0, true);
            commit.commit(2, write.prepareCommit(true, 2));
        }
    }

    /** Reads with the raw reader and, via {@code forceKeepDelete}, with the merge reader. */
    private org.assertj.core.api.ListAssert<String> assertBatchReads(String... paths)
            throws Exception {
        ChangelogEventMetadataTable table = new ChangelogEventMetadataTable(getTableDefault());
        RowType readType = table.rowType().projectByPaths(Arrays.asList(paths));
        TableScan.Plan plan = table.newReadBuilder().newScan().plan();
        // Guard against silently falling back to the merge read.
        assertThat(plan.splits())
                .isNotEmpty()
                .allSatisfy(split -> assertThat(((DataSplit) split).rawConvertible()).isTrue());

        InnerTableRead rawRead = table.newRead().withReadType(readType);
        List<String> raw = read(rawRead, plan, readType);

        InnerTableRead mergeRead = table.newRead().withReadType(readType).forceKeepDelete();
        List<String> merged = read(mergeRead, plan, readType);

        assertThat(merged).isEqualTo(raw);
        return assertThat(raw);
    }

    private static List<String> read(TableRead read, TableScan.Plan plan, RowType rowType)
            throws Exception {
        List<String> result = new ArrayList<>();
        read.createReader(plan)
                .forEachRemaining(
                        row -> result.add(row.getRowKind().shortString() + format(row, rowType)));
        return result;
    }

    private static String format(InternalRow row, RowType rowType) {
        List<String> values = new ArrayList<>();
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            if (row.isNullAt(i)) {
                values.add("null");
            } else if (rowType.getTypeAt(i) instanceof RowType) {
                RowType nestedType = (RowType) rowType.getTypeAt(i);
                values.add(format(row.getRow(i, nestedType.getFieldCount()), nestedType));
            } else {
                values.add(String.valueOf(row.getInt(i)));
            }
        }
        return "[" + String.join(", ", values) + "]";
    }
}
