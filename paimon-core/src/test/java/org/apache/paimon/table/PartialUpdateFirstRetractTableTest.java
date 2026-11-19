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

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.FileSystemSchemaManager;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** Verifies first-retract aggregation through committed data and changelog files. */
class PartialUpdateFirstRetractTableTest {

    private static final int PRECISION = 18;
    private static final int SCALE = 6;

    @TempDir java.nio.file.Path tempDir;

    static Stream<Arguments> changelogAndAmount() {
        return Stream.of("none", "input", "lookup")
                .flatMap(
                        producer ->
                                Stream.of("90", "60")
                                        .map(amount -> Arguments.of(producer, amount)));
    }

    @ParameterizedTest
    @MethodSource("changelogAndAmount")
    void testRetractionAndUpdateInOneBatch(String producer, String amount) throws Exception {
        FileStoreTable table = createTable(producer);
        writeBatch(table, row("90", 1L, "+I"));
        assertThat(readRows(table, ScanMode.ALL)).containsExactly("+I|90|1|+I");

        writeBatch(table, row("90", 2L, "-U"), row(amount, 3L, "+U"));
        assertThat(readRows(table, ScanMode.ALL)).containsExactly("+I|" + amount + "|3|+U");
        if ("input".equals(producer)) {
            assertThat(readRows(table, ScanMode.CHANGELOG))
                    .containsExactlyInAnyOrder("-U|90|2|-U", "+U|" + amount + "|3|+U");
        } else if ("lookup".equals(producer)) {
            assertThat(readRows(table, ScanMode.CHANGELOG))
                    .containsExactlyInAnyOrder("-U|90|1|+I", "+U|" + amount + "|3|+U");
        } else {
            assertThat(table.newSnapshotReader().read().dataSplits())
                    .allSatisfy(
                            split ->
                                    assertThat(split.dataFiles())
                                            .allSatisfy(file -> assertThat(file.level()).isZero()));
        }
    }

    private FileStoreTable createTable(String producer) throws Exception {
        RowType rowType =
                RowType.of(
                        new DataType[] {
                            DataTypes.BIGINT().notNull(),
                            DataTypes.DECIMAL(PRECISION, SCALE),
                            DataTypes.BIGINT(),
                            DataTypes.STRING()
                        },
                        new String[] {"id", "amount", "seq", "op"});
        Options options = new Options();
        options.set("bucket", "1");
        options.set("merge-engine", "partial-update");
        options.set("fields.seq.sequence-group", "amount");
        options.set("fields.amount.aggregate-function", "sum");
        options.set("sequence.field", "seq");
        options.set("rowkind.field", "op");
        options.set("changelog-producer", producer);
        options.set("write-only", String.valueOf(!"lookup".equals(producer)));
        Path path = new Path(tempDir.toUri());
        LocalFileIO fileIO = LocalFileIO.create();
        new FileSystemSchemaManager(fileIO, path)
                .createTable(
                        new Schema(
                                rowType.getFields(),
                                Collections.emptyList(),
                                Collections.singletonList("id"),
                                options.toMap(),
                                ""));
        return FileStoreTableFactory.create(fileIO, path);
    }

    private void writeBatch(FileStoreTable table, InternalRow... rows) throws Exception {
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (IOManager ioManager = IOManager.create(tempDir.toString());
                BatchTableWrite write = builder.newWrite().withIOManager(ioManager);
                BatchTableCommit commit = builder.newCommit()) {
            for (InternalRow row : rows) {
                write.write(row);
            }
            commit.commit(write.prepareCommit());
        }
    }

    private GenericRow row(String amount, long sequence, String op) {
        return GenericRow.of(
                1L,
                Decimal.fromBigDecimal(new BigDecimal(amount), PRECISION, SCALE),
                sequence,
                BinaryString.fromString(op));
    }

    private List<String> readRows(FileStoreTable table, ScanMode mode) throws Exception {
        List<String> result = new ArrayList<>();
        try (RecordReader<InternalRow> reader =
                table.newRead()
                        .createReader(
                                new ArrayList<>(
                                        table.newSnapshotReader()
                                                .withMode(mode)
                                                .read()
                                                .dataSplits()))) {
            reader.forEachRemaining(
                    row ->
                            result.add(
                                    row.getRowKind().shortString()
                                            + "|"
                                            + row.getDecimal(1, PRECISION, SCALE)
                                                    .toBigDecimal()
                                                    .stripTrailingZeros()
                                                    .toPlainString()
                                            + "|"
                                            + row.getLong(2)
                                            + "|"
                                            + row.getString(3)));
        }
        return result;
    }
}
