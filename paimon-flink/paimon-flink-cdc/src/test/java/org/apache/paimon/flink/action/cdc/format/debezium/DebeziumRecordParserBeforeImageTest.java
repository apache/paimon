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

package org.apache.paimon.flink.action.cdc.format.debezium;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.flink.action.cdc.CdcMetadataConverter;
import org.apache.paimon.flink.action.cdc.CdcSourceRecord;
import org.apache.paimon.flink.action.cdc.TypeMapping;
import org.apache.paimon.flink.action.cdc.mysql.MySqlRecordParser;
import org.apache.paimon.flink.action.cdc.postgres.PostgresRecordParser;
import org.apache.paimon.flink.sink.cdc.CdcRecord;
import org.apache.paimon.flink.sink.cdc.CdcRecordUtils;
import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecord;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.schema.FileSystemSchemaManager;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.utils.CloseableIterator;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.util.ListCollector;
import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/** Before-image handling in the native MySQL and PostgreSQL Debezium parsers. */
class DebeziumRecordParserBeforeImageTest {

    @TempDir private java.nio.file.Path temporaryDirectory;

    @ParameterizedTest
    @CsvSource({"mysql, null", "mysql, {}", "postgres, null", "postgres, {}"})
    void testUpdateWithoutBeforeImage(String format, String beforeImage) throws Exception {
        // The empty-object case is synthetic input supported by the parsers before null fields
        // were populated for expression lookup. It must not fabricate a delete with a null key.
        List<CdcRecord> records = parseUpdate(format, beforeImage);
        FileStoreTable table = createPrimaryKeyTable();

        assertThatCode(() -> writeRecords(table, records))
                .as("An update without a before image must remain writable to a primary-key table")
                .doesNotThrowAnyException();
        assertThat(records).extracting(CdcRecord::kind).containsExactly(RowKind.INSERT);
        assertThat(records.get(0).data()).containsEntry("id", "1").containsEntry("value", "new");

        ReadBuilder read = table.newReadBuilder();
        try (CloseableIterator<InternalRow> rows =
                read.newRead().createReader(read.newScan().plan()).toCloseableIterator()) {
            assertThat(rows.hasNext()).isTrue();
            InternalRow row = rows.next();
            assertThat(row.getInt(0)).isEqualTo(1);
            assertThat(row.getString(1).toString()).isEqualTo("new");
            assertThat(rows.hasNext()).isFalse();
        }
    }

    @ParameterizedTest
    @CsvSource({"mysql, false", "mysql, true", "postgres, false", "postgres, true"})
    void testNonEmptyBeforeImageRetainsDelete(String format, boolean fullBeforeImage)
            throws Exception {
        String beforeImage = fullBeforeImage ? "{\"id\":1,\"value\":\"old\"}" : "{\"id\":1}";
        List<CdcRecord> records = parseUpdate(format, beforeImage);

        assertThat(records)
                .extracting(CdcRecord::kind)
                .containsExactly(RowKind.DELETE, RowKind.INSERT);
        assertThat(records.get(0).data()).containsEntry("id", "1");
        if (fullBeforeImage) {
            assertThat(records.get(0).data()).containsEntry("value", "old");
        }
        assertThat(records.get(1).data()).containsEntry("id", "1").containsEntry("value", "new");
    }

    private FileStoreTable createPrimaryKeyTable() throws Exception {
        Path path = new Path(temporaryDirectory.resolve("table").toUri());
        LocalFileIO fileIO = LocalFileIO.create();
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("value", DataTypes.STRING())
                        .primaryKey("id")
                        .option("bucket", "1")
                        .build();
        new FileSystemSchemaManager(fileIO, path).createTable(schema);
        return FileStoreTableFactory.create(fileIO, path);
    }

    private static void writeRecords(FileStoreTable table, List<CdcRecord> records)
            throws Exception {
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite();
                BatchTableCommit commit = builder.newCommit()) {
            for (CdcRecord record : records) {
                write.write(
                        CdcRecordUtils.toGenericRow(record, table.schema().fields(), true).get());
            }
            commit.commit(write.prepareCommit());
        }
    }

    private static List<CdcRecord> parseUpdate(String format, String beforeImage) throws Exception {
        FlatMapFunction<CdcSourceRecord, RichCdcMultiplexRecord> parser =
                format.equals("mysql")
                        ? new MySqlRecordParser(
                                new Configuration(),
                                Collections.emptyList(),
                                TypeMapping.defaultMapping(),
                                new CdcMetadataConverter[0])
                        : new PostgresRecordParser(
                                new Configuration(),
                                Collections.emptyList(),
                                TypeMapping.defaultMapping(),
                                new CdcMetadataConverter[0]);
        String rowFields =
                "\"fields\":[{\"field\":\"id\",\"type\":\"int32\",\"optional\":false},"
                        + "{\"field\":\"value\",\"type\":\"string\",\"optional\":true}]";
        String event =
                "{\"schema\":{\"type\":\"struct\",\"fields\":["
                        + "{\"field\":\"before\",\"type\":\"struct\",\"optional\":true,"
                        + rowFields
                        + "},{\"field\":\"after\",\"type\":\"struct\",\"optional\":true,"
                        + rowFields
                        + "}]},\"payload\":{\"op\":\"u\","
                        + "\"source\":{\"db\":\"test\",\"table\":\"records\"},\"before\":"
                        + beforeImage
                        + ",\"after\":{\"id\":1,\"value\":\"new\"}}}";
        List<RichCdcMultiplexRecord> records = new ArrayList<>();
        parser.flatMap(new CdcSourceRecord(event), new ListCollector<>(records));
        return records.stream()
                .map(record -> record.toRichCdcRecord().toCdcRecord())
                .collect(Collectors.toList());
    }
}
