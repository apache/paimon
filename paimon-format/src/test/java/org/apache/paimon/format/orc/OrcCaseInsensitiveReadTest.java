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

package org.apache.paimon.format.orc;

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FormatReaderContext;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcConf;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** Test case-insensitive column matching in {@link OrcReaderFactory}. */
class OrcCaseInsensitiveReadTest {

    @TempDir File folder;

    @Test
    void testCaseInsensitiveColumnMatching() throws Exception {
        Path path = writeMixedCaseOrc();

        // Read with lowercase column names + caseSensitive=false.
        RowType readType =
                RowType.builder()
                        .field("event_name", DataTypes.STRING())
                        .field("campaign_id", DataTypes.STRING())
                        .field("amount", DataTypes.BIGINT())
                        .build();

        Configuration conf = new Configuration(false);
        OrcConf.IS_SCHEMA_EVOLUTION_CASE_SENSITIVE.setBoolean(conf, false);

        OrcReaderFactory factory =
                new OrcReaderFactory(conf, readType, Collections.emptyList(), 1024, false, false);

        LocalFileIO fileIO = new LocalFileIO();
        List<InternalRow> rows = new ArrayList<>();
        try (RecordReader<InternalRow> reader =
                factory.createReader(
                        new FormatReaderContext(fileIO, path, fileIO.getFileSize(path)))) {
            reader.forEachRemaining(
                    row ->
                            rows.add(
                                    GenericRow.of(
                                            row.isNullAt(0) ? null : row.getString(0).copy(),
                                            row.isNullAt(1) ? null : row.getString(1).copy(),
                                            row.isNullAt(2) ? null : (Object) row.getLong(2))));
        }

        assertThat(rows).hasSize(3);
        assertThat(rows.get(0).getString(0).toString()).isEqualTo("install");
        assertThat(rows.get(0).getString(1).toString()).isEqualTo("c001");
        assertThat(rows.get(0).getLong(2)).isEqualTo(100L);
    }

    private Path writeMixedCaseOrc() throws Exception {
        Path path = new Path(folder.getPath(), UUID.randomUUID().toString());
        TypeDescription schema =
                TypeDescription.fromString(
                        "struct<Event_Name:string,Campaign_ID:string,Amount:bigint>");
        Configuration conf = new Configuration(false);

        try (Writer writer =
                OrcFile.createWriter(
                        new org.apache.hadoop.fs.Path(path.toString()),
                        OrcFile.writerOptions(conf).setSchema(schema))) {
            VectorizedRowBatch batch = schema.createRowBatch();
            BytesColumnVector eventName = (BytesColumnVector) batch.cols[0];
            BytesColumnVector campaignId = (BytesColumnVector) batch.cols[1];
            LongColumnVector amount = (LongColumnVector) batch.cols[2];

            String[][] data = {{"install", "c001"}, {"purchase", "c002"}, {"login", "c001"}};
            long[] amounts = {100L, 200L, 300L};

            for (int i = 0; i < 3; i++) {
                int row = batch.size++;
                byte[] eventBytes = data[i][0].getBytes(StandardCharsets.UTF_8);
                byte[] cidBytes = data[i][1].getBytes(StandardCharsets.UTF_8);
                eventName.setVal(row, eventBytes);
                campaignId.setVal(row, cidBytes);
                amount.vector[row] = amounts[i];
            }
            writer.addRowBatch(batch);
        }
        return path;
    }
}
