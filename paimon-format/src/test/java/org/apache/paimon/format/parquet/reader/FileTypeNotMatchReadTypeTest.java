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

package org.apache.paimon.format.parquet.reader;

import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.format.FormatReaderContext;
import org.apache.paimon.format.parquet.ParquetReaderFactory;
import org.apache.paimon.format.parquet.writer.ParquetRowDataBuilder;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.io.LocalOutputFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/** Test field type not match correctly with read type. */
public class FileTypeNotMatchReadTypeTest {

    private static final Random RANDOM = new Random();
    @TempDir private Path tempDir;

    @Test
    public void testTimestamp() throws Exception {
        String fileName = "test.parquet";
        String fileWholePath = tempDir + "/" + fileName;
        for (int i = 0; i < 100; i++) {
            int writePrecision = RANDOM.nextInt(10);
            int readPrecision = writePrecision == 0 ? 0 : RANDOM.nextInt(writePrecision);

            if (readPrecision <= 3 && writePrecision > 3) {
                readPrecision = 4;
            }

            RowType rowTypeWrite = RowType.of(DataTypes.TIMESTAMP(writePrecision));
            RowType rowTypeRead = RowType.of(DataTypes.TIMESTAMP(readPrecision));

            ParquetRowDataBuilder parquetRowDataBuilder =
                    new ParquetRowDataBuilder(
                            new LocalOutputFile(new File(fileWholePath).toPath()), rowTypeWrite);

            ParquetWriter<InternalRow> parquetWriter = parquetRowDataBuilder.build();
            Timestamp timestamp = Timestamp.now();
            parquetWriter.write(GenericRow.of(timestamp));
            parquetWriter.write(GenericRow.of(Timestamp.now()));
            parquetWriter.close();

            ParquetReaderFactory parquetReaderFactory =
                    new ParquetReaderFactory(new Options(), rowTypeRead, 100, null);

            File file = new File(fileWholePath);
            FileRecordReader<InternalRow> fileRecordReader =
                    parquetReaderFactory.createReader(
                            new FormatReaderContext(
                                    LocalFileIO.create(),
                                    new org.apache.paimon.fs.Path(tempDir.toString(), fileName),
                                    file.length()));

            InternalRow row = fileRecordReader.readBatch().next();
            Timestamp getTimestamp = row.getTimestamp(0, readPrecision);
            assertThat(timestamp.getMillisecond()).isEqualTo(getTimestamp.getMillisecond());
            file.delete();
        }
    }

    @Test
    public void testDecimal() throws Exception {
        String fileName = "test.parquet";
        String fileWholePath = tempDir + "/" + fileName;
        for (int i = 0; i < 100; i++) {
            int writePrecision = 1 + RANDOM.nextInt(30);
            int readPrecision = 1 + RANDOM.nextInt(writePrecision);

            RowType rowTypeWrite = RowType.of(DataTypes.DECIMAL(writePrecision, 0));
            RowType rowTypeRead = RowType.of(DataTypes.DECIMAL(readPrecision, 0));

            ParquetRowDataBuilder parquetRowDataBuilder =
                    new ParquetRowDataBuilder(
                            new LocalOutputFile(new File(fileWholePath).toPath()), rowTypeWrite);

            ParquetWriter<InternalRow> parquetWriter = parquetRowDataBuilder.build();
            Decimal decimal = Decimal.fromBigDecimal(
                    new java.math.BigDecimal(1.0), writePrecision, 0);
            parquetWriter.write(
                    GenericRow.of(decimal));
            parquetWriter.write(
                    GenericRow.of(
                            Decimal.fromBigDecimal(
                                    new java.math.BigDecimal(2.0), writePrecision, 0)));
            parquetWriter.close();

            ParquetReaderFactory parquetReaderFactory =
                    new ParquetReaderFactory(new Options(), rowTypeRead, 100, null);

            File file = new File(fileWholePath);
            FileRecordReader<InternalRow> fileRecordReader =
                    parquetReaderFactory.createReader(
                            new FormatReaderContext(
                                    LocalFileIO.create(),
                                    new org.apache.paimon.fs.Path(tempDir.toString(), fileName),
                                    file.length()));

            InternalRow row = fileRecordReader.readBatch().next();
            Decimal getDecimal = row.getDecimal(0, readPrecision, 0);
            assertThat(decimal.toUnscaledLong()).isEqualTo(getDecimal.toUnscaledLong());
            file.delete();
        }
    }
}
