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

package org.apache.paimon.format.avro;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.utils.FileIOUtils;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.paimon.format.avro.AvroBulkFormatTestUtils.ROW_TYPE;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link AbstractAvroBulkFormat}. */
class AvroBulkFormatTest {

    private static final List<InternalRow> TEST_DATA =
            Arrays.asList(
                    // -------- batch 0, block start 232 --------
                    GenericRow.of(
                            BinaryString.fromString("AvroBulk"),
                            BinaryString.fromString("FormatTest")),
                    GenericRow.of(
                            BinaryString.fromString("Apache"), BinaryString.fromString("Paimon")),
                    GenericRow.of(
                            BinaryString.fromString(
                                    "永和九年，岁在癸丑，暮春之初，会于会稽山阴之兰亭，修禊事也。群贤毕至，少"
                                            + "长咸集。此地有崇山峻岭，茂林修竹，又有清流激湍，映带左右。引"
                                            + "以为流觞曲水，列坐其次。虽无丝竹管弦之盛，一觞一咏，亦足以畅"
                                            + "叙幽情。"),
                            BinaryString.fromString("")),
                    // -------- batch 1, block start 593 --------
                    GenericRow.of(
                            BinaryString.fromString("File"), BinaryString.fromString("Format")),
                    GenericRow.of(
                            null,
                            BinaryString.fromString(
                                    "This is a string with English, 中文 and even 🍎🍌🍑🥝🍍🥭🍐")),
                    // -------- batch 2, block start 705 --------
                    GenericRow.of(
                            BinaryString.fromString("block with"),
                            BinaryString.fromString("only one record"))
                    // -------- file length 752 --------
                    );
    private static final List<Long> BLOCK_STARTS = Arrays.asList(233L, 595L, 707L);

    private File tmpFile;

    @BeforeEach
    public void before() throws IOException {
        tmpFile = Files.createTempFile("avro-bulk-format-test", ".avro").toFile();
        tmpFile.createNewFile();
        FileOutputStream out = new FileOutputStream(tmpFile);

        Schema schema = AvroSchemaConverter.convertToSchema(ROW_TYPE);
        RowDataToAvroConverters.RowDataToAvroConverter converter =
                RowDataToAvroConverters.createConverter(ROW_TYPE);

        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
        dataFileWriter.create(schema, out);

        //  Generate the sync points manually in order to test blocks.
        long syncBlock1 = dataFileWriter.sync();
        dataFileWriter.append((GenericRecord) converter.convert(schema, TEST_DATA.get(0)));
        dataFileWriter.append((GenericRecord) converter.convert(schema, TEST_DATA.get(1)));
        dataFileWriter.append((GenericRecord) converter.convert(schema, TEST_DATA.get(2)));
        long syncBlock2 = dataFileWriter.sync();
        dataFileWriter.append((GenericRecord) converter.convert(schema, TEST_DATA.get(3)));
        dataFileWriter.append((GenericRecord) converter.convert(schema, TEST_DATA.get(4)));
        long syncBlock3 = dataFileWriter.sync();
        dataFileWriter.append((GenericRecord) converter.convert(schema, TEST_DATA.get(5)));
        long syncEnd = dataFileWriter.sync();
        dataFileWriter.close();

        // These values should be constant if nothing else changes with the file.
        assertThat(BLOCK_STARTS).isEqualTo(Arrays.asList(syncBlock1, syncBlock2, syncBlock3));
        assertThat(tmpFile).hasSize(syncEnd);
    }

    @AfterEach
    public void after() throws IOException {
        FileIOUtils.deleteFileOrDirectory(tmpFile);
    }

    @Test
    void testReadWholeFileWithOneSplit() throws IOException {
        AvroBulkFormatTestUtils.TestingAvroBulkFormat bulkFormat =
                new AvroBulkFormatTestUtils.TestingAvroBulkFormat();
        RecordReader<InternalRow> reader =
                bulkFormat.createReader(new LocalFileIO(), new Path(tmpFile.toString()));
        AtomicInteger i = new AtomicInteger(0);
        reader.forEachRemaining(
                rowData -> assertThat(rowData).isEqualTo(TEST_DATA.get(i.getAndIncrement())));
    }
}
