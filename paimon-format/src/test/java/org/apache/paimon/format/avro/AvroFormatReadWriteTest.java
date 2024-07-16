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

import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FileFormatFactory;
import org.apache.paimon.format.FormatReadWriteTest;
import org.apache.paimon.format.FormatReaderContext;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** An avro {@link FormatReadWriteTest}. */
public class AvroFormatReadWriteTest extends FormatReadWriteTest {

    protected AvroFormatReadWriteTest() {
        super("avro");
    }

    @Override
    protected FileFormat fileFormat() {
        return new AvroFileFormat(new FileFormatFactory.FormatContext(new Options(), 1024));
    }

    @Test
    public void testMapTypeNonStringKey() throws Exception {
        RowType rowType =
                RowType.of(
                        DataTypes.MAP(DataTypes.INT(), DataTypes.BIGINT()),
                        DataTypes.MAP(DataTypes.BIGINT(), DataTypes.BOOLEAN()));

        Map<Integer, Long> map1 = new HashMap<>();
        map1.put(1, 10L);
        map1.put(2, 20L);
        Map<Long, Boolean> map2 = new HashMap<>();
        map2.put(100L, true);
        map2.put(200L, false);
        InternalRow expected = GenericRow.of(new GenericMap(map1), new GenericMap(map2));

        FileFormat format = fileFormat();
        PositionOutputStream out = fileIO.newOutputStream(file, false);
        FormatWriter writer = format.createWriterFactory(rowType).create(out, "zstd");
        writer.addElement(expected);
        writer.close();
        out.close();

        RecordReader<InternalRow> reader =
                format.createReaderFactory(rowType)
                        .createReader(
                                new FormatReaderContext(fileIO, file, fileIO.getFileSize(file)));
        List<InternalRow> result = new ArrayList<>();
        reader.forEachRemaining(result::add);
        assertThat(result.size()).isEqualTo(1);
        InternalRowSerializer serializer = new InternalRowSerializer(rowType);
        assertThat(serializer.toBinaryRow(result.get(0)).copy())
                .isEqualTo(serializer.toBinaryRow(expected).copy());
    }
}
