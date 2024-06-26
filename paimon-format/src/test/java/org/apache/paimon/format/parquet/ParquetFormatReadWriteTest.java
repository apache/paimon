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

package org.apache.paimon.format.parquet;

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FileFormatFactory.FormatContext;
import org.apache.paimon.format.FormatReadWriteTest;
import org.apache.paimon.format.FormatReaderContext;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThat;

/** A parquet {@link FormatReadWriteTest}. */
public class ParquetFormatReadWriteTest extends FormatReadWriteTest {

    protected ParquetFormatReadWriteTest() {
        super("parquet");
    }

    @Override
    protected boolean supportNestedNested() {
        return false;
    }

    @Override
    protected FileFormat fileFormat(FormatContext context) {
        return new ParquetFileFormat(context);
    }

    @Test
    public void testColumnIndexFilterPushDown() throws IOException {
        Options options = new Options();
        options.set("page.size", "64");
        FormatContext context = new FormatContext(options, 1024);
        FileFormat format = fileFormat(context);
        RowType rowType = DataTypes.ROW(DataTypes.INT().notNull(), DataTypes.BIGINT());
        InternalRowSerializer serializer = new InternalRowSerializer(rowType);
        PositionOutputStream out = fileIO.newOutputStream(file, false);
        FormatWriter writer = format.createWriterFactory(rowType).create(out, "zstd");
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        int batchSize = 10_000;
        for (int i = 0; i < batchSize; i++) {
            int value = rnd.nextInt(100);
            writer.addElement(GenericRow.of(value, (long) value));
        }
        for (int i = 0; i < batchSize; i++) {
            int value = rnd.nextInt(100, 500);
            writer.addElement(GenericRow.of(value, (long) value));
        }
        writer.flush();
        writer.finish();
        out.close();

        PredicateBuilder predicateBuilder = new PredicateBuilder(rowType);
        List<Predicate> filters = Collections.singletonList(predicateBuilder.equal(0, 2));
        RecordReader<InternalRow> reader =
                format.createReaderFactory(rowType, filters)
                        .createReader(
                                new FormatReaderContext(fileIO, file, fileIO.getFileSize(file)));
        List<InternalRow> result = new ArrayList<>();
        reader.forEachRemaining(row -> result.add(serializer.copy(row)));

        assertThat(result.size()).isLessThan(batchSize * 2);
    }
}
