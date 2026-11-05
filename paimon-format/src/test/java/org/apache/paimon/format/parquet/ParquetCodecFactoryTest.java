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

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FormatReaderContext;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.hadoop.util.ReflectionUtils;
import org.apache.parquet.hadoop.CodecFactory;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ParquetCodecFactory}. */
class ParquetCodecFactoryTest {

    @TempDir java.nio.file.Path tempDir;

    private final RowType rowType = RowType.of(DataTypes.INT(), DataTypes.STRING());

    /**
     * Hadoop's {@code ReflectionUtils} keeps every class it instantiates in a static constructor
     * cache. With Hadoop on a shared class loader and Paimon in a job jar, a Parquet codec class in
     * that cache keeps the job's class loader alive after the job finishes, so neither the write
     * nor the read path may put one there.
     */
    @ParameterizedTest
    @ValueSource(strings = {"zstd", "snappy", "lz4_raw"})
    void testCodecClassStaysOutOfHadoopConstructorCache(String compression) throws Exception {
        Class<?> codecClass =
                Class.forName(
                        CompressionCodecName.fromConf(compression)
                                .getHadoopCompressionCodecClassName());
        // Other tests in this JVM may have created the codec already.
        hadoopConstructorCache().remove(codecClass);
        parquetCodecCache().clear();

        Path file = new Path(tempDir.toUri().toString(), compression + ".parquet");
        FileFormat format = FileFormat.fromIdentifier("parquet", new Options());
        LocalFileIO fileIO = LocalFileIO.create();
        try (PositionOutputStream out = fileIO.newOutputStream(file, false)) {
            FormatWriter writer = format.createWriterFactory(rowType).create(out, compression);
            for (int i = 0; i < 100; i++) {
                writer.addElement(GenericRow.of(i, BinaryString.fromString("v" + i)));
            }
            writer.close();
        }
        assertThat(hadoopConstructorCache()).doesNotContainKey(codecClass);

        // Read with an empty codec cache, so the reader has to create the codec itself.
        parquetCodecCache().clear();
        List<Integer> ids = new ArrayList<>();
        try (RecordReader<InternalRow> reader =
                format.createReaderFactory(rowType, rowType, null)
                        .createReader(
                                new FormatReaderContext(
                                        fileIO, file, fileIO.getFileSize(file), null, null))) {
            reader.forEachRemaining(row -> ids.add(row.getInt(0)));
        }
        assertThat(ids).hasSize(100).startsWith(0, 1, 2).endsWith(99);
        assertThat(hadoopConstructorCache()).doesNotContainKey(codecClass);
    }

    @SuppressWarnings("unchecked")
    private static Map<Class<?>, ?> hadoopConstructorCache() throws Exception {
        Field field = ReflectionUtils.class.getDeclaredField("CONSTRUCTOR_CACHE");
        field.setAccessible(true);
        return (Map<Class<?>, ?>) field.get(null);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, ?> parquetCodecCache() throws Exception {
        Field field = CodecFactory.class.getDeclaredField("CODEC_BY_NAME");
        field.setAccessible(true);
        return (Map<String, ?>) field.get(null);
    }
}
