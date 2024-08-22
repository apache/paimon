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

package org.apache.paimon.format.orc.writer;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.format.FileFormatFactory;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.format.orc.OrcFileFormat;
import org.apache.paimon.format.orc.OrcFileFormatFactory;
import org.apache.paimon.format.orc.OrcWriterFactory;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import com.github.luben.zstd.ZstdException;
import org.apache.hadoop.conf.Configuration;
import org.apache.orc.CompressionCodec;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.impl.ZstdCodec;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.UUID;

import static org.apache.paimon.format.orc.OrcFileFormatFactory.IDENTIFIER;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

class OrcZstdTest {

    @Test
    void testWriteOrcWithZstd(@TempDir java.nio.file.Path tempDir) throws IOException {
        Options options = new Options();
        options.set("compress", "zstd");
        options.set("stripe.size", "31457280");
        options.set("compression.zstd.level", "1");
        OrcFileFormat orc =
                new OrcFileFormatFactory()
                        .create(new FileFormatFactory.FormatContext(options, 1024));
        Assertions.assertThat(orc).isInstanceOf(OrcFileFormat.class);

        Assertions.assertThat(orc.orcProperties().getProperty(IDENTIFIER + ".compress", ""))
                .isEqualTo("zstd");
        Assertions.assertThat(
                        orc.orcProperties().getProperty(IDENTIFIER + ".compression.zstd.level", ""))
                .isEqualTo("1");
        Assertions.assertThat(orc.orcProperties().getProperty(IDENTIFIER + ".stripe.size", ""))
                .isEqualTo("31457280");

        RowType rowType =
                RowType.builder()
                        .field("a", DataTypes.INT())
                        .field("b", DataTypes.STRING())
                        .field("c", DataTypes.STRING())
                        .field("d", DataTypes.STRING())
                        .build();
        FormatWriterFactory writerFactory = orc.createWriterFactory(rowType);
        Assertions.assertThat(writerFactory).isInstanceOf(OrcWriterFactory.class);

        Path path = new Path(tempDir.toUri().toString(), "1.orc");
        PositionOutputStream out = LocalFileIO.create().newOutputStream(path, true);
        FormatWriter formatWriter = writerFactory.create(out, "zstd");

        Assertions.assertThat(formatWriter).isInstanceOf(OrcBulkWriter.class);

        Options optionsWithLowLevel = new Options();
        optionsWithLowLevel.set("compress", "zstd");
        optionsWithLowLevel.set("stripe.size", "31457280");
        optionsWithLowLevel.set("compression.zstd.level", "1");

        Random random = new Random();
        for (int i = 0; i < 1000; i++) {
            GenericRow element =
                    GenericRow.of(
                            random.nextInt(),
                            BinaryString.fromString(
                                    UUID.randomUUID().toString() + random.nextInt()),
                            BinaryString.fromString(
                                    UUID.randomUUID().toString() + random.nextInt()),
                            BinaryString.fromString(
                                    UUID.randomUUID().toString() + random.nextInt()));
            formatWriter.addElement(element);
        }
        formatWriter.close();
        OrcFile.ReaderOptions readerOptions = OrcFile.readerOptions(new Configuration());
        Reader reader =
                OrcFile.createReader(new org.apache.hadoop.fs.Path(path.toString()), readerOptions);
        Assertions.assertThat(reader.getNumberOfRows()).isEqualTo(1000);
        Assertions.assertThat(reader.getCompressionKind()).isEqualTo(CompressionKind.ZSTD);
        Assertions.assertThat(com.github.luben.zstd.util.Native.isLoaded()).isEqualTo(true);
    }

    @Test
    public void testCorrupt() throws Exception {
        ByteBuffer buf = ByteBuffer.allocate(1000);
        buf.put(new byte[] {127, 125, 1, 99, 98, 1});
        buf.flip();
        try (CompressionCodec codec = new ZstdCodec()) {
            ByteBuffer out = ByteBuffer.allocate(1000);
            codec.decompress(buf, out);
            fail();
        } catch (ZstdException ioe) {
            // EXPECTED
        }
    }

    @Test
    public void testZstdDirectDecompress() {
        ByteBuffer in = ByteBuffer.allocate(10000);
        ByteBuffer out = ByteBuffer.allocate(10000);
        ByteBuffer directOut = ByteBuffer.allocateDirect(10000);
        ByteBuffer directResult = ByteBuffer.allocateDirect(10000);
        for (int i = 0; i < 10000; i++) {
            in.put((byte) i);
        }
        in.flip();
        try (ZstdCodec zstdCodec = new ZstdCodec()) {
            // write bytes to heap buffer.
            assertTrue(zstdCodec.compress(in, out, null, zstdCodec.getDefaultOptions()));
            out.flip();
            // copy heap buffer to direct buffer.
            directOut.put(out);
            directOut.flip();

            zstdCodec.decompress(directOut, directResult);

            // copy result from direct buffer to heap.
            byte[] heapBytes = new byte[in.array().length];
            directResult.get(heapBytes, 0, directResult.limit());

            assertArrayEquals(in.array(), heapBytes);
        } catch (Exception e) {
            fail(e);
        }
    }
}
