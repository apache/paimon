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

package org.apache.paimon.globalindex.btree;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.globalindex.GlobalIndexParallelWriter;
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;
import org.apache.paimon.memory.MemorySliceOutput;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.VarCharType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link BTreeIndexMeta}. */
class BTreeIndexMetaTest {

    @TempDir private java.nio.file.Path tempPath;

    @Test
    void testEmptyFirstKey() {
        byte[] lastKey = new byte[] {1, 2, 3};

        BTreeIndexMeta meta = roundTrip(new BTreeIndexMeta(new byte[0], lastKey, false));

        assertThat(meta.getFirstKey()).isEmpty();
        assertThat(meta.getLastKey()).containsExactly(lastKey);
        assertThat(meta.hasNulls()).isFalse();
        assertThat(meta.onlyNulls()).isFalse();
    }

    @Test
    void testEmptyFirstAndLastKeyWithNulls() {
        BTreeIndexMeta meta = roundTrip(new BTreeIndexMeta(new byte[0], new byte[0], true));

        assertThat(meta.getFirstKey()).isEmpty();
        assertThat(meta.getLastKey()).isEmpty();
        assertThat(meta.hasNulls()).isTrue();
        assertThat(meta.onlyNulls()).isFalse();
    }

    @Test
    void testOnlyNulls() {
        BTreeIndexMeta meta = roundTrip(new BTreeIndexMeta(null, null, true));

        assertThat(meta.getFirstKey()).isNull();
        assertThat(meta.getLastKey()).isNull();
        assertThat(meta.hasNulls()).isTrue();
        assertThat(meta.onlyNulls()).isTrue();
    }

    @Test
    void testWriterEmptyStringKeyWithNulls() throws Exception {
        List<ResultEntry> results =
                writeVarCharIndex(
                        null, BinaryString.fromString(""), BinaryString.fromString("abc"));

        BTreeIndexMeta meta = BTreeIndexMeta.deserialize(results.get(0).meta());

        assertThat(meta.getFirstKey()).isEmpty();
        assertThat(meta.getLastKey()).containsExactly(BinaryString.fromString("abc").toBytes());
        assertThat(meta.hasNulls()).isTrue();
        assertThat(meta.onlyNulls()).isFalse();
    }

    @Test
    void testLegacyOnlyNulls() {
        BTreeIndexMeta meta =
                BTreeIndexMeta.deserialize(legacyMetaBytes(new byte[0], new byte[0], true));

        assertThat(meta.getFirstKey()).isNull();
        assertThat(meta.getLastKey()).isNull();
        assertThat(meta.hasNulls()).isTrue();
        assertThat(meta.onlyNulls()).isTrue();
    }

    @Test
    void testLegacyEmptyFirstKey() {
        byte[] lastKey = new byte[] {1, 2, 3};

        BTreeIndexMeta meta =
                BTreeIndexMeta.deserialize(legacyMetaBytes(new byte[0], lastKey, false));

        assertThat(meta.getFirstKey()).isEmpty();
        assertThat(meta.getLastKey()).containsExactly(lastKey);
        assertThat(meta.hasNulls()).isFalse();
        assertThat(meta.onlyNulls()).isFalse();
    }

    @Test
    void testLegacyEmptyFirstAndLastKeyWithoutNulls() {
        BTreeIndexMeta meta =
                BTreeIndexMeta.deserialize(legacyMetaBytes(new byte[0], new byte[0], false));

        assertThat(meta.getFirstKey()).isEmpty();
        assertThat(meta.getLastKey()).isEmpty();
        assertThat(meta.hasNulls()).isFalse();
        assertThat(meta.onlyNulls()).isFalse();
    }

    private BTreeIndexMeta roundTrip(BTreeIndexMeta meta) {
        return BTreeIndexMeta.deserialize(meta.serialize());
    }

    private byte[] legacyMetaBytes(byte[] firstKey, byte[] lastKey, boolean hasNulls) {
        MemorySliceOutput output = new MemorySliceOutput(firstKey.length + lastKey.length + 9);
        output.writeInt(firstKey.length);
        output.writeBytes(firstKey);
        output.writeInt(lastKey.length);
        output.writeBytes(lastKey);
        output.writeByte(hasNulls ? 1 : 0);
        return output.toSlice().copyBytes();
    }

    private List<ResultEntry> writeVarCharIndex(Object... keys) throws IOException {
        FileIO fileIO = LocalFileIO.create();
        GlobalIndexFileWriter fileWriter =
                new GlobalIndexFileWriter() {
                    @Override
                    public String newFileName(String prefix) {
                        return "test-btree-" + prefix;
                    }

                    @Override
                    public PositionOutputStream newOutputStream(String fileName)
                            throws IOException {
                        return fileIO.newOutputStream(
                                new Path(new Path(tempPath.toUri()), fileName), true);
                    }
                };
        GlobalIndexParallelWriter indexWriter =
                new BTreeGlobalIndexer(
                                new DataField(
                                        1, "testField", new VarCharType(VarCharType.MAX_LENGTH)),
                                new Options())
                        .createWriter(fileWriter);
        for (int index = 0; index < keys.length; index++) {
            indexWriter.write(keys[index], index);
        }
        List<ResultEntry> results = indexWriter.finish();
        assertThat(results).hasSize(1);
        return results;
    }
}
