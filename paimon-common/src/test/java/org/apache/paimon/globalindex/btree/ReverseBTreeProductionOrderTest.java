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
import org.apache.paimon.globalindex.GlobalIndexSingleColumnWriter;
import org.apache.paimon.globalindex.KeySerializer;
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.VarCharType;
import org.apache.paimon.utils.Pair;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the reverse-btree writer key-ordering contract. */
public class ReverseBTreeProductionOrderTest {

    private static final DataType TYPE = new VarCharType(VarCharType.MAX_LENGTH);

    @TempDir java.nio.file.Path tempPath;

    @Test
    public void testNaturalOrderInputIsRejected() {
        assertThatThrownBy(this::buildIndexInNaturalOrder)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("non-decreasing order");
    }

    private void buildIndexInNaturalOrder() throws IOException {
        FileIO fileIO = LocalFileIO.create();
        GlobalIndexFileWriter fileWriter =
                new GlobalIndexFileWriter() {
                    @Override
                    public String newFileName(String prefix) {
                        return "reverse-btree-prod-" + prefix;
                    }

                    @Override
                    public PositionOutputStream newOutputStream(String fileName)
                            throws IOException {
                        return fileIO.newOutputStream(
                                new Path(new Path(tempPath.toUri()), fileName), true);
                    }
                };

        Options options = new Options();
        options.set(BTreeIndexOptions.BTREE_INDEX_CACHE_SIZE, MemorySize.ofMebiBytes(8));
        ReverseBTreeGlobalIndexer indexer =
                new ReverseBTreeGlobalIndexer(new DataField(1, "f", TYPE), options);

        Comparator<Object> naturalCmp = KeySerializer.create(TYPE).createComparator();

        String[] suffixes = {"red", "green", "blue", "gold"};
        Random rnd = new Random(7);
        List<Pair<Object, Long>> data = new ArrayList<>();
        for (int i = 0; i < 20000; i++) {
            String v = "row" + rnd.nextInt(1_000_000) + suffixes[rnd.nextInt(suffixes.length)];
            data.add(Pair.of(BinaryString.fromString(v), (long) i));
        }
        data.sort((a, b) -> naturalCmp.compare(a.getKey(), b.getKey()));

        GlobalIndexSingleColumnWriter writer = indexer.createWriter(fileWriter);
        for (Pair<Object, Long> p : data) {
            writer.write(p.getKey(), p.getValue());
        }
        writer.finish();
    }
}
