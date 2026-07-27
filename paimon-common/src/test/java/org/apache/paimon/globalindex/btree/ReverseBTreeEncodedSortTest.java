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
import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.GlobalIndexReader;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.globalindex.GlobalIndexSingleColumnWriter;
import org.apache.paimon.globalindex.KeySerializer;
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.globalindex.ReversedKeySerializer;
import org.apache.paimon.globalindex.io.GlobalIndexFileReader;
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.VarCharType;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.SortUtil;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

import static org.apache.paimon.shade.guava30.com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the reverse-btree encoded sort key. */
public class ReverseBTreeEncodedSortTest {

    private static final DataType TYPE = new VarCharType(VarCharType.MAX_LENGTH);
    private static final FieldRef REF = new FieldRef(1, "f", TYPE);

    private static final String E_ACUTE = String.valueOf((char) 0x00E9);
    private static final String CJK_ZHONG = String.valueOf((char) 0x4E2D);
    private static final String CJK_WEN = String.valueOf((char) 0x6587);
    private static final String HIGH_CHAR = String.valueOf((char) 0xFFFF);
    private static final String ACCENTED = "caf" + E_ACUTE;

    @TempDir java.nio.file.Path tempPath;

    private List<Pair<Object, Long>> data;

    @Test
    public void testEncodedSortOrderMatchesWriterComparator() {
        ReversedKeySerializer reversed = new ReversedKeySerializer(KeySerializer.create(TYPE));
        Comparator<Object> writerComparator = reversed.createComparator();

        String[] samples = {
            "",
            "a",
            "ab",
            "b",
            "red",
            "bored",
            "gold",
            "de",
            ACCENTED,
            "cafe",
            E_ACUTE,
            E_ACUTE + "a",
            CJK_ZHONG,
            CJK_ZHONG + CJK_WEN,
            HIGH_CHAR,
            "z",
            "z ",
            "zz"
        };

        for (String s1 : samples) {
            for (String s2 : samples) {
                BinaryString a = BinaryString.fromString(s1);
                BinaryString b = BinaryString.fromString(s2);
                int engineOrder =
                        SortUtil.compareBinary(reversed.serialize(a), reversed.serialize(b));
                int writerOrder = writerComparator.compare(a, b);
                assertThat(Integer.signum(engineOrder))
                        .as("encoded-sort order must match writer order")
                        .isEqualTo(Integer.signum(writerOrder));
            }
        }
    }

    @Test
    public void testEndsWithWhenSortedByEncodedKey() throws Exception {
        String[] suffixes = {"red", "green", "blue", "gold", ACCENTED};
        ReversedKeySerializer reversed = new ReversedKeySerializer(KeySerializer.create(TYPE));

        try (GlobalIndexReader reader = buildIndexSortedByEncodedKey(reversed, suffixes)) {
            for (String suffix : suffixes) {
                List<Long> expected = idsEndingWith(suffix);
                GlobalIndexResult endsWith =
                        reader.visitEndsWith(REF, BinaryString.fromString(suffix)).join().get();
                assertThat(rowIds(endsWith)).containsExactlyInAnyOrderElementsOf(expected);
            }

            GlobalIndexResult none =
                    reader.visitEndsWith(REF, BinaryString.fromString("nosuchsuffix")).join().get();
            assertThat(none.results().isEmpty()).isTrue();
        }
    }

    private GlobalIndexReader buildIndexSortedByEncodedKey(
            ReversedKeySerializer reversed, String[] suffixes) throws IOException {
        FileIO fileIO = LocalFileIO.create();
        GlobalIndexFileWriter fileWriter =
                new GlobalIndexFileWriter() {
                    @Override
                    public String newFileName(String prefix) {
                        return "reverse-btree-encoded-" + prefix;
                    }

                    @Override
                    public PositionOutputStream newOutputStream(String fileName)
                            throws IOException {
                        return fileIO.newOutputStream(
                                new Path(new Path(tempPath.toUri()), fileName), true);
                    }
                };
        GlobalIndexFileReader fileReader =
                meta ->
                        fileIO.newInputStream(
                                new Path(new Path(tempPath.toUri()), meta.filePath()));

        Options options = new Options();
        options.set(BTreeIndexOptions.BTREE_INDEX_CACHE_SIZE, MemorySize.ofMebiBytes(8));
        ReverseBTreeGlobalIndexer indexer =
                new ReverseBTreeGlobalIndexer(new DataField(1, "f", TYPE), options);

        Random rnd = new Random(11);
        data = new ArrayList<>();
        for (int i = 0; i < 20000; i++) {
            String v = "row" + rnd.nextInt(1_000_000) + suffixes[rnd.nextInt(suffixes.length)];
            data.add(Pair.of(BinaryString.fromString(v), (long) i));
        }

        data.sort(
                (a, b) ->
                        SortUtil.compareBinary(
                                reversed.serialize(a.getKey()), reversed.serialize(b.getKey())));

        GlobalIndexSingleColumnWriter writer = indexer.createWriter(fileWriter);
        for (Pair<Object, Long> p : data) {
            writer.write(p.getKey(), p.getValue());
        }
        List<GlobalIndexIOMeta> metas = new ArrayList<>();
        for (ResultEntry e : writer.finish()) {
            Path path = new Path(new Path(tempPath.toUri()), e.fileName());
            metas.add(new GlobalIndexIOMeta(path, fileIO.getFileSize(path), e.meta()));
        }
        return indexer.createReader(fileReader, metas, newDirectExecutorService());
    }

    private List<Long> idsEndingWith(String suffix) {
        return data.stream()
                .filter(p -> ((BinaryString) p.getKey()).toString().endsWith(suffix))
                .map(Pair::getValue)
                .collect(Collectors.toList());
    }

    private static List<Long> rowIds(GlobalIndexResult result) {
        List<Long> out = new ArrayList<>();
        Iterator<Long> it = result.results().iterator();
        while (it.hasNext()) {
            out.add(it.next());
        }
        return out;
    }
}
