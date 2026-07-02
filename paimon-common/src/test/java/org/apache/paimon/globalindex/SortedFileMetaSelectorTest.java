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

package org.apache.paimon.globalindex;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.fs.Path;
import org.apache.paimon.memory.MemorySlice;
import org.apache.paimon.memory.MemorySliceOutput;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.VarCharType;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/** Test class for {@link SortedFileMetaSelector}. */
public class SortedFileMetaSelectorTest {
    private List<GlobalIndexIOMeta> files;

    @BeforeEach
    public void setUp() {
        MemorySliceOutput sliceOutput = new MemorySliceOutput(4);

        SortedIndexFileMeta meta1 =
                new SortedIndexFileMeta(writeInt(1, sliceOutput), writeInt(10, sliceOutput), true);
        SortedIndexFileMeta meta2 =
                new SortedIndexFileMeta(
                        writeInt(15, sliceOutput), writeInt(20, sliceOutput), false);
        SortedIndexFileMeta meta3 =
                new SortedIndexFileMeta(writeInt(21, sliceOutput), writeInt(30, sliceOutput), true);
        SortedIndexFileMeta meta4 =
                new SortedIndexFileMeta(writeInt(1, sliceOutput), writeInt(5, sliceOutput), false);
        SortedIndexFileMeta meta5 =
                new SortedIndexFileMeta(writeInt(19, sliceOutput), writeInt(25, sliceOutput), true);
        SortedIndexFileMeta meta6 = new SortedIndexFileMeta(null, null, true);

        files =
                Arrays.asList(
                        new GlobalIndexIOMeta(new Path("file1"), 1, meta1.serialize()),
                        new GlobalIndexIOMeta(new Path("file2"), 1, meta2.serialize()),
                        new GlobalIndexIOMeta(new Path("file3"), 1, meta3.serialize()),
                        new GlobalIndexIOMeta(new Path("file4"), 1, meta4.serialize()),
                        new GlobalIndexIOMeta(new Path("file5"), 1, meta5.serialize()),
                        new GlobalIndexIOMeta(new Path("file6"), 1, meta6.serialize()));
    }

    @Test
    public void testMetaSelector() {
        DataType dataType = new IntType();
        FieldRef ref = new FieldRef(1, "testField", dataType);
        KeySerializer serializer = KeySerializer.create(dataType);
        SortedFileMetaSelector selector = new SortedFileMetaSelector(files, serializer);

        Optional<List<GlobalIndexIOMeta>> result;

        // 1. test range queries
        result = selector.visitLessThan(ref, 8);
        Assertions.assertThat(result).isNotEmpty();
        assertFiles(result.get(), Arrays.asList("file1", "file4"));

        result = selector.visitLessOrEqual(ref, 20);
        Assertions.assertThat(result).isNotEmpty();
        assertFiles(result.get(), Arrays.asList("file1", "file2", "file4", "file5"));

        result = selector.visitGreaterThan(ref, 20);
        Assertions.assertThat(result).isNotEmpty();
        assertFiles(result.get(), Arrays.asList("file3", "file5"));

        result = selector.visitGreaterOrEqual(ref, 5);
        Assertions.assertThat(result).isNotEmpty();
        assertFiles(result.get(), Arrays.asList("file1", "file2", "file3", "file4", "file5"));

        result = selector.visitEqual(ref, 22);
        Assertions.assertThat(result).isNotEmpty();
        assertFiles(result.get(), Arrays.asList("file3", "file5"));

        result = selector.visitNotEqual(ref, 22);
        Assertions.assertThat(result).isNotEmpty();
        assertFiles(result.get(), Arrays.asList("file1", "file2", "file3", "file4", "file5"));

        // 1.1 test range boundaries
        result = selector.visitLessThan(ref, 15);
        Assertions.assertThat(result).isNotEmpty();
        assertFiles(result.get(), Arrays.asList("file1", "file4"));

        result = selector.visitLessOrEqual(ref, 15);
        Assertions.assertThat(result).isNotEmpty();
        assertFiles(result.get(), Arrays.asList("file1", "file2", "file4"));

        result = selector.visitGreaterThan(ref, 20);
        Assertions.assertThat(result).isNotEmpty();
        assertFiles(result.get(), Arrays.asList("file3", "file5"));

        result = selector.visitGreaterOrEqual(ref, 20);
        Assertions.assertThat(result).isNotEmpty();
        assertFiles(result.get(), Arrays.asList("file2", "file3", "file5"));

        result = selector.visitEqual(ref, 30);
        Assertions.assertThat(result).isNotEmpty();
        assertFiles(result.get(), Arrays.asList("file3"));

        // 1.2 test out of boundaries
        result = selector.visitLessThan(ref, 1);
        Assertions.assertThat(result).isNotEmpty();
        Assertions.assertThat(result.get()).isEmpty();

        result = selector.visitGreaterThan(ref, 30);
        Assertions.assertThat(result).isNotEmpty();
        Assertions.assertThat(result.get()).isEmpty();

        // 2. test isNull & isNotNull
        result = selector.visitIsNull(ref);
        Assertions.assertThat(result).isNotEmpty();
        assertFiles(result.get(), Arrays.asList("file1", "file3", "file5", "file6"));

        result = selector.visitIsNotNull(ref);
        Assertions.assertThat(result).isNotEmpty();
        assertFiles(result.get(), Arrays.asList("file1", "file2", "file3", "file4", "file5"));

        // 3. test in
        result = selector.visitIn(ref, Arrays.asList(1, 2, 3, 26, 27, 28));
        Assertions.assertThat(result).isNotEmpty();
        assertFiles(result.get(), Arrays.asList("file1", "file4", "file3"));

        result = selector.visitNotIn(ref, Arrays.asList(1, 7, 19, 30, 31));
        Assertions.assertThat(result).isNotEmpty();
        assertFiles(result.get(), Arrays.asList("file1", "file2", "file3", "file4", "file5"));

        // 4. test between
        result = selector.visitBetween(ref, 0, 15);
        Assertions.assertThat(result).isNotEmpty();
        assertFiles(result.get(), Arrays.asList("file1", "file2", "file4"));

        result = selector.visitBetween(ref, 0, 30);
        Assertions.assertThat(result).isNotEmpty();
        assertFiles(result.get(), Arrays.asList("file1", "file2", "file3", "file4", "file5"));

        result = selector.visitBetween(ref, 40, 50);
        Assertions.assertThat(result).isNotEmpty();
        Assertions.assertThat(result.get()).isEmpty();

        // 5. test null literals
        result = selector.visitEqual(ref, null);
        Assertions.assertThat(result).isNotEmpty();
        Assertions.assertThat(result.get()).isEmpty();

        result = selector.visitNotEqual(ref, null);
        Assertions.assertThat(result).isNotEmpty();
        Assertions.assertThat(result.get()).isEmpty();

        result = selector.visitIn(ref, Arrays.asList(null, 22));
        Assertions.assertThat(result).isNotEmpty();
        assertFiles(result.get(), Arrays.asList("file3", "file5"));

        result = selector.visitIn(ref, Arrays.asList(null, null));
        Assertions.assertThat(result).isNotEmpty();
        Assertions.assertThat(result.get()).isEmpty();

        result = selector.visitNotIn(ref, Arrays.asList(1, null));
        Assertions.assertThat(result).isNotEmpty();
        Assertions.assertThat(result.get()).isEmpty();

        result = selector.visitBetween(ref, 20, 10);
        Assertions.assertThat(result).isNotEmpty();
        Assertions.assertThat(result.get()).isEmpty();
    }

    @Test
    public void testStringPrefixSelector() {
        DataType dataType = new VarCharType();
        FieldRef ref = new FieldRef(1, "testField", dataType);
        KeySerializer serializer = KeySerializer.create(dataType);
        List<GlobalIndexIOMeta> stringFiles =
                Arrays.asList(
                        newStringFile("file1", serializer, "alpha", "azalea", true),
                        newStringFile("file2", serializer, "beta", "delta", false),
                        newStringFile("file3", serializer, "tag-001", "tag-999", false),
                        new GlobalIndexIOMeta(
                                new Path("file4"),
                                1,
                                new SortedIndexFileMeta(null, null, true).serialize()));
        SortedFileMetaSelector selector = new SortedFileMetaSelector(stringFiles, serializer);

        Optional<List<GlobalIndexIOMeta>> result;

        result = selector.visitStartsWith(ref, str("a"));
        Assertions.assertThat(result).isNotEmpty();
        assertFiles(result.get(), Arrays.asList("file1"));

        result = selector.visitStartsWith(ref, str("tag-"));
        Assertions.assertThat(result).isNotEmpty();
        assertFiles(result.get(), Arrays.asList("file3"));

        result = selector.visitStartsWith(ref, str(""));
        Assertions.assertThat(result).isNotEmpty();
        assertFiles(result.get(), Arrays.asList("file1", "file2", "file3"));

        result = selector.visitStartsWith(ref, null);
        Assertions.assertThat(result).isNotEmpty();
        Assertions.assertThat(result.get()).isEmpty();

        result = selector.visitContains(ref, str("a"));
        Assertions.assertThat(result).isNotEmpty();
        assertFiles(result.get(), Arrays.asList("file1", "file2", "file3"));
    }

    @Test
    public void testStartsWithUsesKeyComparatorForMetaPruning() {
        DataType dataType = new VarCharType();
        FieldRef ref = new FieldRef(1, "testField", dataType);
        KeySerializer serializer = new LengthPrefixedStringSerializer();
        List<GlobalIndexIOMeta> stringFiles =
                Arrays.asList(newStringFile("file1", serializer, "b", "bb", false));
        SortedFileMetaSelector selector = new SortedFileMetaSelector(stringFiles, serializer);

        // Serialized byte order would overlap ["b", "bb"] with the prefix range ["aa", "ab").
        Optional<List<GlobalIndexIOMeta>> result = selector.visitStartsWith(ref, str("aa"));

        Assertions.assertThat(result).isNotEmpty();
        Assertions.assertThat(result.get()).isEmpty();
    }

    private void assertFiles(List<GlobalIndexIOMeta> files, List<String> expected) {
        Assertions.assertThat(
                        files.stream()
                                .map(GlobalIndexIOMeta::filePath)
                                .map(Path::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrderElementsOf(expected);
    }

    private byte[] writeInt(int value, MemorySliceOutput sliceOutput) {
        sliceOutput.reset();
        sliceOutput.writeInt(value);
        return sliceOutput.toSlice().copyBytes();
    }

    private GlobalIndexIOMeta newStringFile(
            String fileName,
            KeySerializer serializer,
            String firstKey,
            String lastKey,
            boolean hasNulls) {
        SortedIndexFileMeta meta =
                new SortedIndexFileMeta(
                        serializer.serialize(str(firstKey)),
                        serializer.serialize(str(lastKey)),
                        hasNulls);
        return new GlobalIndexIOMeta(new Path(fileName), 1, meta.serialize());
    }

    private BinaryString str(String value) {
        return BinaryString.fromString(value);
    }

    @Test
    public void testEmptyStringKeyDoesNotThrowNPE() {
        KeySerializer serializer = KeySerializer.create(new VarCharType());
        FieldRef ref = new FieldRef(1, "page_host", new VarCharType());

        byte[] emptyKey = serializer.serialize(BinaryString.EMPTY_UTF8);
        byte[] normalKey = serializer.serialize(BinaryString.fromString("www.example.com"));

        SortedIndexFileMeta metaWithEmptyFirstKey =
                new SortedIndexFileMeta(emptyKey, normalKey, false);
        SortedIndexFileMeta metaWithNormalKeys =
                new SortedIndexFileMeta(
                        serializer.serialize(BinaryString.fromString("aaa.com")),
                        serializer.serialize(BinaryString.fromString("zzz.com")),
                        false);
        SortedIndexFileMeta metaOnlyNulls = new SortedIndexFileMeta(null, null, true);

        List<GlobalIndexIOMeta> testFiles =
                Arrays.asList(
                        new GlobalIndexIOMeta(
                                new Path("file_empty"), 1, metaWithEmptyFirstKey.serialize()),
                        new GlobalIndexIOMeta(
                                new Path("file_normal"), 1, metaWithNormalKeys.serialize()),
                        new GlobalIndexIOMeta(
                                new Path("file_nulls"), 1, metaOnlyNulls.serialize()));

        SortedFileMetaSelector selector = new SortedFileMetaSelector(testFiles, serializer);

        // visitEqual should not throw NPE
        Optional<List<GlobalIndexIOMeta>> result =
                selector.visitEqual(ref, BinaryString.fromString("www.example.com"));
        Assertions.assertThat(result).isNotEmpty();
        assertFiles(result.get(), Arrays.asList("file_empty", "file_normal"));

        // visitLessThan should not throw NPE
        result = selector.visitLessThan(ref, BinaryString.fromString("bbb.com"));
        Assertions.assertThat(result).isNotEmpty();
        assertFiles(result.get(), Arrays.asList("file_empty", "file_normal"));

        // visitGreaterThan should not throw NPE
        result = selector.visitGreaterThan(ref, BinaryString.fromString("www.example.com"));
        Assertions.assertThat(result).isNotEmpty();

        // visitIn should not throw NPE
        result =
                selector.visitIn(
                        ref,
                        Arrays.asList(
                                BinaryString.fromString("www.example.com"),
                                BinaryString.fromString("zzz.com")));
        Assertions.assertThat(result).isNotEmpty();

        // visitBetween should not throw NPE
        result =
                selector.visitBetween(
                        ref, BinaryString.EMPTY_UTF8, BinaryString.fromString("zzz.com"));
        Assertions.assertThat(result).isNotEmpty();
    }

    private static class LengthPrefixedStringSerializer implements KeySerializer {

        @Override
        public byte[] serialize(Object key) {
            byte[] bytes = ((BinaryString) key).toBytes();
            byte[] result = new byte[bytes.length + 1];
            result[0] = (byte) bytes.length;
            System.arraycopy(bytes, 0, result, 1, bytes.length);
            return result;
        }

        @Override
        public Object deserialize(MemorySlice data) {
            byte[] bytes = data.copyBytes();
            return BinaryString.fromBytes(Arrays.copyOfRange(bytes, 1, bytes.length));
        }

        @Override
        public Comparator<Object> createComparator() {
            return Comparator.comparing(o -> (BinaryString) o);
        }
    }
}
