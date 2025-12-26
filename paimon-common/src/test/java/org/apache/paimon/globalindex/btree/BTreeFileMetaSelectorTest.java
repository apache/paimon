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

import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.memory.MemorySliceOutput;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.IntType;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/** Test class for {@link BTreeFileMetaSelector}. */
public class BTreeFileMetaSelectorTest {
    private List<GlobalIndexIOMeta> files;

    @BeforeEach
    public void setUp() {
        MemorySliceOutput sliceOutput = new MemorySliceOutput(4);

        BTreeIndexMeta meta1 =
                new BTreeIndexMeta(writeInt(1, sliceOutput), writeInt(10, sliceOutput), true);
        BTreeIndexMeta meta2 =
                new BTreeIndexMeta(writeInt(15, sliceOutput), writeInt(20, sliceOutput), false);
        BTreeIndexMeta meta3 =
                new BTreeIndexMeta(writeInt(21, sliceOutput), writeInt(30, sliceOutput), true);
        BTreeIndexMeta meta4 =
                new BTreeIndexMeta(writeInt(1, sliceOutput), writeInt(5, sliceOutput), false);
        BTreeIndexMeta meta5 =
                new BTreeIndexMeta(writeInt(19, sliceOutput), writeInt(25, sliceOutput), true);

        files =
                Arrays.asList(
                        new GlobalIndexIOMeta("file1", 1, meta1.serialize()),
                        new GlobalIndexIOMeta("file2", 1, meta2.serialize()),
                        new GlobalIndexIOMeta("file3", 1, meta3.serialize()),
                        new GlobalIndexIOMeta("file4", 1, meta4.serialize()),
                        new GlobalIndexIOMeta("file5", 1, meta5.serialize()));
    }

    @Test
    public void testMetaSelector() {
        DataType dataType = new IntType();
        FieldRef ref = new FieldRef(1, "testField", dataType);
        KeySerializer serializer = KeySerializer.create(dataType);
        BTreeFileMetaSelector selector = new BTreeFileMetaSelector(files, serializer);

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
        assertFiles(result.get(), Arrays.asList("file1", "file3", "file5"));

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
    }

    private void assertFiles(List<GlobalIndexIOMeta> files, List<String> expected) {
        Assertions.assertThat(
                        files.stream()
                                .map(GlobalIndexIOMeta::fileName)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrderElementsOf(expected);
    }

    private byte[] writeInt(int value, MemorySliceOutput sliceOutput) {
        sliceOutput.reset();
        sliceOutput.writeInt(value);
        return sliceOutput.toSlice().copyBytes();
    }
}
