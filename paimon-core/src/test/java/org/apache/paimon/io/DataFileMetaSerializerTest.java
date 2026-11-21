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

package org.apache.paimon.io;

import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.utils.ObjectSerializerTestBase;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/** Tests for {@link DataFileMetaSerializer}. */
public class DataFileMetaSerializerTest extends ObjectSerializerTestBase<DataFileMeta> {

    private final DataFileTestDataGenerator gen = DataFileTestDataGenerator.builder().build();

    @Override
    protected DataFileMetaSerializer serializer() {
        return new DataFileMetaSerializer();
    }

    @Override
    protected DataFileMeta object() {
        return gen.next()
                .meta
                .copy(Arrays.asList("extra1", "extra2"))
                .withWriteColsSequences(new long[] {3L, 42L});
    }

    @Test
    void testCopyOperationsPreserveWriteColsSequences() {
        DataFileMeta file = object();
        assertWriteColsSequences(file.upgrade(file.level() + 1));
        assertWriteColsSequences(file.rename("renamed.parquet"));
        assertWriteColsSequences(file.copyWithoutStats());
        assertWriteColsSequences(file.assignSequenceNumber(1L, 2L));
        assertWriteColsSequences(file.assignFirstRowId(1L));
        assertWriteColsSequences(file.newFirstRowId(null));
        assertWriteColsSequences(file.copy(Collections.emptyList()));
        assertWriteColsSequences(file.newExternalPath("external/renamed.parquet"));
        assertWriteColsSequences(file.copy(new byte[] {1}));
        assertWriteColsSequences(file.withWriteCols(Arrays.asList("first", "second")));
    }

    @Test
    void testWriteColumnCopyPreservesMetadataAndSerialization() {
        DataFileMeta original = metadataWithAllFields();
        List<String> sharedNames = new ArrayList<>(Arrays.asList("f0", "nested.f1", "f0", "F0"));
        DataFileMeta copy = original.withWriteCols(sharedNames);
        assertThat(copy).isEqualTo(original);
        assertThat(copy.hashCode()).isEqualTo(original.hashCode());
        assertThat(copy.writeCols()).isNotSameAs(original.writeCols());
        assertThat(copy.writeCols().get(0)).isSameAs(sharedNames.get(0));
        assertThat(copy.writeColsSequences()).containsExactly(3L, 42L, 43L, 50L);
        assertThat(serializer().fromRow(serializer().toRow(copy))).isEqualTo(original);
        assertThat(copy.withWriteCols(null).writeCols()).isNull();
        assertThat(copy.withWriteCols(Collections.emptyList()).writeCols()).isEmpty();
        copy.writeCols().set(0, "changed");
        assertThat(original.writeCols()).containsExactly("f0", "nested.f1", "f0", "F0");
    }

    @Test
    void testDefaultWriteColumnCopyMatchesPojo() {
        DataFileMeta original = metadataWithAllFields();
        DataFileMeta external =
                mock(
                        DataFileMeta.class,
                        invocation -> {
                            if (invocation.getMethod().getName().equals("withWriteCols")) {
                                return invocation.callRealMethod();
                            }
                            return invocation
                                    .getMethod()
                                    .invoke(original, invocation.getArguments());
                        });
        for (List<String> names :
                Arrays.<List<String>>asList(
                        null,
                        Collections.emptyList(),
                        Arrays.asList("f0", "nested.f1", "f0", "F0"))) {
            DataFileMeta copy = external.withWriteCols(names);
            assertThat(copy).isEqualTo(original.withWriteCols(names));
            assertThat(serializer().fromRow(serializer().toRow(copy))).isEqualTo(copy);
        }
    }

    private DataFileMeta metadataWithAllFields() {
        DataFileMeta template = object();
        return DataFileMeta.create(
                "columns.parquet",
                123L,
                5L,
                template.minKey(),
                template.maxKey(),
                template.keyStats(),
                template.valueStats(),
                11L,
                99L,
                7L,
                2,
                Arrays.asList("extra-1", "extra-2"),
                template.creationTime(),
                1L,
                new byte[] {1, 2},
                FileSource.COMPACT,
                Collections.singletonList("stats-column"),
                "external/columns.parquet",
                100L,
                Arrays.asList(
                        new String("f0"),
                        new String("nested.f1"),
                        new String("f0"),
                        new String("F0")),
                new long[] {3L, 42L, 43L, 50L});
    }

    @Test
    void testLegacySerializerDropsWriteColsSequences() {
        DataFileMetaWriteColsLegacySerializer legacy = new DataFileMetaWriteColsLegacySerializer();
        DataFileMeta file = legacy.fromRow(legacy.toRow(object()));
        assertThat(file.writeColsSequences()).isNull();
    }

    @Test
    void testWriteColsSequencesAreDefensivelyCopied() {
        long[] sequences = {3L, 42L};
        DataFileMeta file = gen.next().meta.withWriteColsSequences(sequences);

        sequences[0] = 100L;
        assertWriteColsSequences(file);

        long[] returned = file.writeColsSequences();
        returned[1] = 100L;
        assertWriteColsSequences(file);
    }

    private void assertWriteColsSequences(DataFileMeta file) {
        assertThat(file.writeColsSequences()).containsExactly(3L, 42L);
    }
}
