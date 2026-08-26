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

package org.apache.paimon.mergetree.compact;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.compact.CompactManager;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.deletionvectors.BucketedDvMaintainer;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.io.KeyValueFileReaderFactory;
import org.apache.paimon.io.KeyValueFileWriterFactory;
import org.apache.paimon.io.cache.CacheManager;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Comparator;
import java.util.concurrent.ExecutorService;

import static org.apache.paimon.CoreOptions.DELETION_VECTORS_ENABLED;
import static org.mockito.Answers.RETURNS_SELF;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests for {@link MergeTreeCompactManagerFactory}. */
public class MergeTreeCompactManagerFactoryTest {

    private static final RowType KEY_TYPE =
            DataTypes.ROW(
                    DataTypes.FIELD(SpecialFields.KEY_FIELD_ID_START, "_KEY_key", DataTypes.INT()));
    private static final RowType VALUE_TYPE =
            DataTypes.ROW(
                    DataTypes.FIELD(0, "key", DataTypes.INT()),
                    DataTypes.FIELD(1, "value", DataTypes.INT()));

    @Test
    public void testLookupValueProjection() throws Exception {
        Options options = new Options();
        options.set(DELETION_VECTORS_ENABLED, true);
        CoreOptions coreOptions = new CoreOptions(options);

        KeyValueFileReaderFactory.Builder readerBuilder =
                mock(KeyValueFileReaderFactory.Builder.class);
        KeyValueFileReaderFactory.Builder lookupReaderBuilder =
                mock(KeyValueFileReaderFactory.Builder.class, RETURNS_SELF);
        KeyValueFileReaderFactory readerFactory = mock(KeyValueFileReaderFactory.class);
        when(readerBuilder.build(any(BinaryRow.class), anyInt(), any(DeletionVector.Factory.class)))
                .thenReturn(readerFactory);
        when(readerBuilder.copyWithoutProjection()).thenReturn(lookupReaderBuilder);
        when(lookupReaderBuilder.build(
                        any(BinaryRow.class), anyInt(), any(DeletionVector.Factory.class)))
                .thenReturn(mock(KeyValueFileReaderFactory.class));

        KeyValueFileWriterFactory.Builder writerBuilder =
                mock(KeyValueFileWriterFactory.Builder.class);
        when(writerBuilder.build(any(BinaryRow.class), anyInt(), any(CoreOptions.class)))
                .thenReturn(mock(KeyValueFileWriterFactory.class));

        TableSchema schema =
                new TableSchema(
                        0,
                        VALUE_TYPE.getFields(),
                        1,
                        Collections.emptyList(),
                        Collections.singletonList("key"),
                        Collections.emptyMap(),
                        null);
        MergeTreeCompactManagerFactory factory =
                new MergeTreeCompactManagerFactory(
                        readerBuilder,
                        writerBuilder,
                        () -> Comparator.comparingInt(row -> row.getInt(0)),
                        () -> null,
                        () -> null,
                        LookupMergeFunction.wrap(
                                DeduplicateMergeFunction.factory(),
                                coreOptions,
                                KEY_TYPE,
                                VALUE_TYPE),
                        coreOptions,
                        KEY_TYPE,
                        VALUE_TYPE,
                        RowType.of(),
                        mock(FileIO.class),
                        mock(SchemaManager.class),
                        schema,
                        null,
                        mock(CacheManager.class));
        factory.withIOManager(mock(IOManager.class));

        CompactManager manager =
                factory.create(
                        BinaryRow.EMPTY_ROW,
                        0,
                        mock(ExecutorService.class),
                        Collections.emptyList(),
                        mock(BucketedDvMaintainer.class),
                        false);
        try {
            verify(readerBuilder).copyWithoutProjection();
            verify(readerBuilder, never()).withReadValueType(any(RowType.class));
            verify(lookupReaderBuilder).withReadValueType(RowType.of());
            verify(lookupReaderBuilder)
                    .build(any(BinaryRow.class), anyInt(), any(DeletionVector.Factory.class));
        } finally {
            manager.close();
            factory.close();
        }
    }
}
