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

package org.apache.paimon.postpone;

import org.apache.paimon.KeyValue;
import org.apache.paimon.compression.CompressOptions;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.io.KeyValueFileWriterFactory;
import org.apache.paimon.io.RollingFileWriter;
import org.apache.paimon.mergetree.compact.MergeFunction;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.types.RowKind;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.same;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests for {@link PostponeBucketWriter}. */
class PostponeBucketWriterTest {

    @Test
    @SuppressWarnings("unchecked")
    void testSkipBlobExternalizationWithoutExternalizer() throws Exception {
        KeyValueFileWriterFactory writerFactory = mock(KeyValueFileWriterFactory.class);
        RollingFileWriter<KeyValue, DataFileMeta> rollingWriter = mock(RollingFileWriter.class);
        when(writerFactory.hasBlobExternalizer()).thenReturn(false);
        when(writerFactory.createRollingMergeTreeFileWriter(anyInt(), any()))
                .thenReturn(rollingWriter);
        PostponeBucketWriter writer =
                new PostponeBucketWriter(
                        mock(FileIO.class),
                        mock(DataFilePathFactory.class),
                        CompressOptions.defaultOptions(),
                        new MemorySize(1024),
                        null,
                        mock(MergeFunction.class),
                        writerFactory,
                        null,
                        false,
                        false,
                        null);
        KeyValue record =
                new KeyValue()
                        .replace(
                                GenericRow.of(1),
                                RowKind.INSERT,
                                GenericRow.of(1, "ordinary-value"));

        writer.write(record);

        verify(writerFactory).hasBlobExternalizer();
        verify(writerFactory, never()).externalizeBlob(any(), any());
        verify(rollingWriter).write(same(record));
    }

    @Test
    void testGetWriteIdWithSeparatorInCommitUser() {
        // the commit user is interpolated verbatim into the writer prefix, so it may
        // contain the "-s-" separator itself
        assertThat(
                        PostponeBucketFileStoreWrite.getWriteId(
                                "a-s-123-b-u-my-s-user-s-42-w-data-uuid-0.orc"))
                .isEqualTo(42);
        assertThat(PostponeBucketFileStoreWrite.getWriteId("p-u-cu-s-7-w-data-uuid-0.orc"))
                .isEqualTo(7);
        // hostile commit user alone: the first split would hit the in-user separator and
        // crash on the missing dash
        assertThat(PostponeBucketFileStoreWrite.getWriteId("p-u-my-s-user-s-42-w-data-uuid-0.orc"))
                .isEqualTo(42);
    }
}
