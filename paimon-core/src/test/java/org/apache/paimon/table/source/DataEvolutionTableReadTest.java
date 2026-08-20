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

package org.apache.paimon.table.source;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.ReadBatchSizer;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests for {@link DataEvolutionTableRead}. */
class DataEvolutionTableReadTest {

    @Test
    void testReadBatchSizerPropagatesToBlobPrescan() throws IOException {
        Options options = new Options();
        options.set(CoreOptions.BLOB_VIEW_FIELD, "blob");
        TableSchema schema = mock(TableSchema.class);
        when(schema.logicalRowType())
                .thenReturn(RowType.builder().field("blob", DataTypes.BLOB()).build());
        InnerTableRead prescanRead = mock(InnerTableRead.class);
        when(prescanRead.createReader(any(Split.class)))
                .thenThrow(new IOException("expected prescan stop"));
        DataEvolutionTableRead read =
                new DataEvolutionTableRead(
                        Collections.emptyList(),
                        schema,
                        new CoreOptions(options),
                        CatalogContext.create(new Options()),
                        () -> prescanRead);
        ReadBatchSizer sizer = new ReadBatchSizer();
        read.withReadBatchSizer(sizer);

        assertThatThrownBy(() -> read.createReader(mock(Split.class)))
                .isInstanceOf(IOException.class)
                .hasMessage("expected prescan stop");
        verify(prescanRead).withReadBatchSizer(sizer);
    }
}
