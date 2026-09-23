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
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link BlobViewTableReadSupport}. */
class BlobViewTableReadSupportTest {

    @Test
    void testBlobViewFieldIndexesWhenResolveDisabled() {
        RowType rowType =
                RowType.of(
                        new DataField(0, "id", DataTypes.INT()),
                        new DataField(1, "view", DataTypes.BLOB()));
        Options options = new Options();
        options.set(CoreOptions.BLOB_VIEW_FIELD, "view");
        options.set(CoreOptions.BLOB_VIEW_RESOLVE_ENABLED, false);

        assertThat(BlobViewTableReadSupport.blobViewFieldIndexes(rowType, new CoreOptions(options)))
                .isEmpty();
    }

    @Test
    void testBlobViewFieldIndexesReturnsProjectedIndexes() {
        RowType rowType =
                RowType.of(
                        new DataField(0, "id", DataTypes.INT()),
                        new DataField(1, "label", DataTypes.STRING()),
                        new DataField(2, "view", DataTypes.BLOB()));
        Options options = new Options();
        options.set(CoreOptions.BLOB_VIEW_FIELD, "view");

        assertThat(BlobViewTableReadSupport.blobViewFieldIndexes(rowType, new CoreOptions(options)))
                .containsExactly(2);
    }

    @Test
    void testBlobViewFieldIndexesIgnoresNonConfiguredFields() {
        RowType rowType =
                RowType.of(
                        new DataField(0, "id", DataTypes.INT()),
                        new DataField(1, "payload", DataTypes.BLOB()),
                        new DataField(2, "view", DataTypes.BLOB()));
        Options options = new Options();
        options.set(CoreOptions.BLOB_VIEW_FIELD, "view");

        assertThat(BlobViewTableReadSupport.blobViewFieldIndexes(rowType, new CoreOptions(options)))
                .containsExactly(2);
    }
}
