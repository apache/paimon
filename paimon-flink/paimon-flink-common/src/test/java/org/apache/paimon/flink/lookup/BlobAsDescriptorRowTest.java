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

package org.apache.paimon.flink.lookup;

import org.apache.paimon.data.Blob;
import org.apache.paimon.data.BlobDescriptor;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.UriReader;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link BlobAsDescriptorRow}. */
class BlobAsDescriptorRowTest {

    @Test
    void testConvertBlobToDescriptor() {
        BlobDescriptor descriptor = new BlobDescriptor("https://example.com/blob", 12, 34);
        GenericRow wrapped =
                GenericRow.of(7, Blob.fromDescriptor(UriReader.fromHttp(), descriptor), null);
        BlobAsDescriptorRow row =
                new BlobAsDescriptorRow(wrapped, new HashSet<>(Arrays.asList(1, 2)));

        assertThat(row.getInt(0)).isEqualTo(7);
        assertThat(BlobDescriptor.deserialize(row.getBinary(1))).isEqualTo(descriptor);
        assertThat(row.getBinary(2)).isNull();

        row.setRowKind(RowKind.DELETE);
        assertThat(row.getRowKind()).isEqualTo(RowKind.DELETE);
        assertThat(wrapped.getRowKind()).isEqualTo(RowKind.DELETE);
    }

    @Test
    void testReplaceBlobWithVarBinary() {
        RowType rowType =
                RowType.builder()
                        .field("id", DataTypes.INT())
                        .field("picture", DataTypes.BLOB())
                        .field("raw", DataTypes.BYTES())
                        .build();

        Set<Integer> blobPositions = BlobAsDescriptorRow.blobFieldPositions(rowType);
        RowType converted = BlobAsDescriptorRow.replaceBlobWithVarBinary(rowType, blobPositions);

        assertThat(blobPositions).containsExactlyInAnyOrder(1);
        assertThat(converted.getTypeAt(0)).isEqualTo(DataTypes.INT());
        assertThat(converted.getTypeAt(1).getTypeRoot()).isEqualTo(DataTypeRoot.VARBINARY);
        assertThat(converted.getTypeAt(2)).isEqualTo(DataTypes.BYTES());
        assertThat(rowType.getTypeAt(1)).isEqualTo(DataTypes.BLOB());
    }

    @Test
    void testKeepOriginalTypeWhenThereIsNoBlob() {
        RowType rowType = RowType.of(DataTypes.INT(), DataTypes.BYTES());

        assertThat(
                        BlobAsDescriptorRow.replaceBlobWithVarBinary(
                                rowType, BlobAsDescriptorRow.blobFieldPositions(rowType)))
                .isSameAs(rowType);
    }
}
