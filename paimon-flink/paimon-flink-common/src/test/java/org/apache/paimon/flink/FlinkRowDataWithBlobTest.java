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

package org.apache.paimon.flink;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.Blob;
import org.apache.paimon.data.BlobDescriptor;
import org.apache.paimon.data.BlobViewStruct;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.utils.UriReader;

import org.apache.flink.table.data.ArrayData;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link FlinkRowDataWithBlob}. */
public class FlinkRowDataWithBlobTest {

    @Test
    public void testRawAndArrayBlobAsData() {
        byte[] raw = new byte[] {1, 2};
        byte[] first = new byte[] {3, 4};
        byte[] second = new byte[] {5, 6};
        byte[] normal = new byte[] {7, 8};
        GenericRow row =
                GenericRow.of(
                        Blob.fromData(raw),
                        new GenericArray(
                                new Object[] {Blob.fromData(first), null, Blob.fromData(second)}),
                        new GenericArray(new Object[] {normal}));

        FlinkRowDataWithBlob rowData =
                new FlinkRowDataWithBlob(row, new HashSet<>(Arrays.asList(0, 1)), false);

        assertThat(rowData.getBinary(0)).isEqualTo(raw);
        ArrayData blobArray = rowData.getArray(1);
        assertThat(blobArray.size()).isEqualTo(3);
        assertThat(blobArray.getBinary(0)).isEqualTo(first);
        assertThat(blobArray.isNullAt(1)).isTrue();
        assertThat(blobArray.getBinary(1)).isNull();
        assertThat(blobArray.getBinary(2)).isEqualTo(second);
        assertThat(rowData.getArray(2).getBinary(0)).isEqualTo(normal);
    }

    @Test
    public void testRawAndArrayBlobAsDescriptor() {
        BlobDescriptor rawDescriptor = new BlobDescriptor("file:///raw", 1, 2);
        BlobDescriptor arrayDescriptor = new BlobDescriptor("file:///array", 3, 4);
        UriReader uriReader = UriReader.fromHttp();
        GenericRow row =
                GenericRow.of(
                        Blob.fromDescriptor(uriReader, rawDescriptor),
                        new GenericArray(
                                new Object[] {Blob.fromDescriptor(uriReader, arrayDescriptor)}));

        FlinkRowDataWithBlob rowData =
                new FlinkRowDataWithBlob(row, new HashSet<>(Arrays.asList(0, 1)), true);

        assertThat(rowData.getBinary(0)).isEqualTo(rawDescriptor.serialize());
        assertThat(rowData.getArray(1).getBinary(0)).isEqualTo(arrayDescriptor.serialize());
    }

    @Test
    public void testUnresolvedBlobViewInArray() {
        BlobViewStruct viewStruct =
                new BlobViewStruct(Identifier.create("database", "table"), 1, 2L);
        GenericRow row =
                GenericRow.of(new GenericArray(new Object[] {Blob.fromView(viewStruct), null}));

        FlinkRowDataWithBlob rowData =
                new FlinkRowDataWithBlob(row, new HashSet<>(Arrays.asList(0)), false);

        ArrayData blobArray = rowData.getArray(0);
        assertThat(blobArray.getBinary(0)).isEqualTo(viewStruct.serialize());
        assertThat(blobArray.getBinary(1)).isNull();
    }
}
