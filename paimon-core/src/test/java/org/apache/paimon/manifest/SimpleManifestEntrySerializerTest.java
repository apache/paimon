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

package org.apache.paimon.manifest;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.BinaryString;

import org.junit.jupiter.api.Test;

/** Test for {@link SimpleManifestEntrySerializer}. */
public class SimpleManifestEntrySerializerTest {

    @Test
    public void testSerializer() {
        SimpleManifestEntrySerializer simpleManifestEntrySerializer =
                new SimpleManifestEntrySerializer();

        BinaryRow row = new BinaryRow(1);
        BinaryRowWriter binaryRowWriter = new BinaryRowWriter(row);
        binaryRowWriter.writeString(0, BinaryString.fromString("p1"));
        binaryRowWriter.complete();

        SimpleManifestEntry simpleManifestEntry =
                new SimpleManifestEntry(FileKind.ADD, row.copy(), 0, 1, "filetest.avro", 0);
        simpleManifestEntrySerializer
                .convertFrom(2, simpleManifestEntrySerializer.convertTo(simpleManifestEntry))
                .equals(simpleManifestEntry);
    }
}
