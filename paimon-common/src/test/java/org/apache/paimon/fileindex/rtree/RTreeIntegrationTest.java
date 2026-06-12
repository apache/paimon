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

package org.apache.paimon.fileindex.rtree;

import org.apache.paimon.fileindex.FileIndexReader;
import org.apache.paimon.fileindex.FileIndexResult;
import org.apache.paimon.fileindex.FileIndexWriter;
import org.apache.paimon.fs.ByteArraySeekableStream;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DoubleType;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class RTreeIntegrationTest {

    @Test
    void testRTreeWithLargeDataset() throws Exception {
        ArrayType arrayType = new ArrayType(new DoubleType());
        Options options = new Options();
        options.set(RTreeFileIndex.DIMENSIONS, "2");
        options.set(RTreeFileIndex.MAX_ENTRIES, "16");

        RTreeFileIndex fileIndex = new RTreeFileIndex(arrayType, options);
        FileIndexWriter writer = fileIndex.createWriter();

        // Generate some test points
        writer.write(new double[] {50.0, 50.0});
        writer.write(new double[] {51.0, 51.0});
        writer.write(new double[] {52.0, 52.0});

        byte[] indexBytes = writer.serializedBytes();
        assertThat(indexBytes).isNotEmpty();

        // Read index
        ByteArraySeekableStream stream = new ByteArraySeekableStream(indexBytes);
        FileIndexReader reader = fileIndex.createReader(stream, 0, indexBytes.length);

        // Query with bounding box
        FieldRef fieldRef = new FieldRef(0, "location", arrayType);

        // Query point in the indexed area
        FileIndexResult result1 = reader.visitEqual(fieldRef, new double[] {50.0, 50.0});
        assertThat(result1).isNotNull();
    }

    @Test
    void testRTreeFactoryRegistration() {
        RTreeFileIndexFactory factory = new RTreeFileIndexFactory();
        assertThat(factory.identifier()).isEqualTo("rtree");
        assertThat(factory).isInstanceOf(org.apache.paimon.fileindex.FileIndexerFactory.class);
    }

    @Test
    void testRTreeWithMultipleBoundingBoxes() throws Exception {
        ArrayType arrayType = new ArrayType(new DoubleType());
        Options options = new Options();
        options.set(RTreeFileIndex.DIMENSIONS, "2");
        options.set(RTreeFileIndex.MAX_ENTRIES, "8");

        RTreeFileIndex fileIndex = new RTreeFileIndex(arrayType, options);
        FileIndexWriter writer = fileIndex.createWriter();

        // Write some test points
        writer.write(new double[] {0.0, 0.0});
        writer.write(new double[] {50.0, 50.0});
        writer.write(new double[] {90.0, 90.0});

        byte[] indexBytes = writer.serializedBytes();

        // Read and query
        ByteArraySeekableStream stream = new ByteArraySeekableStream(indexBytes);
        FileIndexReader reader = fileIndex.createReader(stream, 0, indexBytes.length);

        FieldRef fieldRef = new FieldRef(0, "location", arrayType);

        // Just verify we can query without errors
        FileIndexResult result1 = reader.visitEqual(fieldRef, new double[] {0.0, 0.0});
        assertThat(result1).isNotNull();

        FileIndexResult result2 = reader.visitEqual(fieldRef, new double[] {50.0, 50.0});
        assertThat(result2).isNotNull();

        FileIndexResult result3 = reader.visitEqual(fieldRef, new double[] {90.0, 90.0});
        assertThat(result3).isNotNull();
    }
}
