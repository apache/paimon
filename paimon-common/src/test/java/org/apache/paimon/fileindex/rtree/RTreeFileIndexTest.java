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

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class RTreeFileIndexTest {

    @Test
    void testRTreeFileIndexWriterReader() throws Exception {
        // Create index
        ArrayType arrayType = new ArrayType(new DoubleType());
        Options options = new Options();
        options.set(RTreeFileIndex.DIMENSIONS, "2");
        options.set(RTreeFileIndex.MAX_ENTRIES, "8");

        RTreeFileIndex fileIndex = new RTreeFileIndex(arrayType, options);
        FileIndexWriter writer = fileIndex.createWriter();

        // Write test data
        List<double[]> points = new ArrayList<>();
        points.add(new double[] {0.0, 0.0});
        points.add(new double[] {5.0, 5.0});
        points.add(new double[] {10.0, 10.0});
        points.add(new double[] {15.0, 15.0});

        for (double[] point : points) {
            writer.write(point);
        }

        byte[] indexBytes = writer.serializedBytes();

        // Read index
        ByteArraySeekableStream stream = new ByteArraySeekableStream(indexBytes);
        FileIndexReader reader = fileIndex.createReader(stream, 0, indexBytes.length);

        // Query
        FieldRef fieldRef = new FieldRef(0, "location", arrayType);
        FileIndexResult result = reader.visitEqual(fieldRef, new double[] {5.0, 5.0});

        assertThat(result).isNotNull();
        assertThat(result.remain()).isTrue();
    }

    @Test
    void testRTreeFileIndexWithNullValues() throws Exception {
        ArrayType arrayType = new ArrayType(new DoubleType());
        Options options = new Options();
        options.set(RTreeFileIndex.DIMENSIONS, "2");
        options.set(RTreeFileIndex.MAX_ENTRIES, "8");

        RTreeFileIndex fileIndex = new RTreeFileIndex(arrayType, options);
        FileIndexWriter writer = fileIndex.createWriter();

        // Write with null value
        writer.write(null);
        writer.write(new double[] {5.0, 5.0});
        writer.write(null);

        byte[] indexBytes = writer.serializedBytes();

        assertThat(indexBytes).isNotNull();
        assertThat(indexBytes.length).isGreaterThan(0);
    }

    @Test
    void testRTreeFileIndexFactory() {
        RTreeFileIndexFactory factory = new RTreeFileIndexFactory();
        assertThat(factory.identifier()).isEqualTo("rtree");

        ArrayType arrayType = new ArrayType(new DoubleType());
        Options options = new Options();

        RTreeFileIndex index = (RTreeFileIndex) factory.create(arrayType, options);
        assertThat(index).isNotNull();
    }
}
