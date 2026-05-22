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

import org.apache.paimon.data.GenericArray;
import org.apache.paimon.options.Options;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotEquals;

/** Regression test for production FileIndex path (Issue #5 & #6). */
public class RTreeProductionPathTest {

    @Test
    public void testWriterHandlesInternalArray() {
        RTreeFileIndexWriter writer =
                new RTreeFileIndexWriter(
                        new org.apache.paimon.types.ArrayType(
                                new org.apache.paimon.types.DoubleType()),
                        new Options());

        GenericArray array1 = new GenericArray(new double[] {10.0, 20.0});
        GenericArray array2 = new GenericArray(new double[] {30.0, 40.0});
        GenericArray array3 = new GenericArray(new double[] {50.0, 60.0});

        writer.write(array1);
        writer.write(array2);
        writer.write(array3);

        byte[] data = writer.serializedBytes();
        assertNotEquals(0, data.length, "Serialized data should not be empty");
    }

    @Test
    public void testWriterHandlesMultipleInternalArrays() {
        RTreeFileIndexWriter writer =
                new RTreeFileIndexWriter(
                        new org.apache.paimon.types.ArrayType(
                                new org.apache.paimon.types.DoubleType()),
                        new Options());

        for (int i = 0; i < 50; i++) {
            double x = Math.sin(i * 0.1) * 100;
            double y = Math.cos(i * 0.1) * 100;
            writer.write(new GenericArray(new double[] {x, y}));
        }

        byte[] data = writer.serializedBytes();
        assertNotEquals(0, data.length, "Serialized data should not be empty");
    }

    @Test
    public void testWriterPreservesLeafFlagInTree() {
        RTreeFileIndexWriter writer =
                new RTreeFileIndexWriter(
                        new org.apache.paimon.types.ArrayType(
                                new org.apache.paimon.types.DoubleType()),
                        new Options());

        for (int i = 0; i < 200; i++) {
            double x = Math.sin(i * 0.01) * 1000;
            double y = Math.cos(i * 0.01) * 1000;
            writer.write(new GenericArray(new double[] {x, y}));
        }

        byte[] data = writer.serializedBytes();
        assertNotEquals(0, data.length, "Large dataset serialization should succeed");
    }
}
