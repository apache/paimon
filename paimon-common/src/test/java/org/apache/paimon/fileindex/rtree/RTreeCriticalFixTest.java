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

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Test for deserialization correctness and range query precision. */
public class RTreeCriticalFixTest {

    @Test
    public void testRootNodeLeafFlagAfterDeserialization() throws IOException {
        RTree originalTree = new RTree(2, 4);

        for (int i = 0; i < 20; i++) {
            originalTree.insert(new double[] {i, i}, i);
        }

        assertFalse(originalTree.getRoot().isLeaf(), "Original tree root should be internal node");

        byte[] serialized = serializeTree(originalTree);

        RTree deserializedTree = deserializeTree(serialized, 2, 4);

        assertFalse(
                deserializedTree.getRoot().isLeaf(),
                "Root leaf flag should be corrected during deserialization");

        for (int i = 0; i < 20; i++) {
            BoundingBox bbox = BoundingBox.fromPoint(new double[] {i, i});
            List<Integer> results = deserializedTree.search(bbox);
            assertTrue(results.contains(i), "Should find rowId " + i + " after deserialization");
        }
    }

    @Test
    public void testRangeQueryPrecision() {
        RTree rtree = new RTree(2, 4);

        rtree.insert(new double[] {35, 35}, 1);
        rtree.insert(new double[] {45, 45}, 2);
        rtree.insert(new double[] {10, 10}, 3);

        BoundingBox query = new BoundingBox(new double[] {30, 30}, new double[] {40, 40});
        List<Integer> results = rtree.search(query);

        assertEquals(1, results.size(), "Should have only 1 result");
        assertTrue(results.contains(1), "Should contain rowId 1 (35,35)");
        assertFalse(results.contains(2), "Should NOT contain rowId 2 (45,45)");
    }

    @Test
    public void testRangeQueryWithMultipleEntries() {
        RTree rtree = new RTree(2, 8);

        for (int i = 0; i < 100; i++) {
            rtree.insert(new double[] {Math.sin(i * 0.1) * 50, Math.cos(i * 0.1) * 50}, i);
        }

        BoundingBox query = new BoundingBox(new double[] {-25, -25}, new double[] {25, 25});
        List<Integer> results = rtree.search(query);

        for (Integer rowId : results) {
            double[] point = new double[] {Math.sin(rowId * 0.1) * 50, Math.cos(rowId * 0.1) * 50};
            assertTrue(query.contains(point), "All results should be within query box");
        }
    }

    @Test
    public void testSerializationRoundTripCorrectness() throws IOException {
        RTree originalTree = new RTree(2, 16);

        for (int i = 0; i < 500; i++) {
            originalTree.insert(new double[] {Math.sin(i * 0.1), Math.cos(i * 0.1)}, i);
        }

        byte[] serialized = serializeTree(originalTree);
        RTree deserializedTree = deserializeTree(serialized, 2, 16);

        BoundingBox smallQuery =
                new BoundingBox(new double[] {-0.5, -0.5}, new double[] {0.5, 0.5});

        List<Integer> originalResults = originalTree.search(smallQuery);
        List<Integer> deserializedResults = deserializedTree.search(smallQuery);

        assertEquals(
                originalResults.size(),
                deserializedResults.size(),
                "Range query results should match after round-trip");

        for (Integer rowId : deserializedResults) {
            double[] point = new double[] {Math.sin(rowId * 0.1), Math.cos(rowId * 0.1)};
            assertTrue(
                    smallQuery.contains(point),
                    "Deserialized tree should return only points within query box");
        }
    }

    @Test
    public void testDeepTreeDeserialization() throws IOException {
        RTree originalTree = new RTree(2, 8);

        for (int i = 0; i < 5000; i++) {
            originalTree.insert(new double[] {i % 100, i / 100}, i);
        }

        byte[] serialized = serializeTree(originalTree);
        RTree deserializedTree = deserializeTree(serialized, 2, 8);

        BoundingBox wholeSpace = new BoundingBox(new double[] {0, 0}, new double[] {100, 100});
        List<Integer> results = deserializedTree.search(wholeSpace);

        assertEquals(
                5000,
                results.size(),
                "All 5000 records should be recoverable after deserialization");
    }

    private byte[] serializeTree(RTree tree) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);

        dos.writeInt(tree.getDimensions());
        dos.writeInt(tree.getMaxEntries());
        dos.writeInt(tree.getSize());

        if (tree.getSize() > 0) {
            serializeNode(tree.getRoot(), dos);
        }

        dos.close();
        return baos.toByteArray();
    }

    private void serializeNode(RTreeNode node, DataOutputStream dos) throws IOException {
        dos.writeBoolean(node.isLeaf());
        dos.writeInt(node.getEntryCount());
        node.getBoundingBox().serialize(dos);

        if (node.isLeaf()) {
            for (LeafEntry entry : node.getLeafEntries()) {
                dos.writeInt(entry.getRowId());
                entry.getBbox().serialize(dos);
            }
        } else {
            for (RTreeNode child : node.getChildren()) {
                serializeNode(child, dos);
            }
        }
    }

    private RTree deserializeTree(byte[] data, int dimensions, int maxEntries) throws IOException {
        ByteArrayInputStream bais = new ByteArrayInputStream(data);
        DataInputStream dis = new DataInputStream(bais);

        int deserializedDimensions = dis.readInt();
        int deserializedMaxEntries = dis.readInt();
        int size = dis.readInt();

        RTree tree = new RTree(deserializedDimensions, deserializedMaxEntries);

        if (size > 0) {
            RTreeNode newRoot =
                    deserializeNode(dis, deserializedDimensions, deserializedMaxEntries);
            tree.setRoot(newRoot);
            tree.setSize(size);
        }

        dis.close();
        return tree;
    }

    private RTreeNode deserializeNode(DataInputStream dis, int dimensions, int maxEntries)
            throws IOException {
        boolean isLeaf = dis.readBoolean();
        int entryCount = dis.readInt();

        RTreeNode node = new RTreeNode(dimensions, maxEntries, isLeaf);

        BoundingBox bbox = BoundingBox.deserialize(dis, dimensions);
        node.getBoundingBox().expand(bbox);

        if (isLeaf) {
            for (int i = 0; i < entryCount; i++) {
                int rowId = dis.readInt();
                BoundingBox entryBbox = BoundingBox.deserialize(dis, dimensions);
                node.addLeafEntry(new LeafEntry(rowId, entryBbox));
            }
        } else {
            for (int i = 0; i < entryCount; i++) {
                RTreeNode child = deserializeNode(dis, dimensions, maxEntries);
                node.addChild(child);
            }
        }

        return node;
    }
}
