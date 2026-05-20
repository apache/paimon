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
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Test serialization and deserialization of R-Tree. */
public class RTreeSerializationTest {

    @Test
    public void testSerializeDeserializeSmallTree() throws IOException {
        RTree originalTree = new RTree(2, 4);

        for (int i = 0; i < 20; i++) {
            originalTree.insert(new double[] {i, i * 2}, i);
        }

        byte[] serialized = serializeTree(originalTree);

        RTree deserializedTree = deserializeTree(serialized, 2, 4);

        for (int i = 0; i < 20; i++) {
            BoundingBox bbox = BoundingBox.fromPoint(new double[] {i, i * 2});
            List<Integer> results = deserializedTree.search(bbox);
            assertTrue(results.contains(i), "Data " + i + " not found after deserialization");
        }
    }

    @Test
    public void testSerializeDeserializeLargeTree() throws IOException {
        RTree originalTree = new RTree(2, 32);

        for (int i = 0; i < 1000; i++) {
            originalTree.insert(new double[] {i % 100, i / 100}, i);
        }

        byte[] serialized = serializeTree(originalTree);

        RTree deserializedTree = deserializeTree(serialized, 2, 32);

        BoundingBox queryBox = new BoundingBox(new double[] {0, 0}, new double[] {100, 100});
        List<Integer> results = deserializedTree.search(queryBox);

        assertEquals(
                1000, results.size(), "All 1000 records should be found after deserialization");
    }

    @Test
    public void testSerializeDeserializeWithRangeQueries() throws IOException {
        RTree originalTree = new RTree(2, 16);

        for (int i = 0; i < 500; i++) {
            originalTree.insert(new double[] {Math.sin(i * 0.1), Math.cos(i * 0.1)}, i);
        }

        byte[] serialized = serializeTree(originalTree);

        RTree deserializedTree = deserializeTree(serialized, 2, 16);

        BoundingBox smallBox = new BoundingBox(new double[] {-0.5, -0.5}, new double[] {0.5, 0.5});
        List<Integer> originalResults = originalTree.search(smallBox);
        List<Integer> deserializedResults = deserializedTree.search(smallBox);

        assertEquals(
                originalResults.size(),
                deserializedResults.size(),
                "Range query results should be consistent before and after serialization");
    }

    @Test
    public void testSerializationPreservesTreeStructure() throws IOException {
        RTree originalTree = new RTree(3, 8);

        for (int i = 0; i < 100; i++) {
            originalTree.insert(new double[] {i * 0.1, i * 0.2, i * 0.3}, i);
        }

        byte[] serialized = serializeTree(originalTree);

        RTree deserializedTree = deserializeTree(serialized, 3, 8);

        BoundingBox fullSpace =
                new BoundingBox(new double[] {0, 0, 0}, new double[] {100, 100, 100});
        List<Integer> results = deserializedTree.search(fullSpace);

        assertEquals(100, results.size(), "All 100 records should be recovered");
    }

    @Test
    public void testMultipleRoundTripSerialization() throws IOException {
        RTree tree = new RTree(2, 4);

        for (int i = 0; i < 50; i++) {
            tree.insert(new double[] {i, i}, i);
        }

        RTree tree1 = deserializeTree(serializeTree(tree), 2, 4);
        RTree tree2 = deserializeTree(serializeTree(tree1), 2, 4);
        RTree tree3 = deserializeTree(serializeTree(tree2), 2, 4);

        for (int i = 0; i < 50; i++) {
            BoundingBox bbox = BoundingBox.fromPoint(new double[] {i, i});
            List<Integer> results = tree3.search(bbox);
            assertTrue(results.contains(i), "Data preserved through multiple serialization rounds");
        }
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

    private RTree deserializeTree(byte[] data, int dims, int maxEnt) throws IOException {
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
