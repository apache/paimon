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
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.utils.RoaringBitmap32;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.List;

/** Reader for R-Tree file index. */
public class RTreeFileIndexReader extends FileIndexReader {

    private final SeekableInputStream stream;
    private final int start;
    private final Options options;
    private RTree rtree;
    private int dimensions;
    private int maxEntries;
    private int treeSize;

    public RTreeFileIndexReader(SeekableInputStream stream, int start, Options options)
            throws IOException {
        this.stream = stream;
        this.start = start;
        this.options = options;
        deserializeRTree();
    }

    private void deserializeRTree() throws IOException {
        stream.seek(start);
        DataInputStream dis = new DataInputStream(stream);

        this.dimensions = dis.readInt();
        this.maxEntries = dis.readInt();
        this.treeSize = dis.readInt();

        this.rtree = new RTree(dimensions, maxEntries);

        if (treeSize > 0) {
            deserializeNodes(dis, rtree);
        }
    }

    private void deserializeNodes(DataInputStream dis, RTree tree) throws IOException {
        RTreeNode root = tree.getRoot();
        deserializeNode(dis, root, true);
    }

    private void deserializeNode(DataInputStream dis, RTreeNode node, boolean isRoot)
            throws IOException {
        boolean isLeaf = dis.readBoolean();
        int entryCount = dis.readInt();

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
                RTreeNode child = new RTreeNode(dimensions, maxEntries, false);
                node.addChild(child);
                deserializeNode(dis, child, false);
            }
        }
    }

    @Override
    public FileIndexResult visitEqual(FieldRef fieldRef, Object literal) {
        try {
            if (literal instanceof double[]) {
                double[] point = (double[]) literal;
                if (point.length != dimensions) {
                    return FileIndexResult.REMAIN;
                }

                BoundingBox searchBox = BoundingBox.fromPoint(point);
                List<Integer> results = rtree.search(searchBox);

                if (results.isEmpty()) {
                    return FileIndexResult.SKIP;
                }

                RoaringBitmap32 bitmap = new RoaringBitmap32();
                for (Integer rowId : results) {
                    bitmap.add(rowId);
                }

                return new RTreeIndexResult(() -> bitmap, treeSize);
            } else if (literal instanceof BoundingBox) {
                BoundingBox searchBox = (BoundingBox) literal;
                if (searchBox.getDimensions() != dimensions) {
                    return FileIndexResult.REMAIN;
                }

                List<Integer> results = rtree.search(searchBox);

                if (results.isEmpty()) {
                    return FileIndexResult.SKIP;
                }

                RoaringBitmap32 bitmap = new RoaringBitmap32();
                for (Integer rowId : results) {
                    bitmap.add(rowId);
                }

                return new RTreeIndexResult(() -> bitmap, treeSize);
            }

            return FileIndexResult.REMAIN;
        } catch (Exception e) {
            throw new RuntimeException("Error reading R-Tree index: " + e.getMessage(), e);
        }
    }
}
