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

import java.util.ArrayList;
import java.util.List;

/** R-Tree spatial index implementation. */
public class RTree {
    private RTreeNode root;
    private final int dimensions;
    private final int minEntries;
    private final int maxEntries;
    private int size;

    public RTree(int dimensions, int maxEntries) {
        this.dimensions = dimensions;
        this.maxEntries = maxEntries;
        this.minEntries = Math.max(2, maxEntries / 2);
        this.root = new RTreeNode(dimensions, maxEntries, true);
        this.size = 0;
    }

    public int getDimensions() {
        return dimensions;
    }

    public int getMinEntries() {
        return minEntries;
    }

    public int getMaxEntries() {
        return maxEntries;
    }

    public RTreeNode getRoot() {
        return root;
    }

    public void setRoot(RTreeNode newRoot) {
        this.root = newRoot;
    }

    public void setSize(int newSize) {
        this.size = newSize;
    }

    public int getSize() {
        return size;
    }

    public void insert(double[] point, int rowId) {
        BoundingBox pointBox = BoundingBox.fromPoint(point);
        insert(pointBox, rowId, root);
        size++;
    }

    private void insert(BoundingBox bbox, int rowId, RTreeNode node) {
        if (node.isLeaf()) {
            node.addLeafEntry(new LeafEntry(rowId, bbox));

            if (node.canSplit()) {
                splitNode(node);
            }
        } else {
            RTreeNode bestChild = chooseBestChild(node, bbox);
            insert(bbox, rowId, bestChild);

            if (bestChild.canSplit()) {
                splitNode(bestChild);
            }
            node.getBoundingBox().expand(bbox);
        }
    }

    private RTreeNode chooseBestChild(RTreeNode node, BoundingBox bbox) {
        RTreeNode best = null;
        double minExpansion = Double.POSITIVE_INFINITY;
        double minArea = Double.POSITIVE_INFINITY;

        for (RTreeNode child : node.getChildren()) {
            double expansion = child.getBoundingBox().getExpansionArea(bbox);
            double area = child.getBoundingBox().getArea();

            if (expansion < minExpansion || (expansion == minExpansion && area < minArea)) {
                minExpansion = expansion;
                minArea = area;
                best = child;
            }
        }

        if (best == null) {
            throw new IllegalStateException("No child found in non-leaf node");
        }
        return best;
    }

    private void splitNode(RTreeNode node) {
        if (node.isLeaf()) {
            splitLeafNode(node);
        } else {
            splitInternalNode(node);
        }
    }

    private void splitLeafNode(RTreeNode node) {
        List<LeafEntry> entries = new ArrayList<>(node.getLeafEntries());
        node.getLeafRowIds().clear();
        node.getLeafEntries().clear();
        node.getBoundingBox().clear();

        RTreeNode newNode = new RTreeNode(dimensions, maxEntries, true);

        // Use quadratic split instead of linear split
        QuadraticSplit split = new QuadraticSplit(entries, dimensions);

        for (LeafEntry entry : split.getGroup1()) {
            node.addLeafEntry(entry);
        }
        for (LeafEntry entry : split.getGroup2()) {
            newNode.addLeafEntry(entry);
        }

        if (node == root) {
            RTreeNode newRoot = new RTreeNode(dimensions, maxEntries, false);
            newRoot.addChild(node);
            newRoot.addChild(newNode);
            root = newRoot;
        } else {
            adjustParent(node, newNode);
        }
    }

    private void splitInternalNode(RTreeNode node) {
        List<RTreeNode> children = new ArrayList<>(node.getChildren());
        node.getChildren().clear();
        node.getBoundingBox().clear();

        RTreeNode newNode = new RTreeNode(dimensions, maxEntries, false);

        // Use quadratic split for internal nodes too
        QuadraticSplitInternal split = new QuadraticSplitInternal(children, dimensions);

        for (RTreeNode child : split.getGroup1()) {
            node.addChild(child);
        }
        for (RTreeNode child : split.getGroup2()) {
            newNode.addChild(child);
        }

        if (node == root) {
            RTreeNode newRoot = new RTreeNode(dimensions, maxEntries, false);
            newRoot.addChild(node);
            newRoot.addChild(newNode);
            root = newRoot;
        } else {
            adjustParent(node, newNode);
        }
    }

    private void adjustParent(RTreeNode node, RTreeNode newNode) {
        RTreeNode parent = node.getParent();
        if (parent == null) {
            return;
        }

        parent.addChild(newNode);

        if (parent.canSplit()) {
            splitNode(parent);
        }
    }

    public List<Integer> search(BoundingBox searchBox) {
        List<Integer> results = new ArrayList<>();
        search(searchBox, root, results);
        return results;
    }

    private void search(BoundingBox searchBox, RTreeNode node, List<Integer> results) {
        if (!node.getBoundingBox().intersects(searchBox)) {
            return;
        }

        if (node.isLeaf()) {
            // Check entry bbox precisely, not just node bbox
            for (LeafEntry entry : node.getLeafEntries()) {
                if (entry.getBbox().intersects(searchBox)) {
                    results.add(entry.getRowId());
                }
            }
        } else {
            for (RTreeNode child : node.getChildren()) {
                search(searchBox, child, results);
            }
        }
    }

    public List<Integer> search(double[] point) {
        BoundingBox pointBox = BoundingBox.fromPoint(point);
        return search(pointBox);
    }
}
