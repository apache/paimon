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

/**
 * STR (Sort-Tile-Recursive) bulk loader for R-Tree.
 *
 * <p>STR is a highly efficient algorithm for bulk-loading R-Trees. It sorts data by the first
 * dimension, creates vertical slices (tiles), then recursively sorts and groups within each tile.
 */
public class STRBulkLoader {

    private final int dimensions;
    private final int maxEntries;

    public STRBulkLoader(int dimensions, int maxEntries) {
        this.dimensions = dimensions;
        this.maxEntries = maxEntries;
    }

    /**
     * Build an R-Tree from a list of leaf entries using STR algorithm.
     *
     * @param entries sorted leaf entries to bulk load
     * @return built R-Tree
     */
    public RTree bulkLoad(List<LeafEntry> entries) {
        RTree tree = new RTree(dimensions, maxEntries);

        if (entries.isEmpty()) {
            return tree;
        }

        if (entries.size() <= maxEntries) {
            // Single leaf node
            RTreeNode root = tree.getRoot();
            for (LeafEntry entry : entries) {
                root.addLeafEntry(entry);
            }
            tree.setSize(entries.size());
            return tree;
        }

        // Build tree recursively
        List<LeafEntry> sorted = new ArrayList<>(entries);
        RTreeNode root = buildLevel(sorted, 0);
        tree.setRoot(root);
        tree.setSize(entries.size());

        return tree;
    }

    /**
     * Recursively build levels of the tree.
     *
     * @param entries entries to organize at this level
     * @param dimension which dimension to sort by
     * @return the root node of this subtree
     */
    private RTreeNode buildLevel(List<LeafEntry> entries, int dimension) {
        if (entries.size() <= maxEntries) {
            // Create leaf node
            RTreeNode leaf = new RTreeNode(dimensions, maxEntries, true);
            for (LeafEntry entry : entries) {
                leaf.addLeafEntry(entry);
            }
            return leaf;
        }

        // Sort by current dimension
        int sortDim = dimension % dimensions;
        entries.sort(
                (a, b) ->
                        Double.compare(
                                a.getBbox().getMin()[sortDim], b.getBbox().getMin()[sortDim]));

        // Create vertical slices (tiles)
        int numTiles = (int) Math.ceil((double) entries.size() / maxEntries);
        int tileSize = (int) Math.ceil((double) entries.size() / numTiles);

        List<List<LeafEntry>> tiles = new ArrayList<>();
        for (int i = 0; i < entries.size(); i += tileSize) {
            int end = Math.min(i + tileSize, entries.size());
            tiles.add(entries.subList(i, end));
        }

        // Sort tiles along next dimension and build nodes
        List<RTreeNode> nodes = new ArrayList<>();
        for (List<LeafEntry> tile : tiles) {
            RTreeNode node = buildLevel(tile, dimension + 1);
            nodes.add(node);
        }

        return buildInternalLevel(nodes);
    }

    /** Build internal level from child nodes. */
    private RTreeNode buildInternalLevel(List<RTreeNode> nodes) {
        if (nodes.size() <= maxEntries) {
            // Create internal node
            RTreeNode internal = new RTreeNode(dimensions, maxEntries, false);
            for (RTreeNode node : nodes) {
                internal.addChild(node);
            }
            return internal;
        }

        // Recursively build higher levels
        List<RTreeNode> parentNodes = new ArrayList<>();
        for (int i = 0; i < nodes.size(); i += maxEntries) {
            int end = Math.min(i + maxEntries, nodes.size());
            List<RTreeNode> chunk = nodes.subList(i, end);

            RTreeNode parent = new RTreeNode(dimensions, maxEntries, false);
            for (RTreeNode node : chunk) {
                parent.addChild(node);
            }
            parentNodes.add(parent);
        }

        return buildInternalLevel(parentNodes);
    }
}
