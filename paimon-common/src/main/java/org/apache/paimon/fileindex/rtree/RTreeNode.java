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

/** Represents a node in the R-Tree structure. */
public class RTreeNode {
    private final BoundingBox boundingBox;
    private final List<RTreeNode> children;
    private final List<Integer> leafRowIds;
    private final List<LeafEntry> leafEntries;
    private final boolean isLeaf;
    private final int maxEntries;
    private RTreeNode parent;

    public RTreeNode(int dimensions, int maxEntries, boolean isLeaf) {
        this.boundingBox = new BoundingBox(dimensions);
        this.children = new ArrayList<>();
        this.leafRowIds = new ArrayList<>();
        this.leafEntries = new ArrayList<>();
        this.isLeaf = isLeaf;
        this.maxEntries = maxEntries;
    }

    public BoundingBox getBoundingBox() {
        return boundingBox;
    }

    public List<RTreeNode> getChildren() {
        return children;
    }

    public List<Integer> getLeafRowIds() {
        return leafRowIds;
    }

    public boolean isLeaf() {
        return isLeaf;
    }

    public int getMaxEntries() {
        return maxEntries;
    }

    public void addChild(RTreeNode child) {
        children.add(child);
        child.setParent(this);
        boundingBox.expand(child.getBoundingBox());
    }

    public void addRowId(int rowId) {
        leafRowIds.add(rowId);
    }

    public void addLeafEntry(LeafEntry entry) {
        leafEntries.add(entry);
        leafRowIds.add(entry.getRowId());
        boundingBox.expand(entry.getBbox());
    }

    public List<LeafEntry> getLeafEntries() {
        return leafEntries;
    }

    public RTreeNode getParent() {
        return parent;
    }

    public void setParent(RTreeNode parent) {
        this.parent = parent;
    }

    public int getEntryCount() {
        return isLeaf ? leafRowIds.size() : children.size();
    }

    public boolean isFull() {
        return getEntryCount() >= maxEntries;
    }

    public boolean canSplit() {
        return getEntryCount() > maxEntries;
    }
}
