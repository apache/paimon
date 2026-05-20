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

import org.apache.paimon.fileindex.FileIndexWriter;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DoubleType;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/** Writer for R-Tree file index. */
public class RTreeFileIndexWriter extends FileIndexWriter {

    private final DataType dataType;
    private final Options options;
    private final RTree rtree;
    private final int dimensions;
    private final int maxEntries;
    private int rowNumber;

    public RTreeFileIndexWriter(DataType dataType, Options options) {
        this.dataType = dataType;
        this.options = options;
        this.dimensions =
                options.getInteger(RTreeFileIndex.DIMENSIONS, RTreeFileIndex.DEFAULT_DIMENSIONS);
        this.maxEntries =
                options.getInteger(RTreeFileIndex.MAX_ENTRIES, RTreeFileIndex.DEFAULT_MAX_ENTRIES);
        this.rtree = new RTree(dimensions, maxEntries);
        this.rowNumber = 0;

        validateDataType();
    }

    private void validateDataType() {
        if (!(dataType instanceof ArrayType)) {
            throw new RuntimeException("RTree index only supports ARRAY type, got: " + dataType);
        }
        ArrayType arrayType = (ArrayType) dataType;
        if (!(arrayType.getElementType() instanceof DoubleType)) {
            throw new RuntimeException("RTree index requires ARRAY<DOUBLE>, got: " + dataType);
        }
    }

    @Override
    public void write(Object key) {
        if (key == null) {
            rowNumber++;
            return;
        }

        try {
            double[] point = extractPoint(key);
            if (point.length != dimensions) {
                throw new RuntimeException(
                        String.format("Expected %d dimensions, got %d", dimensions, point.length));
            }
            rtree.insert(point, rowNumber);
            rowNumber++;
        } catch (Exception e) {
            throw new RuntimeException("Error writing R-Tree index: " + e.getMessage(), e);
        }
    }

    private double[] extractPoint(Object key) {
        if (key instanceof java.util.List) {
            java.util.List<?> list = (java.util.List<?>) key;
            double[] point = new double[list.size()];
            for (int i = 0; i < list.size(); i++) {
                Object val = list.get(i);
                if (val instanceof Number) {
                    point[i] = ((Number) val).doubleValue();
                } else {
                    throw new RuntimeException("Expected number, got: " + val.getClass());
                }
            }
            return point;
        } else if (key instanceof double[]) {
            return (double[]) key;
        } else {
            throw new RuntimeException("Cannot extract point from: " + key.getClass().getName());
        }
    }

    @Override
    public byte[] serializedBytes() {
        try {
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(output);

            serializeRTree(dos);

            dos.flush();
            return output.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Error serializing R-Tree: " + e.getMessage(), e);
        }
    }

    private void serializeRTree(DataOutputStream dos) throws IOException {
        dos.writeInt(dimensions);
        dos.writeInt(maxEntries);
        dos.writeInt(rtree.getSize());

        if (rtree.getSize() > 0) {
            serializeNode(rtree.getRoot(), dos);
        }
    }

    private void serializeNode(RTreeNode node, DataOutputStream dos) throws IOException {
        // Write node metadata
        dos.writeBoolean(node.isLeaf());
        dos.writeInt(node.getEntryCount());

        // Write bounding box
        BoundingBox bbox = node.getBoundingBox();
        serializeBoundingBox(bbox, dos);

        // Write entries
        if (node.isLeaf()) {
            for (Integer rowId : node.getLeafRowIds()) {
                dos.writeInt(rowId);
            }
        } else {
            for (RTreeNode child : node.getChildren()) {
                serializeNode(child, dos);
            }
        }
    }

    private void serializeBoundingBox(BoundingBox bbox, DataOutputStream dos) throws IOException {
        bbox.serialize(dos);
    }
}
