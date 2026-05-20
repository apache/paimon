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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;

/** Represents an axis-aligned bounding box (AABB) for spatial indexing. */
public class BoundingBox {
    private final double[] min;
    private final double[] max;
    private final int dimensions;

    public BoundingBox(double[] min, double[] max) {
        if (min.length != max.length) {
            throw new IllegalArgumentException("Min and max must have same dimensions");
        }
        this.min = min.clone();
        this.max = max.clone();
        this.dimensions = min.length;
    }

    public BoundingBox(int dimensions) {
        this.min = new double[dimensions];
        this.max = new double[dimensions];
        this.dimensions = dimensions;
        java.util.Arrays.fill(min, Double.POSITIVE_INFINITY);
        java.util.Arrays.fill(max, Double.NEGATIVE_INFINITY);
    }

    public static BoundingBox fromPoint(double[] point) {
        return new BoundingBox(point, point);
    }

    public int getDimensions() {
        return dimensions;
    }

    public double[] getMin() {
        return min;
    }

    public double[] getMax() {
        return max;
    }

    public void expand(BoundingBox other) {
        for (int i = 0; i < dimensions; i++) {
            min[i] = Math.min(min[i], other.min[i]);
            max[i] = Math.max(max[i], other.max[i]);
        }
    }

    public void expand(double[] point) {
        for (int i = 0; i < dimensions; i++) {
            min[i] = Math.min(min[i], point[i]);
            max[i] = Math.max(max[i], point[i]);
        }
    }

    public void clear() {
        java.util.Arrays.fill(min, Double.POSITIVE_INFINITY);
        java.util.Arrays.fill(max, Double.NEGATIVE_INFINITY);
    }

    public double getArea() {
        double area = 1.0;
        for (int i = 0; i < dimensions; i++) {
            area *= (max[i] - min[i]);
        }
        return area;
    }

    public double getMargin() {
        double margin = 0.0;
        for (int i = 0; i < dimensions; i++) {
            margin += 2 * (max[i] - min[i]);
        }
        return margin;
    }

    public double getExpansionArea(BoundingBox other) {
        double newArea = 1.0;
        for (int i = 0; i < dimensions; i++) {
            newArea *= (Math.max(max[i], other.max[i]) - Math.min(min[i], other.min[i]));
        }
        return newArea - getArea();
    }

    public boolean intersects(BoundingBox other) {
        for (int i = 0; i < dimensions; i++) {
            if (max[i] < other.min[i] || min[i] > other.max[i]) {
                return false;
            }
        }
        return true;
    }

    public boolean contains(double[] point) {
        for (int i = 0; i < dimensions; i++) {
            if (point[i] < min[i] || point[i] > max[i]) {
                return false;
            }
        }
        return true;
    }

    public boolean contains(BoundingBox other) {
        for (int i = 0; i < dimensions; i++) {
            if (other.min[i] < min[i] || other.max[i] > max[i]) {
                return false;
            }
        }
        return true;
    }

    public void serialize(DataOutputStream dos) throws IOException {
        for (int i = 0; i < dimensions; i++) {
            dos.writeDouble(min[i]);
        }
        for (int i = 0; i < dimensions; i++) {
            dos.writeDouble(max[i]);
        }
    }

    public static BoundingBox deserialize(DataInputStream dis, int dimensions) throws IOException {
        double[] min = new double[dimensions];
        double[] max = new double[dimensions];
        for (int i = 0; i < dimensions; i++) {
            min[i] = dis.readDouble();
        }
        for (int i = 0; i < dimensions; i++) {
            max[i] = dis.readDouble();
        }
        return new BoundingBox(min, max);
    }

    @Override
    public String toString() {
        return String.format(
                "BoundingBox(min=%s, max=%s)", Arrays.toString(min), Arrays.toString(max));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BoundingBox that = (BoundingBox) o;
        return Arrays.equals(min, that.min) && Arrays.equals(max, that.max);
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(min);
        result = 31 * result + Arrays.hashCode(max);
        return result;
    }
}
