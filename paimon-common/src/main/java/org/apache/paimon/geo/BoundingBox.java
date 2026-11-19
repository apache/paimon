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

package org.apache.paimon.geo;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** An axis-aligned XY envelope. Empty geometries have an empty envelope. */
public final class BoundingBox implements Serializable {

    private static final long serialVersionUID = 1L;

    private final double minX;
    private final double minY;
    private final double maxX;
    private final double maxY;

    private BoundingBox(double minX, double minY, double maxX, double maxY) {
        this.minX = minX;
        this.minY = minY;
        this.maxX = maxX;
        this.maxY = maxY;
    }

    public static BoundingBox of(double minX, double minY, double maxX, double maxY) {
        checkArgument(
                Double.isFinite(minX)
                        && Double.isFinite(minY)
                        && Double.isFinite(maxX)
                        && Double.isFinite(maxY)
                        && minX <= maxX
                        && minY <= maxY,
                "Bounding box coordinates must be finite and ordered.");
        return new BoundingBox(minX, minY, maxX, maxY);
    }

    public static BoundingBox empty() {
        return new BoundingBox(
                Double.POSITIVE_INFINITY,
                Double.POSITIVE_INFINITY,
                Double.NEGATIVE_INFINITY,
                Double.NEGATIVE_INFINITY);
    }

    /**
     * Reads an envelope from 2D OGC WKB, including nested collections and mixed byte orders.
     * Extended WKB, Z/M coordinates, malformed input and non-finite coordinates are rejected. An
     * empty point is represented by two NaNs.
     */
    public static BoundingBox fromWkb(byte[] wkb) {
        WkbReader reader = new WkbReader(wkb);
        reader.geometry(0, 0);
        checkArgument(!reader.input.hasRemaining(), "Trailing bytes in WKB geometry.");
        return new BoundingBox(reader.minX, reader.minY, reader.maxX, reader.maxY);
    }

    public double minX() {
        return minX;
    }

    public double minY() {
        return minY;
    }

    public double maxX() {
        return maxX;
    }

    public double maxY() {
        return maxY;
    }

    public boolean isEmpty() {
        return minX > maxX;
    }

    public boolean intersects(BoundingBox other) {
        return !isEmpty()
                && !other.isEmpty()
                && minX <= other.maxX
                && maxX >= other.minX
                && minY <= other.maxY
                && maxY >= other.minY;
    }

    public BoundingBox union(BoundingBox other) {
        if (isEmpty()) {
            return other;
        }
        if (other.isEmpty()) {
            return this;
        }
        return new BoundingBox(
                Math.min(minX, other.minX),
                Math.min(minY, other.minY),
                Math.max(maxX, other.maxX),
                Math.max(maxY, other.maxY));
    }

    private static final class WkbReader {
        private final ByteBuffer input;
        private double minX = Double.POSITIVE_INFINITY;
        private double minY = Double.POSITIVE_INFINITY;
        private double maxX = Double.NEGATIVE_INFINITY;
        private double maxY = Double.NEGATIVE_INFINITY;

        private WkbReader(byte[] wkb) {
            checkArgument(wkb != null, "WKB must not be null.");
            input = ByteBuffer.wrap(wkb);
        }

        private void geometry(int expectedType, int depth) {
            checkArgument(depth <= 64, "WKB collection nesting exceeds 64.");
            require(5);
            byte order = input.get();
            checkArgument(order == 0 || order == 1, "Invalid WKB byte order.");
            input.order(order == 0 ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN);
            int type = input.getInt();
            checkArgument(
                    type >= 1 && type <= 7, "Only 2D OGC WKB types 1 through 7 are supported.");
            checkArgument(
                    expectedType == 0 || expectedType == type,
                    "Invalid WKB multi-geometry member.");
            switch (type) {
                case 1:
                    coordinate(true);
                    break;
                case 2:
                    coordinates();
                    break;
                case 3:
                    int rings = count(4);
                    for (int i = 0; i < rings; i++) {
                        coordinates();
                    }
                    break;
                default:
                    int members = count(5);
                    for (int i = 0; i < members; i++) {
                        geometry(type == 7 ? 0 : type - 3, depth + 1);
                    }
            }
        }

        private int count(int minimumBytes) {
            require(4);
            int count = input.getInt();
            checkArgument(
                    count >= 0 && count <= input.remaining() / minimumBytes,
                    "Invalid or truncated WKB element count.");
            return count;
        }

        private void coordinates() {
            int count = count(16);
            for (int i = 0; i < count; i++) {
                coordinate(false);
            }
        }

        private void coordinate(boolean allowEmpty) {
            require(16);
            double x = input.getDouble();
            double y = input.getDouble();
            if (allowEmpty && Double.isNaN(x) && Double.isNaN(y)) {
                return;
            }
            checkArgument(
                    Double.isFinite(x) && Double.isFinite(y),
                    "Bounding box coordinates must be finite and ordered.");
            minX = Math.min(minX, x);
            minY = Math.min(minY, y);
            maxX = Math.max(maxX, x);
            maxY = Math.max(maxY, y);
        }

        private void require(int bytes) {
            checkArgument(input.remaining() >= bytes, "Truncated WKB geometry.");
        }
    }
}
