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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/** Small explicit OGC WKB fixtures, independent of the envelope reader. */
public final class WkbTestUtils {

    private WkbTestUtils() {}

    public static byte[] point(double x, double y) {
        return point(x, y, ByteOrder.LITTLE_ENDIAN);
    }

    public static byte[] point(double x, double y, ByteOrder order) {
        return header(21, 1, order).putDouble(x).putDouble(y).array();
    }

    public static byte[] line(double... coordinates) {
        ByteBuffer buffer = header(9 + coordinates.length * 8, 2, ByteOrder.LITTLE_ENDIAN);
        buffer.putInt(coordinates.length / 2);
        for (double coordinate : coordinates) {
            buffer.putDouble(coordinate);
        }
        return buffer.array();
    }

    public static byte[] polygon(double[]... rings) {
        int length = 9;
        for (double[] ring : rings) {
            length += 4 + ring.length * 8;
        }
        ByteBuffer buffer = header(length, 3, ByteOrder.LITTLE_ENDIAN).putInt(rings.length);
        for (double[] ring : rings) {
            buffer.putInt(ring.length / 2);
            for (double coordinate : ring) {
                buffer.putDouble(coordinate);
            }
        }
        return buffer.array();
    }

    public static byte[] collection(int type, byte[]... members) {
        int length = 9;
        for (byte[] member : members) {
            length += member.length;
        }
        ByteBuffer buffer = header(length, type, ByteOrder.BIG_ENDIAN).putInt(members.length);
        for (byte[] member : members) {
            buffer.put(member);
        }
        return buffer.array();
    }

    private static ByteBuffer header(int length, int type, ByteOrder order) {
        return ByteBuffer.allocate(length)
                .order(order)
                .put((byte) (order == ByteOrder.BIG_ENDIAN ? 0 : 1))
                .putInt(type);
    }
}
