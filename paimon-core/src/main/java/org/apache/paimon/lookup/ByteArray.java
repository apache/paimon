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

package org.apache.paimon.lookup;

import org.apache.paimon.utils.SortUtil;

import java.util.Arrays;

/** A class wraps byte[] to implement equals and hashCode. */
public class ByteArray implements Comparable<ByteArray> {

    public final byte[] bytes;

    public ByteArray(byte[] bytes) {
        this.bytes = bytes;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(bytes);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ByteArray byteArray = (ByteArray) o;
        return Arrays.equals(bytes, byteArray.bytes);
    }

    public static ByteArray wrapBytes(byte[] bytes) {
        return new ByteArray(bytes);
    }

    @Override
    public int compareTo(ByteArray o) {
        return SortUtil.compareBinary(bytes, o.bytes);
    }
}
