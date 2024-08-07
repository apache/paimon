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

package org.apache.paimon.utils;

import java.io.ByteArrayOutputStream;

/** A {@link ByteArrayOutputStream} which can reuse byte array. */
public class FixLenByteArrayOutputStream {

    private byte[] buf;
    private int count;

    public void setBuffer(byte[] buffer) {
        this.buf = buffer;
    }

    public byte[] getBuffer() {
        return buf;
    }

    public int write(byte[] b, int off, int len) {
        if ((off < 0) || (off > b.length) || (len < 0) || ((off + len) - b.length > 0)) {
            throw new IndexOutOfBoundsException();
        }
        int writeLen = Math.min(len, buf.length - count);
        System.arraycopy(b, off, buf, count, writeLen);
        count += writeLen;
        return writeLen;
    }

    public int getCount() {
        return count;
    }

    public int write(byte b) {
        if (count < buf.length) {
            buf[count] = b;
            count += 1;
            return 1;
        }
        return 0;
    }

    public void setCount(int count) {
        this.count = count;
    }
}
