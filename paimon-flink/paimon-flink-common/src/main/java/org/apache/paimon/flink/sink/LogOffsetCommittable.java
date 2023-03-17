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

package org.apache.paimon.flink.sink;

import java.nio.ByteBuffer;
import java.util.Objects;

/** Log offset committable for a bucket. */
public class LogOffsetCommittable {

    private final int bucket;

    private final long offset;

    public LogOffsetCommittable(int bucket, long offset) {
        this.bucket = bucket;
        this.offset = offset;
    }

    public int bucket() {
        return bucket;
    }

    public long offset() {
        return offset;
    }

    public byte[] toBytes() {
        return ByteBuffer.allocate(12).putInt(bucket).putLong(offset).array();
    }

    public static LogOffsetCommittable fromBytes(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        return new LogOffsetCommittable(buffer.getInt(), buffer.getLong());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LogOffsetCommittable that = (LogOffsetCommittable) o;
        return bucket == that.bucket && offset == that.offset;
    }

    @Override
    public int hashCode() {
        return Objects.hash(bucket, offset);
    }
}
