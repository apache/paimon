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

package org.apache.paimon.table.sink;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.*;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Objects;

import static org.apache.paimon.utils.SerializationUtils.deserializedBytes;
import static org.apache.paimon.utils.SerializationUtils.serializeBytes;

/** File committable for sink. */
public class CommitMessageImpl implements CommitMessage {

    private static final long serialVersionUID = 1L;

    private static final ThreadLocal<CommitMessageSerializer> CACHE =
            ThreadLocal.withInitial(CommitMessageSerializer::new);

    private transient BinaryRow partition;
    private transient int bucket;
    private transient @Nullable Integer totalBuckets;
    private transient DataIncrement dataIncrement;
    private transient CompactIncrement compactIncrement;
    private transient @Nullable CompactMetricIncrement compactMetricIncrement;

    public CommitMessageImpl(
            BinaryRow partition,
            int bucket,
            @Nullable Integer totalBuckets,
            DataIncrement dataIncrement,
            CompactIncrement compactIncrement) {
        this(partition, bucket, totalBuckets, dataIncrement, compactIncrement, null);
    }

    public CommitMessageImpl(
            BinaryRow partition,
            int bucket,
            @Nullable Integer totalBuckets,
            DataIncrement dataIncrement,
            CompactIncrement compactIncrement,
            @Nullable CompactMetricIncrement compactMetricIncrement) {
        this.partition = partition;
        this.bucket = bucket;
        this.totalBuckets = totalBuckets;
        this.dataIncrement = dataIncrement;
        this.compactIncrement = compactIncrement;
        this.compactMetricIncrement = compactMetricIncrement;
    }

    @Override
    public BinaryRow partition() {
        return partition;
    }

    @Override
    public int bucket() {
        return bucket;
    }

    @Override
    public @Nullable Integer totalBuckets() {
        return totalBuckets;
    }

    public DataIncrement newFilesIncrement() {
        return dataIncrement;
    }

    public CompactIncrement compactIncrement() {
        return compactIncrement;
    }

    public CompactMetricIncrement compactMetricIncrement() {
        return compactMetricIncrement;
    }

    public boolean isEmpty() {
        return dataIncrement.isEmpty() && compactIncrement.isEmpty();
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();
        CommitMessageSerializer serializer = CACHE.get();
        out.writeInt(serializer.getVersion());
        serializeBytes(new DataOutputViewStreamWrapper(out), serializer.serialize(this));
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        int version = in.readInt();
        byte[] bytes = deserializedBytes(new DataInputViewStreamWrapper(in));
        CommitMessageImpl message = (CommitMessageImpl) CACHE.get().deserialize(version, bytes);
        this.partition = message.partition;
        this.bucket = message.bucket;
        this.totalBuckets = message.totalBuckets;
        this.dataIncrement = message.dataIncrement;
        this.compactIncrement = message.compactIncrement;
        this.compactMetricIncrement = message.compactMetricIncrement;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        CommitMessageImpl that = (CommitMessageImpl) o;
        return bucket == that.bucket
                && Objects.equals(partition, that.partition)
                && Objects.equals(totalBuckets, that.totalBuckets)
                && Objects.equals(dataIncrement, that.dataIncrement)
                && Objects.equals(compactIncrement, that.compactIncrement)
                && Objects.equals(compactMetricIncrement, that.compactMetricIncrement);
    }

    @Override
    public int hashCode() {
        return Objects.hash(partition, bucket, totalBuckets, dataIncrement, compactIncrement, compactMetricIncrement);
    }

    @Override
    public String toString() {
        return String.format(
                "FileCommittable {"
                        + "partition = %s, "
                        + "bucket = %d, "
                        + "totalBuckets = %s, "
                        + "newFilesIncrement = %s, "
                        + "compactIncrement = %s, "
                        + "compactMetricIncrement = $s}",
                partition, bucket, totalBuckets, dataIncrement, compactIncrement, compactMetricIncrement);
    }
}
