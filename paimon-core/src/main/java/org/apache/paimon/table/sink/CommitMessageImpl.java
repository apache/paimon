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

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.io.DataInputViewStreamWrapper;
import org.apache.paimon.io.DataOutputViewStreamWrapper;
import org.apache.paimon.io.IndexIncrement;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Collections;
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
    private transient DataIncrement dataIncrement;
    private transient CompactIncrement compactIncrement;
    private transient IndexIncrement indexIncrement;

    @VisibleForTesting
    public CommitMessageImpl(
            BinaryRow partition,
            int bucket,
            DataIncrement dataIncrement,
            CompactIncrement compactIncrement) {
        this(
                partition,
                bucket,
                dataIncrement,
                compactIncrement,
                new IndexIncrement(Collections.emptyList()));
    }

    public CommitMessageImpl(
            BinaryRow partition,
            int bucket,
            DataIncrement dataIncrement,
            CompactIncrement compactIncrement,
            IndexIncrement indexIncrement) {
        this.partition = partition;
        this.bucket = bucket;
        this.dataIncrement = dataIncrement;
        this.compactIncrement = compactIncrement;
        this.indexIncrement = indexIncrement;
    }

    @Override
    public BinaryRow partition() {
        return partition;
    }

    @Override
    public int bucket() {
        return bucket;
    }

    public DataIncrement newFilesIncrement() {
        return dataIncrement;
    }

    public CompactIncrement compactIncrement() {
        return compactIncrement;
    }

    public IndexIncrement indexIncrement() {
        return indexIncrement;
    }

    public boolean isEmpty() {
        return dataIncrement.isEmpty() && compactIncrement.isEmpty() && indexIncrement.isEmpty();
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
        this.dataIncrement = message.dataIncrement;
        this.compactIncrement = message.compactIncrement;
        this.indexIncrement = message.indexIncrement;
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
                && Objects.equals(dataIncrement, that.dataIncrement)
                && Objects.equals(compactIncrement, that.compactIncrement)
                && Objects.equals(indexIncrement, that.indexIncrement);
    }

    @Override
    public int hashCode() {
        return Objects.hash(partition, bucket, dataIncrement, compactIncrement, indexIncrement);
    }

    @Override
    public String toString() {
        return String.format(
                "FileCommittable {"
                        + "partition = %s, "
                        + "bucket = %d, "
                        + "newFilesIncrement = %s, "
                        + "compactIncrement = %s, "
                        + "indexIncrement = %s}",
                partition, bucket, dataIncrement, compactIncrement, indexIncrement);
    }
}
