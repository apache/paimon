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

package org.apache.flink.table.store.table.sink;

import org.apache.flink.table.store.io.DataInputViewStreamWrapper;
import org.apache.flink.table.store.io.DataOutputViewStreamWrapper;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import static org.apache.flink.table.store.file.utils.SerializationUtils.deserializedBytes;
import static org.apache.flink.table.store.file.utils.SerializationUtils.serializeBytes;

/** A serializable {@link FileCommittable}. */
public class SerializableCommittable implements Serializable {

    private static final ThreadLocal<FileCommittableSerializer> CACHE =
            ThreadLocal.withInitial(FileCommittableSerializer::new);

    private transient FileCommittable committable;

    public FileCommittable delegate() {
        return committable;
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();
        FileCommittableSerializer serializer = CACHE.get();
        out.writeInt(serializer.getVersion());
        serializeBytes(new DataOutputViewStreamWrapper(out), serializer.serialize(committable));
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        int version = in.readInt();
        byte[] bytes = deserializedBytes(new DataInputViewStreamWrapper(in));
        committable = CACHE.get().deserialize(version, bytes);
    }

    public static SerializableCommittable wrap(FileCommittable committable) {
        SerializableCommittable ret = new SerializableCommittable();
        ret.committable = committable;
        return ret;
    }
}
