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

import org.apache.paimon.data.serializer.VersionedSerializer;
import org.apache.paimon.fs.TwoPhaseOutputStream;
import org.apache.paimon.io.DataInputDeserializer;
import org.apache.paimon.io.DataOutputViewStreamWrapper;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/** {@link VersionedSerializer} for {@link TwoPhaseOutputStream.Committer}. */
public class TwoPhaseCommitterSerializer
        implements VersionedSerializer<TwoPhaseOutputStream.Committer> {

    public static final int CURRENT_VERSION = 1;

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(TwoPhaseOutputStream.Committer obj) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputViewStreamWrapper view = new DataOutputViewStreamWrapper(out);
        TwoPhaseOutputStream.Committer committer = obj;
        ByteArrayOutputStream committerOut = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(committerOut)) {
            oos.writeObject(committer);
        }
        byte[] committerBytes = committerOut.toByteArray();
        view.writeInt(committerBytes.length);
        view.write(committerBytes);

        return out.toByteArray();
    }

    @Override
    public TwoPhaseOutputStream.Committer deserialize(int version, byte[] serialized)
            throws IOException {
        if (version > CURRENT_VERSION) {
            throw new UnsupportedOperationException(
                    "Expecting TwoPhaseCommitterSerializer version to be smaller or equal than "
                            + CURRENT_VERSION
                            + ", but found "
                            + version
                            + ".");
        }

        DataInputDeserializer view = new DataInputDeserializer(serialized);

        int committerLength = view.readInt();
        byte[] committerBytes = new byte[committerLength];
        view.readFully(committerBytes);
        TwoPhaseOutputStream.Committer committer;
        try (ObjectInputStream ois =
                new ObjectInputStream(new java.io.ByteArrayInputStream(committerBytes))) {
            committer = (TwoPhaseOutputStream.Committer) ois.readObject();
        } catch (ClassNotFoundException e) {
            throw new IOException("Failed to deserialize TwoPhaseOutputStream.Committer", e);
        }

        return committer;
    }
}
