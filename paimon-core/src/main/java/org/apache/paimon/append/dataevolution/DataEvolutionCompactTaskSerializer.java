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

package org.apache.paimon.append.dataevolution;

import org.apache.paimon.data.serializer.VersionedSerializer;
import org.apache.paimon.io.DataFileMetaSerializer;
import org.apache.paimon.io.DataInputDeserializer;
import org.apache.paimon.io.DataInputView;
import org.apache.paimon.io.DataOutputView;
import org.apache.paimon.io.DataOutputViewStreamWrapper;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.paimon.utils.SerializationUtils.deserializeBinaryRow;
import static org.apache.paimon.utils.SerializationUtils.serializeBinaryRow;

/** Serializer for {@link DataEvolutionCompactTask}. */
public class DataEvolutionCompactTaskSerializer
        implements VersionedSerializer<DataEvolutionCompactTask> {

    private static final int CURRENT_VERSION = 1;

    private final DataFileMetaSerializer dataFileSerializer;

    public DataEvolutionCompactTaskSerializer() {
        this.dataFileSerializer = new DataFileMetaSerializer();
    }

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(DataEvolutionCompactTask obj) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputViewStreamWrapper view = new DataOutputViewStreamWrapper(out);
        serialize(obj, view);
        return out.toByteArray();
    }

    public void serializeList(List<DataEvolutionCompactTask> list, DataOutputView view)
            throws IOException {
        view.writeInt(list.size());
        for (DataEvolutionCompactTask task : list) {
            serialize(task, view);
        }
    }

    private void serialize(DataEvolutionCompactTask task, DataOutputView view) throws IOException {
        serializeBinaryRow(task.partition(), view);
        dataFileSerializer.serializeList(task.compactBefore(), view);
        view.writeBoolean(task.isBlobTask());
    }

    @Override
    public DataEvolutionCompactTask deserialize(int version, byte[] serialized) throws IOException {
        checkVersion(version);
        DataInputDeserializer view = new DataInputDeserializer(serialized);
        return deserialize(view);
    }

    public List<DataEvolutionCompactTask> deserializeList(int version, DataInputView view)
            throws IOException {
        checkVersion(version);
        int length = view.readInt();
        List<DataEvolutionCompactTask> list = new ArrayList<>(length);
        for (int i = 0; i < length; i++) {
            list.add(deserialize(view));
        }
        return list;
    }

    private void checkVersion(int version) {
        if (version != CURRENT_VERSION) {
            throw new UnsupportedOperationException(
                    "Expecting DataEvolutionCompactTask version to be "
                            + CURRENT_VERSION
                            + ", but found "
                            + version
                            + ".\nDataEvolutionCompactTask is not a compatible data structure. "
                            + "Please restart the job afresh (do not recover from savepoint).");
        }
    }

    private DataEvolutionCompactTask deserialize(DataInputView view) throws IOException {
        return new DataEvolutionCompactTask(
                deserializeBinaryRow(view),
                dataFileSerializer.deserializeList(view),
                view.readBoolean());
    }
}
