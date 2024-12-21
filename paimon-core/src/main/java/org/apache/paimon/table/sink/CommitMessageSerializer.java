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
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.IndexFileMeta09Serializer;
import org.apache.paimon.index.IndexFileMetaSerializer;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFileMeta08Serializer;
import org.apache.paimon.io.DataFileMeta09Serializer;
import org.apache.paimon.io.DataFileMetaSerializer;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.io.DataInputDeserializer;
import org.apache.paimon.io.DataInputView;
import org.apache.paimon.io.DataOutputView;
import org.apache.paimon.io.DataOutputViewStreamWrapper;
import org.apache.paimon.io.IndexIncrement;
import org.apache.paimon.utils.IOExceptionSupplier;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.paimon.utils.SerializationUtils.deserializeBinaryRow;
import static org.apache.paimon.utils.SerializationUtils.serializeBinaryRow;

/** {@link VersionedSerializer} for {@link CommitMessage}. */
public class CommitMessageSerializer implements VersionedSerializer<CommitMessage> {

    private static final int CURRENT_VERSION = 5;

    private final DataFileMetaSerializer dataFileSerializer;
    private final IndexFileMetaSerializer indexEntrySerializer;

    // private DataFileMeta10Serializer dataFile10Serializer;
    private DataFileMeta09Serializer dataFile09Serializer;
    private DataFileMeta08Serializer dataFile08Serializer;
    private IndexFileMeta09Serializer indexEntry09Serializer;

    public CommitMessageSerializer() {
        this.dataFileSerializer = new DataFileMetaSerializer();
        this.indexEntrySerializer = new IndexFileMetaSerializer();
    }

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(CommitMessage obj) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputViewStreamWrapper view = new DataOutputViewStreamWrapper(out);
        serialize(obj, view);
        return out.toByteArray();
    }

    public void serializeList(List<CommitMessage> list, DataOutputView view) throws IOException {
        view.writeInt(list.size());
        for (CommitMessage commitMessage : list) {
            serialize(commitMessage, view);
        }
    }

    private void serialize(CommitMessage obj, DataOutputView view) throws IOException {
        CommitMessageImpl message = (CommitMessageImpl) obj;
        serializeBinaryRow(obj.partition(), view);
        view.writeInt(obj.bucket());
        dataFileSerializer.serializeList(message.newFilesIncrement().newFiles(), view);
        dataFileSerializer.serializeList(message.newFilesIncrement().deletedFiles(), view);
        dataFileSerializer.serializeList(message.newFilesIncrement().changelogFiles(), view);
        dataFileSerializer.serializeList(message.compactIncrement().compactBefore(), view);
        dataFileSerializer.serializeList(message.compactIncrement().compactAfter(), view);
        dataFileSerializer.serializeList(message.compactIncrement().changelogFiles(), view);
        indexEntrySerializer.serializeList(message.indexIncrement().newIndexFiles(), view);
        indexEntrySerializer.serializeList(message.indexIncrement().deletedIndexFiles(), view);
    }

    @Override
    public CommitMessage deserialize(int version, byte[] serialized) throws IOException {
        DataInputDeserializer view = new DataInputDeserializer(serialized);
        return deserialize(version, view);
    }

    public List<CommitMessage> deserializeList(int version, DataInputView view) throws IOException {
        int length = view.readInt();
        List<CommitMessage> list = new ArrayList<>(length);
        for (int i = 0; i < length; i++) {
            list.add(deserialize(version, view));
        }
        return list;
    }

    private CommitMessage deserialize(int version, DataInputView view) throws IOException {
        IOExceptionSupplier<List<DataFileMeta>> fileDeserializer = fileDeserializer(version, view);
        IOExceptionSupplier<List<IndexFileMeta>> indexEntryDeserializer =
                indexEntryDeserializer(version, view);

        return new CommitMessageImpl(
                deserializeBinaryRow(view),
                view.readInt(),
                new DataIncrement(
                        fileDeserializer.get(), fileDeserializer.get(), fileDeserializer.get()),
                new CompactIncrement(
                        fileDeserializer.get(), fileDeserializer.get(), fileDeserializer.get()),
                new IndexIncrement(
                        indexEntryDeserializer.get(),
                        version <= 2 ? Collections.emptyList() : indexEntryDeserializer.get()));
    }

    private IOExceptionSupplier<List<DataFileMeta>> fileDeserializer(
            int version, DataInputView view) {
        if (version >= 4) {
            return () -> dataFileSerializer.deserializeList(view);
        } else if (version == 3) {
            if (dataFile09Serializer == null) {
                dataFile09Serializer = new DataFileMeta09Serializer();
            }
            return () -> dataFile09Serializer.deserializeList(view);
        } else {
            if (dataFile08Serializer == null) {
                dataFile08Serializer = new DataFileMeta08Serializer();
            }
            return () -> dataFile08Serializer.deserializeList(view);
        }
    }

    private IOExceptionSupplier<List<IndexFileMeta>> indexEntryDeserializer(
            int version, DataInputView view) {
        if (version >= 5) {
            return () -> indexEntrySerializer.deserializeList(view);
        } else {
            if (indexEntry09Serializer == null) {
                indexEntry09Serializer = new IndexFileMeta09Serializer();
            }
            return () -> indexEntry09Serializer.deserializeList(view);
        }
    }
}
