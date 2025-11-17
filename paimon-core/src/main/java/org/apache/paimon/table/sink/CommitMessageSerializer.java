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
import org.apache.paimon.data.serializer.VersionedSerializer;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.IndexFileMetaSerializer;
import org.apache.paimon.index.IndexFileMetaV1Deserializer;
import org.apache.paimon.index.IndexFileMetaV2Deserializer;
import org.apache.paimon.index.IndexFileMetaV3Deserializer;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFileMeta08Serializer;
import org.apache.paimon.io.DataFileMeta09Serializer;
import org.apache.paimon.io.DataFileMeta10LegacySerializer;
import org.apache.paimon.io.DataFileMeta12LegacySerializer;
import org.apache.paimon.io.DataFileMetaFirstRowIdLegacySerializer;
import org.apache.paimon.io.DataFileMetaSerializer;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.io.DataInputDeserializer;
import org.apache.paimon.io.DataInputView;
import org.apache.paimon.io.DataOutputView;
import org.apache.paimon.io.DataOutputViewStreamWrapper;
import org.apache.paimon.utils.IOExceptionSupplier;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.paimon.utils.SerializationUtils.deserializeBinaryRow;
import static org.apache.paimon.utils.SerializationUtils.serializeBinaryRow;

/** {@link VersionedSerializer} for {@link CommitMessage}. */
public class CommitMessageSerializer implements VersionedSerializer<CommitMessage> {

    public static final int CURRENT_VERSION = 11;

    private final DataFileMetaSerializer dataFileSerializer;
    private final IndexFileMetaSerializer indexEntrySerializer;

    private DataFileMetaFirstRowIdLegacySerializer dataFileMetaFirstRowIdLegacySerializer;
    private DataFileMeta12LegacySerializer dataFileMeta12LegacySerializer;
    private DataFileMeta10LegacySerializer dataFileMeta10LegacySerializer;
    private DataFileMeta09Serializer dataFile09Serializer;
    private DataFileMeta08Serializer dataFile08Serializer;
    private IndexFileMetaV1Deserializer indexEntryV1Deserializer;
    private IndexFileMetaV2Deserializer indexEntryV2Deserializer;
    private IndexFileMetaV3Deserializer indexEntryV3Deserializer;

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

        Integer totalBuckets = obj.totalBuckets();
        if (totalBuckets != null) {
            view.writeBoolean(true);
            view.writeInt(totalBuckets);
        } else {
            view.writeBoolean(false);
        }

        // data increment
        dataFileSerializer.serializeList(message.newFilesIncrement().newFiles(), view);
        dataFileSerializer.serializeList(message.newFilesIncrement().deletedFiles(), view);
        dataFileSerializer.serializeList(message.newFilesIncrement().changelogFiles(), view);
        indexEntrySerializer.serializeList(message.newFilesIncrement().newIndexFiles(), view);
        indexEntrySerializer.serializeList(message.newFilesIncrement().deletedIndexFiles(), view);

        // compact increment
        dataFileSerializer.serializeList(message.compactIncrement().compactBefore(), view);
        dataFileSerializer.serializeList(message.compactIncrement().compactAfter(), view);
        dataFileSerializer.serializeList(message.compactIncrement().changelogFiles(), view);
        indexEntrySerializer.serializeList(message.compactIncrement().newIndexFiles(), view);
        indexEntrySerializer.serializeList(message.compactIncrement().deletedIndexFiles(), view);
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
        if (version >= 10) {
            return new CommitMessageImpl(
                    deserializeBinaryRow(view),
                    view.readInt(),
                    view.readBoolean() ? view.readInt() : null,
                    new DataIncrement(
                            fileDeserializer.get(),
                            fileDeserializer.get(),
                            fileDeserializer.get(),
                            indexEntryDeserializer.get(),
                            indexEntryDeserializer.get()),
                    new CompactIncrement(
                            fileDeserializer.get(),
                            fileDeserializer.get(),
                            fileDeserializer.get(),
                            indexEntryDeserializer.get(),
                            indexEntryDeserializer.get()));
        } else {
            BinaryRow partition = deserializeBinaryRow(view);
            int bucket = view.readInt();
            Integer totalBuckets = version >= 7 && view.readBoolean() ? view.readInt() : null;
            DataIncrement dataIncrement =
                    new DataIncrement(
                            fileDeserializer.get(), fileDeserializer.get(), fileDeserializer.get());
            CompactIncrement compactIncrement =
                    new CompactIncrement(
                            fileDeserializer.get(), fileDeserializer.get(), fileDeserializer.get());
            if (compactIncrement.isEmpty()) {
                dataIncrement.newIndexFiles().addAll(indexEntryDeserializer.get());
            } else {
                compactIncrement.newIndexFiles().addAll(indexEntryDeserializer.get());
            }
            if (version > 2) {
                if (compactIncrement.isEmpty()) {
                    dataIncrement.deletedIndexFiles().addAll(indexEntryDeserializer.get());
                } else {
                    compactIncrement.deletedIndexFiles().addAll(indexEntryDeserializer.get());
                }
            }
            return new CommitMessageImpl(
                    partition, bucket, totalBuckets, dataIncrement, compactIncrement);
        }
    }

    private IOExceptionSupplier<List<DataFileMeta>> fileDeserializer(
            int version, DataInputView view) {
        if (version >= 9) {
            return () -> dataFileSerializer.deserializeList(view);
        } else if (version == 8) {
            if (dataFileMetaFirstRowIdLegacySerializer == null) {
                dataFileMetaFirstRowIdLegacySerializer =
                        new DataFileMetaFirstRowIdLegacySerializer();
            }
            return () -> dataFileMetaFirstRowIdLegacySerializer.deserializeList(view);
        } else if (version == 6 || version == 7) {
            if (dataFileMeta12LegacySerializer == null) {
                dataFileMeta12LegacySerializer = new DataFileMeta12LegacySerializer();
            }
            return () -> dataFileMeta12LegacySerializer.deserializeList(view);
        } else if (version == 4 || version == 5) {
            if (dataFileMeta10LegacySerializer == null) {
                dataFileMeta10LegacySerializer = new DataFileMeta10LegacySerializer();
            }
            return () -> dataFileMeta10LegacySerializer.deserializeList(view);
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
        if (version >= 11) {
            return () -> indexEntrySerializer.deserializeList(view);
        } else if (version >= 9) {
            if (indexEntryV3Deserializer == null) {
                indexEntryV3Deserializer = new IndexFileMetaV3Deserializer();
            }
            return () -> indexEntryV3Deserializer.deserializeList(view);
        } else if (version >= 5) {
            if (indexEntryV2Deserializer == null) {
                indexEntryV2Deserializer = new IndexFileMetaV2Deserializer();
            }
            return () -> indexEntryV2Deserializer.deserializeList(view);
        } else {
            if (indexEntryV1Deserializer == null) {
                indexEntryV1Deserializer = new IndexFileMetaV1Deserializer();
            }
            return () -> indexEntryV1Deserializer.deserializeList(view);
        }
    }
}
