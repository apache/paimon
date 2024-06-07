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

package org.apache.paimon.table.source;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFileMeta08Serializer;
import org.apache.paimon.io.DataFileTestDataGenerator;
import org.apache.paimon.io.DataInputDeserializer;
import org.apache.paimon.io.DataInputView;
import org.apache.paimon.io.DataInputViewStreamWrapper;
import org.apache.paimon.io.DataOutputView;
import org.apache.paimon.io.DataOutputViewStreamWrapper;
import org.apache.paimon.utils.InstantiationUtil;
import org.apache.paimon.utils.SerializationUtils;

import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link DataSplit}. */
public class SplitTest {

    @Test
    public void testSerializer() throws IOException {
        DataFileTestDataGenerator gen = DataFileTestDataGenerator.builder().build();
        DataFileTestDataGenerator.Data data = gen.next();
        List<DataFileMeta> files = new ArrayList<>();
        for (int i = 0; i < ThreadLocalRandom.current().nextInt(10); i++) {
            files.add(gen.next().meta);
        }
        DataSplit split =
                DataSplit.builder()
                        .withSnapshot(ThreadLocalRandom.current().nextLong(100))
                        .withPartition(data.partition)
                        .withBucket(data.bucket)
                        .withDataFiles(files)
                        .withBucketPath("my path")
                        .build();

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        split.serialize(new DataOutputViewStreamWrapper(out));

        DataSplit newSplit = DataSplit.deserialize(new DataInputDeserializer(out.toByteArray()));
        assertThat(newSplit).isEqualTo(split);
    }

    @Test
    public void testSerializerCompatible() throws IOException {
        DataFileTestDataGenerator gen = DataFileTestDataGenerator.builder().build();
        DataFileTestDataGenerator.Data data = gen.next();
        List<DataFileMeta> files = new ArrayList<>();
        List<DataFileMeta> files2 = new ArrayList<>();
        for (int i = 0; i < ThreadLocalRandom.current().nextInt(10); i++) {
            DataFileMeta meta = gen.next().meta;
            files.add(meta);
            files2.add(
                    new DataFileMeta(
                            meta.fileName(),
                            meta.fileSize(),
                            meta.rowCount(),
                            meta.minKey(),
                            meta.maxKey(),
                            meta.keyStats(),
                            meta.valueStats(),
                            meta.minSequenceNumber(),
                            meta.maxSequenceNumber(),
                            meta.schemaId(),
                            meta.level(),
                            meta.extraFiles(),
                            meta.creationTime(),
                            meta.deleteRowCount().orElse(null),
                            meta.embeddedIndex(),
                            null));
        }
        DataSplit split =
                DataSplit.builder()
                        .withSnapshot(ThreadLocalRandom.current().nextLong(100))
                        .withPartition(data.partition)
                        .withBucket(data.bucket)
                        .withDataFiles(files)
                        .withBucketPath("my path")
                        .build();

        DataSplit split2 =
                DataSplit.builder()
                        .withSnapshot(split.snapshotId())
                        .withPartition(data.partition)
                        .withBucket(data.bucket)
                        .withDataFiles(files2)
                        .withBucketPath("my path")
                        .build();

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        split.serialize08(new DataOutputViewStreamWrapper(out));

        DataSplit newSplit = DataSplit.deserialize(new DataInputDeserializer(out.toByteArray()));
        assertThat(newSplit).isEqualTo(split2);
    }

    @Test
    public void testSerializerCompatibleStrictly() throws Exception {
        DataFileTestDataGenerator gen = DataFileTestDataGenerator.builder().build();
        DataFileTestDataGenerator.Data data = gen.next();
        List<DataFileMeta> files = new ArrayList<>();
        List<DataFileMeta> files2 = new ArrayList<>();
        for (int i = 0; i < ThreadLocalRandom.current().nextInt(10); i++) {
            DataFileMeta meta = gen.next().meta;
            files.add(meta);
            files2.add(
                    new DataFileMeta(
                            meta.fileName(),
                            meta.fileSize(),
                            meta.rowCount(),
                            meta.minKey(),
                            meta.maxKey(),
                            meta.keyStats(),
                            meta.valueStats(),
                            meta.minSequenceNumber(),
                            meta.maxSequenceNumber(),
                            meta.schemaId(),
                            meta.level(),
                            meta.extraFiles(),
                            meta.creationTime(),
                            meta.deleteRowCount().orElse(null),
                            meta.embeddedIndex(),
                            null));
        }
        long snapshotId = ThreadLocalRandom.current().nextLong(100);
        DataSplitWith08SerializerAnd09Deserializer split =
                new DataSplitWith08SerializerAnd09Deserializer(
                        snapshotId, data.partition, data.bucket, "my path", files);
        DataSplitWith08SerializerAnd09Deserializer split2 =
                new DataSplitWith08SerializerAnd09Deserializer(
                        snapshotId, data.partition, data.bucket, "my path", files2);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputViewStreamWrapper view = new DataOutputViewStreamWrapper(out);
        InstantiationUtil.serializeObject(view, split);
        DataSplitWith08SerializerAnd09Deserializer actual =
                InstantiationUtil.deserializeObject(
                        out.toByteArray(), SplitTest.class.getClassLoader());
        assertThat(actual).isEqualTo(split2);
    }

    /** To test compatible. DataSplit with 08 version serializer and 09 version deserializer. */
    private static class DataSplitWith08SerializerAnd09Deserializer implements Split {

        private static final long serialVersionUID = 7L;

        private long snapshotId;
        private BinaryRow partition;
        private int bucket;
        private String bucketPath;

        private List<DataFileMeta> beforeFiles = new ArrayList<>();
        @Nullable private List<DeletionFile> beforeDeletionFiles;

        private List<DataFileMeta> dataFiles;
        @Nullable private List<DeletionFile> dataDeletionFiles;

        private boolean isStreaming = false;
        private boolean rawConvertible;

        public DataSplitWith08SerializerAnd09Deserializer(
                long snapshotId,
                BinaryRow partition,
                int bucket,
                String bucketPath,
                List<DataFileMeta> dataFiles) {
            this.snapshotId = snapshotId;
            this.partition = partition;
            this.bucket = bucket;
            this.bucketPath = bucketPath;
            this.dataFiles = dataFiles;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            DataSplitWith08SerializerAnd09Deserializer dataSplit =
                    (DataSplitWith08SerializerAnd09Deserializer) o;
            return snapshotId == dataSplit.snapshotId
                    && bucket == dataSplit.bucket
                    && isStreaming == dataSplit.isStreaming
                    && rawConvertible == dataSplit.rawConvertible
                    && Objects.equals(partition, dataSplit.partition)
                    && Objects.equals(bucketPath, dataSplit.bucketPath)
                    && Objects.equals(beforeFiles, dataSplit.beforeFiles)
                    && Objects.equals(beforeDeletionFiles, dataSplit.beforeDeletionFiles)
                    && Objects.equals(dataFiles, dataSplit.dataFiles)
                    && Objects.equals(dataDeletionFiles, dataSplit.dataDeletionFiles);
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                    snapshotId,
                    partition,
                    bucket,
                    bucketPath,
                    beforeFiles,
                    beforeDeletionFiles,
                    dataFiles,
                    dataDeletionFiles,
                    isStreaming,
                    rawConvertible);
        }

        private void writeObject(ObjectOutputStream out) throws IOException {
            serialize(new DataOutputViewStreamWrapper(out));
        }

        private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
            assign(deserialize(new DataInputViewStreamWrapper(in)));
        }

        private void assign(DataSplit other) {
            this.snapshotId = other.snapshotId();
            this.partition = other.partition();
            this.bucket = other.bucket();
            this.bucketPath = other.bucketPath();
            this.beforeFiles = other.beforeFiles();
            this.beforeDeletionFiles = other.beforeDeletionFiles().orElse(null);
            this.dataFiles = other.dataFiles();
            this.dataDeletionFiles = other.deletionFiles().orElse(null);
            this.isStreaming = other.isStreaming();
            this.rawConvertible = other.rawConvertible();
        }

        // 08 serialize method
        public void serialize(DataOutputView out) throws IOException {
            out.writeLong(snapshotId);
            SerializationUtils.serializeBinaryRow(partition, out);
            out.writeInt(bucket);
            out.writeUTF(bucketPath);

            DataFileMeta08Serializer dataFileSer = new DataFileMeta08Serializer();
            out.writeInt(beforeFiles.size());
            for (DataFileMeta file : beforeFiles) {
                dataFileSer.serialize(file, out);
            }

            DeletionFile.serializeList(out, beforeDeletionFiles);

            out.writeInt(dataFiles.size());
            for (DataFileMeta file : dataFiles) {
                dataFileSer.serialize(file, out);
            }

            DeletionFile.serializeList(out, dataDeletionFiles);

            out.writeBoolean(isStreaming);

            out.writeBoolean(rawConvertible);
        }

        // 09 desrialize method
        public static DataSplit deserialize(DataInputView in) throws IOException {
            return DataSplit.deserialize(in);
        }

        @Override
        public long rowCount() {
            return -1;
        }
    }
}
