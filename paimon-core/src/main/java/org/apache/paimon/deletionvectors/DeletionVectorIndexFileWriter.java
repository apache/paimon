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

package org.apache.paimon.deletionvectors;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.PathFactory;
import org.apache.paimon.utils.Preconditions;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.deletionvectors.DeletionVectorsIndexFile.DELETION_VECTORS_INDEX;
import static org.apache.paimon.deletionvectors.DeletionVectorsIndexFile.VERSION_ID_V1;
import static org.apache.paimon.deletionvectors.DeletionVectorsIndexFile.calculateChecksum;

/** Writer for deletion vector index file. */
public class DeletionVectorIndexFileWriter {

    private final PathFactory indexPathFactory;
    private final FileIO fileIO;
    private final boolean isWrittenToMulitFiles;
    private final long targetSizeInBytes;

    private boolean written = false;

    private long writtenSizeInBytes = 0L;

    public DeletionVectorIndexFileWriter(
            FileIO fileIO,
            PathFactory pathFactory,
            BucketMode bucketMode,
            MemorySize targetSizePerIndexFile) {
        this.indexPathFactory = pathFactory;
        this.fileIO = fileIO;
        this.isWrittenToMulitFiles = BucketMode.BUCKET_UNAWARE == bucketMode;
        this.targetSizeInBytes = targetSizePerIndexFile.getBytes();
    }

    public List<IndexFileMeta> write(Map<String, DeletionVector> input) throws IOException {
        List<IndexFileMeta> result = new ArrayList<>();
        SingleIndexFileWriter writer = createWriter();

        for (Map.Entry<String, DeletionVector> entry : input.entrySet()) {
            String dataFile = entry.getKey();
            byte[] valueBytes = entry.getValue().serializeToBytes();
            int currentSize = valueBytes.length;

            if (isWrittenToMulitFiles
                    && written
                    && writtenSizeInBytes + currentSize > targetSizeInBytes) {
                result.add(writer.closeWriter());
                writer = createWriter();
            }

            writer.write(dataFile, valueBytes);
            written = true;
            writtenSizeInBytes += currentSize;
        }
        result.add(writer.closeWriter());
        return result;
    }

    private SingleIndexFileWriter createWriter() throws IOException {
        written = false;
        writtenSizeInBytes = 0L;
        return new SingleIndexFileWriter(fileIO, indexPathFactory.newPath());
    }

    static class SingleIndexFileWriter {
        private final FileIO fileIO;

        private final Path path;

        private final DataOutputStream dataOutputStream;

        private final LinkedHashMap<String, Pair<Integer, Integer>> dvRanges;

        public SingleIndexFileWriter(FileIO fileIO, Path path) throws IOException {
            this.fileIO = fileIO;
            this.path = path;
            this.dataOutputStream = new DataOutputStream(fileIO.newOutputStream(path, true));
            dataOutputStream.writeByte(VERSION_ID_V1);
            this.dvRanges = new LinkedHashMap<>();
        }

        public long write(String key, byte[] data) throws IOException {
            Preconditions.checkNotNull(dataOutputStream);
            int size = data.length;

            dvRanges.put(key, Pair.of(dataOutputStream.size(), size));
            dataOutputStream.writeInt(size);
            dataOutputStream.write(data);
            dataOutputStream.writeInt(calculateChecksum(data));
            return size;
        }

        public IndexFileMeta closeWriter() throws IOException {
            dataOutputStream.close();
            return new IndexFileMeta(
                    DELETION_VECTORS_INDEX,
                    path.getName(),
                    fileIO.getFileSize(path),
                    dvRanges.size(),
                    dvRanges);
        }
    }
}
