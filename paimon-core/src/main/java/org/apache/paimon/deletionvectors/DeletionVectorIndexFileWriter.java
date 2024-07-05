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
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.PathFactory;
import org.apache.paimon.utils.Preconditions;

import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
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
    private final long targetSizeInBytes;

    public DeletionVectorIndexFileWriter(
            FileIO fileIO, PathFactory pathFactory, MemorySize targetSizePerIndexFile) {
        this.indexPathFactory = pathFactory;
        this.fileIO = fileIO;
        this.targetSizeInBytes = targetSizePerIndexFile.getBytes();
    }

    public List<IndexFileMeta> write(Map<String, DeletionVector> input) throws IOException {
        if (input.isEmpty()) {
            return emptyIndexFile();
        }
        List<IndexFileMeta> result = new ArrayList<>();
        Iterator<Map.Entry<String, DeletionVector>> iterator = input.entrySet().iterator();
        while (iterator.hasNext()) {
            result.add(tryWriter(iterator));
        }
        return result;
    }

    private IndexFileMeta tryWriter(Iterator<Map.Entry<String, DeletionVector>> iterator)
            throws IOException {
        SingleIndexFileWriter writer = new SingleIndexFileWriter();
        try {
            while (iterator.hasNext()) {
                Map.Entry<String, DeletionVector> entry = iterator.next();
                long currentSize = writer.write(entry.getKey(), entry.getValue());

                if (writer.writtenSizeInBytes() + currentSize > targetSizeInBytes) {
                    break;
                }
            }
        } finally {
            writer.close();
        }
        return writer.writtenIndexFile();
    }

    /**
     * The deletion file of the bucketed table is updated according to the bucket. If a compaction
     * occurs and there is no longer a deletion file, an empty deletion file needs to be generated
     * to overwrite the old file.
     *
     * <p>TODO: We can consider sending a message to delete the deletion file in the future.
     */
    private List<IndexFileMeta> emptyIndexFile() throws IOException {
        try (SingleIndexFileWriter writer = new SingleIndexFileWriter()) {
            return Collections.singletonList(writer.writtenIndexFile());
        }
    }

    private class SingleIndexFileWriter implements Closeable {

        private final Path path;

        private final DataOutputStream dataOutputStream;

        private final LinkedHashMap<String, Pair<Integer, Integer>> dvRanges;

        private long writtenSizeInBytes = 0L;

        public SingleIndexFileWriter() throws IOException {
            this.path = indexPathFactory.newPath();
            this.dataOutputStream = new DataOutputStream(fileIO.newOutputStream(path, true));
            dataOutputStream.writeByte(VERSION_ID_V1);
            this.dvRanges = new LinkedHashMap<>();
        }

        public long writtenSizeInBytes() {
            return this.writtenSizeInBytes;
        }

        public long write(String key, DeletionVector deletionVector) throws IOException {
            Preconditions.checkNotNull(dataOutputStream);
            byte[] data = deletionVector.serializeToBytes();
            int size = data.length;

            dvRanges.put(key, Pair.of(dataOutputStream.size(), size));
            dataOutputStream.writeInt(size);
            dataOutputStream.write(data);
            dataOutputStream.writeInt(calculateChecksum(data));
            writtenSizeInBytes += size;
            return size;
        }

        public IndexFileMeta writtenIndexFile() throws IOException {
            return new IndexFileMeta(
                    DELETION_VECTORS_INDEX,
                    path.getName(),
                    fileIO.getFileSize(path),
                    dvRanges.size(),
                    dvRanges);
        }

        @Override
        public void close() throws IOException {
            dataOutputStream.close();
        }
    }
}
