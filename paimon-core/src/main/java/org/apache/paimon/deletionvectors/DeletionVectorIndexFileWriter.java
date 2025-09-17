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
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.IndexPathFactory;
import org.apache.paimon.options.MemorySize;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/** Writer for deletion vector index file. */
public class DeletionVectorIndexFileWriter {

    private final IndexPathFactory indexPathFactory;
    private final FileIO fileIO;
    private final long targetSizeInBytes;

    public DeletionVectorIndexFileWriter(
            FileIO fileIO, IndexPathFactory pathFactory, MemorySize targetSizePerIndexFile) {
        this.indexPathFactory = pathFactory;
        this.fileIO = fileIO;
        this.targetSizeInBytes = targetSizePerIndexFile.getBytes();
    }

    /**
     * The deletion file of the bucketed table is updated according to the bucket. If a compaction
     * occurs and there is no longer a deletion file, an empty deletion file needs to be generated
     * to overwrite the old file.
     *
     * <p>TODO: We can consider sending a message to delete the deletion file in the future.
     */
    public IndexFileMeta writeSingleFile(Map<String, DeletionVector> input) throws IOException {
        DeletionFileWriter writer = new DeletionFileWriter(indexPathFactory, fileIO);
        try {
            for (Map.Entry<String, DeletionVector> entry : input.entrySet()) {
                writer.write(entry.getKey(), entry.getValue());
            }
        } finally {
            writer.close();
        }
        return writer.result();
    }

    public List<IndexFileMeta> writeWithRolling(Map<String, DeletionVector> input)
            throws IOException {
        if (input.isEmpty()) {
            return Collections.emptyList();
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
        DeletionFileWriter writer = new DeletionFileWriter(indexPathFactory, fileIO);
        try {
            while (iterator.hasNext()) {
                Map.Entry<String, DeletionVector> entry = iterator.next();
                writer.write(entry.getKey(), entry.getValue());
                if (writer.getPos() > targetSizeInBytes) {
                    break;
                }
            }
        } finally {
            writer.close();
        }
        return writer.result();
    }
}
