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
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.index.DeletionVectorMeta;
import org.apache.paimon.index.IndexFile;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.table.source.DeletionFile;
import org.apache.paimon.utils.PathFactory;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/** DeletionVectors index file. */
public class DeletionVectorsIndexFile extends IndexFile {

    public static final String DELETION_VECTORS_INDEX = "DELETION_VECTORS";
    public static final byte VERSION_ID_V1 = 1;

    private final MemorySize targetSizePerIndexFile;
    private final boolean bitmap64;

    public DeletionVectorsIndexFile(
            FileIO fileIO,
            PathFactory pathFactory,
            MemorySize targetSizePerIndexFile,
            boolean bitmap64) {
        super(fileIO, pathFactory);
        this.targetSizePerIndexFile = targetSizePerIndexFile;
        this.bitmap64 = bitmap64;
    }

    public boolean bitmap64() {
        return bitmap64;
    }

    /**
     * Reads all deletion vectors from a specified file.
     *
     * @return A map where the key represents which file the DeletionVector belongs to, and the
     *     value is the corresponding DeletionVector object.
     * @throws UncheckedIOException If an I/O error occurs while reading from the file.
     */
    public Map<String, DeletionVector> readAllDeletionVectors(IndexFileMeta fileMeta) {
        LinkedHashMap<String, DeletionVectorMeta> deletionVectorMetas =
                fileMeta.deletionVectorMetas();
        checkNotNull(deletionVectorMetas);

        String indexFileName = fileMeta.fileName();
        Map<String, DeletionVector> deletionVectors = new HashMap<>();
        Path filePath = pathFactory.toPath(indexFileName);
        try (SeekableInputStream inputStream = fileIO.newInputStream(filePath)) {
            checkVersion(inputStream);
            DataInputStream dataInputStream = new DataInputStream(inputStream);
            for (DeletionVectorMeta deletionVectorMeta : deletionVectorMetas.values()) {
                deletionVectors.put(
                        deletionVectorMeta.dataFileName(),
                        DeletionVector.read(dataInputStream, (long) deletionVectorMeta.length()));
            }
        } catch (Exception e) {
            throw new RuntimeException(
                    "Unable to read deletion vectors from file: "
                            + filePath
                            + ", deletionVectorMetas: "
                            + deletionVectorMetas,
                    e);
        }
        return deletionVectors;
    }

    public Map<String, DeletionVector> readAllDeletionVectors(List<IndexFileMeta> indexFiles) {
        Map<String, DeletionVector> deletionVectors = new HashMap<>();
        indexFiles.forEach(indexFile -> deletionVectors.putAll(readAllDeletionVectors(indexFile)));
        return deletionVectors;
    }

    /** Reads deletion vectors from a list of DeletionFile which belong to a same index file. */
    public Map<String, DeletionVector> readDeletionVector(
            Map<String, DeletionFile> dataFileToDeletionFiles) {
        Map<String, DeletionVector> deletionVectors = new HashMap<>();
        if (dataFileToDeletionFiles.isEmpty()) {
            return deletionVectors;
        }

        String indexFile = dataFileToDeletionFiles.values().stream().findAny().get().path();
        try (SeekableInputStream inputStream = fileIO.newInputStream(new Path(indexFile))) {
            checkVersion(inputStream);
            for (String dataFile : dataFileToDeletionFiles.keySet()) {
                DeletionFile deletionFile = dataFileToDeletionFiles.get(dataFile);
                checkArgument(deletionFile.path().equals(indexFile));
                inputStream.seek(deletionFile.offset());
                DataInputStream dataInputStream = new DataInputStream(inputStream);
                deletionVectors.put(
                        dataFile, DeletionVector.read(dataInputStream, deletionFile.length()));
            }
        } catch (Exception e) {
            throw new RuntimeException("Unable to read deletion vector from file: " + indexFile, e);
        }
        return deletionVectors;
    }

    public DeletionVector readDeletionVector(DeletionFile deletionFile) {
        String indexFile = deletionFile.path();
        try (SeekableInputStream inputStream = fileIO.newInputStream(new Path(indexFile))) {
            checkVersion(inputStream);
            checkArgument(deletionFile.path().equals(indexFile));
            inputStream.seek(deletionFile.offset());
            DataInputStream dataInputStream = new DataInputStream(inputStream);
            return DeletionVector.read(dataInputStream, deletionFile.length());
        } catch (Exception e) {
            throw new RuntimeException("Unable to read deletion vector from file: " + indexFile, e);
        }
    }

    /**
     * Write deletion vectors to a new file, the format of this file can be referenced at: <a
     * href="https://cwiki.apache.org/confluence/x/Tws4EQ">PIP-16</a>.
     *
     * @param input A map where the key represents which file the DeletionVector belongs to, and the
     *     value is the corresponding DeletionVector object.
     * @return A Pair object specifying the name of the written new file and a map where the key
     *     represents which file the DeletionVector belongs to and the value is a Pair object
     *     specifying the range (start position and size) within the file where the deletion vector
     *     data is located.
     * @throws UncheckedIOException If an I/O error occurs while writing to the file.
     */
    public List<IndexFileMeta> write(Map<String, DeletionVector> input) {
        try {
            DeletionVectorIndexFileWriter writer =
                    new DeletionVectorIndexFileWriter(
                            this.fileIO, this.pathFactory, this.targetSizePerIndexFile);
            return writer.write(input);
        } catch (IOException e) {
            throw new RuntimeException("Failed to write deletion vectors.", e);
        }
    }

    private void checkVersion(InputStream in) throws IOException {
        int version = in.read();
        if (version != VERSION_ID_V1) {
            throw new RuntimeException(
                    "Version not match, actual version: "
                            + version
                            + ", expected version: "
                            + VERSION_ID_V1);
        }
    }
}
