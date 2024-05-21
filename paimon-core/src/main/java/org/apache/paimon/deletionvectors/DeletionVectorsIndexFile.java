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
import org.apache.paimon.index.IndexFile;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.PathFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.zip.CRC32;

/** DeletionVectors index file. */
public class DeletionVectorsIndexFile extends IndexFile {

    public static final String DELETION_VECTORS_INDEX = "DELETION_VECTORS";
    public static final byte VERSION_ID_V1 = 1;

    public DeletionVectorsIndexFile(FileIO fileIO, PathFactory pathFactory) {
        super(fileIO, pathFactory);
    }

    /**
     * Reads all deletion vectors from a specified file.
     *
     * @return A map where the key represents which file the DeletionVector belongs to, and the
     *     value is the corresponding DeletionVector object.
     * @throws UncheckedIOException If an I/O error occurs while reading from the file.
     */
    public Map<String, DeletionVector> readAllDeletionVectors(IndexFileMeta fileMeta) {
        LinkedHashMap<String, Pair<Integer, Integer>> deletionVectorRanges =
                fileMeta.deletionVectorsRanges();
        if (deletionVectorRanges == null || deletionVectorRanges.isEmpty()) {
            return Collections.emptyMap();
        }

        String indexFileName = fileMeta.fileName();
        Map<String, DeletionVector> deletionVectors = new HashMap<>();
        Path filePath = pathFactory.toPath(indexFileName);
        try (SeekableInputStream inputStream = fileIO.newInputStream(filePath)) {
            checkVersion(inputStream);
            DataInputStream dataInputStream = new DataInputStream(inputStream);
            for (Map.Entry<String, Pair<Integer, Integer>> entry :
                    deletionVectorRanges.entrySet()) {
                deletionVectors.put(
                        entry.getKey(),
                        readDeletionVector(dataInputStream, entry.getValue().getRight()));
            }
        } catch (Exception e) {
            throw new RuntimeException(
                    "Unable to read deletion vectors from file: "
                            + filePath
                            + ", deletionVectorRanges: "
                            + deletionVectorRanges,
                    e);
        }
        return deletionVectors;
    }

    /**
     * Reads a single deletion vector from the specified file.
     *
     * @param fileName The name of the file from which to read the deletion vector.
     * @param deletionVectorRange A Pair specifying the range (start position and size) within the
     *     file where the deletion vector data is located.
     * @return The DeletionVector object read from the specified range in the file.
     * @throws UncheckedIOException If an I/O error occurs while reading from the file.
     */
    public DeletionVector readDeletionVector(
            String fileName, Pair<Integer, Integer> deletionVectorRange) {
        Path filePath = pathFactory.toPath(fileName);
        try (SeekableInputStream inputStream = fileIO.newInputStream(filePath)) {
            checkVersion(inputStream);
            inputStream.seek(deletionVectorRange.getLeft());
            DataInputStream dataInputStream = new DataInputStream(inputStream);
            return readDeletionVector(dataInputStream, deletionVectorRange.getRight());
        } catch (Exception e) {
            throw new RuntimeException(
                    "Unable to read deletion vector from file: "
                            + filePath
                            + ", deletionVectorRange: "
                            + deletionVectorRange,
                    e);
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
    public Pair<String, LinkedHashMap<String, Pair<Integer, Integer>>> write(
            Map<String, DeletionVector> input) {
        int size = input.size();
        LinkedHashMap<String, Pair<Integer, Integer>> deletionVectorRanges =
                new LinkedHashMap<>(size);
        Path path = pathFactory.newPath();
        try (DataOutputStream dataOutputStream =
                new DataOutputStream(fileIO.newOutputStream(path, true))) {
            dataOutputStream.writeByte(VERSION_ID_V1);
            for (Map.Entry<String, DeletionVector> entry : input.entrySet()) {
                String key = entry.getKey();
                byte[] valueBytes = entry.getValue().serializeToBytes();
                deletionVectorRanges.put(key, Pair.of(dataOutputStream.size(), valueBytes.length));
                dataOutputStream.writeInt(valueBytes.length);
                dataOutputStream.write(valueBytes);
                dataOutputStream.writeInt(calculateChecksum(valueBytes));
            }
        } catch (IOException e) {
            throw new RuntimeException(
                    "Unable to write deletion vectors to file: " + path.getName(), e);
        }
        return Pair.of(path.getName(), deletionVectorRanges);
    }

    private void checkVersion(InputStream in) throws IOException {
        int version = in.read();
        if (version != VERSION_ID_V1) {
            throw new RuntimeException(
                    "Version not match, actual version: "
                            + version
                            + ", expert version: "
                            + VERSION_ID_V1);
        }
    }

    private DeletionVector readDeletionVector(DataInputStream inputStream, int size) {
        try {
            // check size
            int actualSize = inputStream.readInt();
            if (actualSize != size) {
                throw new RuntimeException(
                        "Size not match, actual size: " + actualSize + ", expert size: " + size);
            }

            // read DeletionVector bytes
            byte[] bytes = new byte[size];
            inputStream.readFully(bytes);

            // check checksum
            int checkSum = calculateChecksum(bytes);
            int actualCheckSum = inputStream.readInt();
            if (actualCheckSum != checkSum) {
                throw new RuntimeException(
                        "Checksum not match, actual checksum: "
                                + actualCheckSum
                                + ", expected checksum: "
                                + checkSum);
            }
            return DeletionVector.deserializeFromBytes(bytes);
        } catch (IOException e) {
            throw new UncheckedIOException("Unable to read deletion vector", e);
        }
    }

    private int calculateChecksum(byte[] bytes) {
        CRC32 crc = new CRC32();
        crc.update(bytes);
        return (int) crc.getValue();
    }
}
