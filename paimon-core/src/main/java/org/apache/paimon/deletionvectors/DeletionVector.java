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
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.table.source.DeletionFile;

import javax.annotation.Nullable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;

import static org.apache.paimon.deletionvectors.Bitmap64DeletionVector.toLittleEndianInt;

/**
 * The DeletionVector can efficiently record the positions of rows that are deleted in a file, which
 * can then be used to filter out deleted rows when processing the file.
 */
public interface DeletionVector extends DeletionVectorJudger {

    /**
     * Marks the row at the specified position as deleted.
     *
     * @param position The position of the row to be marked as deleted.
     */
    void delete(long position);

    /**
     * merge another {@link DeletionVector} to this current one.
     *
     * @param deletionVector the other {@link DeletionVector}
     */
    void merge(DeletionVector deletionVector);

    /**
     * Marks the row at the specified position as deleted.
     *
     * @param position The position of the row to be marked as deleted.
     * @return true if the added position wasn't already deleted. False otherwise.
     */
    default boolean checkedDelete(long position) {
        if (isDeleted(position)) {
            return false;
        } else {
            delete(position);
            return true;
        }
    }

    /**
     * Determines if the deletion vector is empty, indicating no deletions.
     *
     * @return true if the deletion vector is empty, false if it contains deletions.
     */
    boolean isEmpty();

    /** @return the number of distinct integers added to the DeletionVector. */
    long getCardinality();

    /** Serializes the deletion vector. */
    int serializeTo(DataOutputStream out) throws IOException;

    static DeletionVector read(FileIO fileIO, DeletionFile deletionFile) throws IOException {
        Path path = new Path(deletionFile.path());
        try (SeekableInputStream input = fileIO.newInputStream(path)) {
            input.seek(deletionFile.offset());
            DataInputStream dis = new DataInputStream(input);
            return read(dis, deletionFile.length());
        }
    }

    static DeletionVector read(DataInputStream dis, @Nullable Long length) throws IOException {
        // read bitmap length
        int bitmapLength = dis.readInt();
        // read magic number
        int magicNumber = dis.readInt();

        if (magicNumber == BitmapDeletionVector.MAGIC_NUMBER) {
            if (length != null && bitmapLength != length) {
                throw new RuntimeException(
                        "Size not match, actual size: "
                                + bitmapLength
                                + ", expected size: "
                                + length);
            }

            // magic number has been read
            byte[] bytes = new byte[bitmapLength - BitmapDeletionVector.MAGIC_NUMBER_SIZE_BYTES];
            dis.readFully(bytes);
            dis.skipBytes(4); // skip crc
            return BitmapDeletionVector.deserializeFromByteBuffer(ByteBuffer.wrap(bytes));
        } else if (toLittleEndianInt(magicNumber) == Bitmap64DeletionVector.MAGIC_NUMBER) {
            if (length != null) {
                long expectedBitmapLength =
                        length
                                - Bitmap64DeletionVector.LENGTH_SIZE_BYTES
                                - Bitmap64DeletionVector.CRC_SIZE_BYTES;
                if (bitmapLength != expectedBitmapLength) {
                    throw new RuntimeException(
                            "Size not match, actual size: "
                                    + bitmapLength
                                    + ", expected size: "
                                    + expectedBitmapLength);
                }
            }

            // magic number have been read
            byte[] bytes = new byte[bitmapLength - Bitmap64DeletionVector.MAGIC_NUMBER_SIZE_BYTES];
            dis.readFully(bytes);
            dis.skipBytes(4); // skip crc
            return Bitmap64DeletionVector.deserializeFromBitmapDataBytes(bytes);
        } else {
            throw new RuntimeException(
                    "Invalid magic number: "
                            + magicNumber
                            + ", v1 dv magic number: "
                            + BitmapDeletionVector.MAGIC_NUMBER
                            + ", v2 magic number: "
                            + Bitmap64DeletionVector.MAGIC_NUMBER);
        }
    }

    static Factory emptyFactory() {
        return fileName -> Optional.empty();
    }

    static Factory factory(@Nullable BucketedDvMaintainer dvMaintainer) {
        if (dvMaintainer == null) {
            return emptyFactory();
        }
        return dvMaintainer::deletionVectorOf;
    }

    static Factory factory(
            FileIO fileIO, List<DataFileMeta> files, @Nullable List<DeletionFile> deletionFiles) {
        DeletionFile.Factory factory = DeletionFile.factory(files, deletionFiles);
        return fileName -> {
            Optional<DeletionFile> deletionFile = factory.create(fileName);
            if (deletionFile.isPresent()) {
                return Optional.of(DeletionVector.read(fileIO, deletionFile.get()));
            }
            return Optional.empty();
        };
    }

    static byte[] serializeToBytes(DeletionVector deletionVector) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        try {
            deletionVector.serializeTo(dos);
            return bos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    static DeletionVector deserializeFromBytes(byte[] bytes) {
        ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        DataInputStream dis = new DataInputStream(bis);
        try {
            return read(dis, null);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /** Interface to create {@link DeletionVector}. */
    interface Factory {
        Optional<DeletionVector> create(String fileName) throws IOException;
    }
}
