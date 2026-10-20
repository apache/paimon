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

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Old row IDs in the physical row order of a clustering output file. */
public final class ClusteringRowIdMapping {

    private static final String PREFIX = "cluster-row-id-";
    private static final String SUFFIX = ".mapping";
    private static final int MAGIC = 0x43524944;
    private static final int VERSION = 1;

    private ClusteringRowIdMapping() {}

    public static String fileName(String dataFileName) {
        return PREFIX + dataFileName + SUFFIX;
    }

    public static boolean isMappingFile(String fileName) {
        return fileName.startsWith(PREFIX) && fileName.endsWith(SUFFIX);
    }

    public static Writer createWriter(FileIO fileIO, Path path) throws IOException {
        return new Writer(fileIO, path);
    }

    public static void read(FileIO fileIO, Path path, long rowCount, RowIdConsumer consumer)
            throws IOException {
        checkArgument(rowCount >= 0, "Invalid clustering row count: %s", rowCount);
        try (DataInputStream in =
                new DataInputStream(new BufferedInputStream(fileIO.newInputStream(path)))) {
            checkArgument(in.readInt() == MAGIC, "Invalid clustering row ID mapping: %s", path);
            checkArgument(
                    in.readInt() == VERSION, "Unsupported clustering row ID mapping: %s", path);
            for (long ordinal = 0; ordinal < rowCount; ordinal++) {
                long oldRowId = in.readLong();
                checkArgument(oldRowId >= 0, "Invalid old row ID: %s", oldRowId);
                consumer.accept(ordinal, oldRowId);
            }
            checkArgument(in.read() == -1, "Unexpected trailing clustering row IDs: %s", path);
        }
    }

    /** Streaming consumer; the ordinal is local to the output data file. */
    @FunctionalInterface
    public interface RowIdConsumer {
        void accept(long ordinal, long oldRowId) throws IOException;
    }

    /**
     * Streaming writer, also used when splitting a spilled sort output at actual file boundaries.
     */
    public static final class Writer implements Closeable {

        private final DataOutputStream out;

        private Writer(FileIO fileIO, Path path) throws IOException {
            out =
                    new DataOutputStream(
                            new BufferedOutputStream(fileIO.newOutputStream(path, false)));
            try {
                out.writeInt(MAGIC);
                out.writeInt(VERSION);
            } catch (IOException | RuntimeException | Error failure) {
                try {
                    out.close();
                } catch (IOException closeFailure) {
                    failure.addSuppressed(closeFailure);
                }
                fileIO.deleteQuietly(path);
                throw failure;
            }
        }

        public void write(long oldRowId) throws IOException {
            checkArgument(oldRowId >= 0, "Invalid old row ID: %s", oldRowId);
            out.writeLong(oldRowId);
        }

        @Override
        public void close() throws IOException {
            out.close();
        }
    }
}
