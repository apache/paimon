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

package org.apache.paimon.stats;

import org.apache.paimon.annotation.Experimental;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.memory.BytesUtils;
import org.apache.paimon.utils.IOUtils;
import org.apache.paimon.utils.JsonSerdeUtil;
import org.apache.paimon.utils.PathFactory;
import org.apache.paimon.utils.StreamUtils;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/** Sidecar file for statistics blobs. */
@Experimental
public class StatisticsSidecarFile {

    private static final int VERSION = 1;
    private static final int MAGIC_NUMBER = 0x53544331;
    private static final byte[] MAGIC_NUMBER_BYTES = StreamUtils.intToLittleEndian(MAGIC_NUMBER);
    private static final int FOOTER_LENGTH_BYTES = Integer.BYTES;
    private static final int MAGIC_NUMBER_LENGTH = Integer.BYTES;
    private static final int MIN_FILE_LENGTH = FOOTER_LENGTH_BYTES + MAGIC_NUMBER_LENGTH;

    private final FileIO fileIO;
    private final PathFactory pathFactory;

    public StatisticsSidecarFile(FileIO fileIO, PathFactory pathFactory) {
        this.fileIO = fileIO;
        this.pathFactory = pathFactory;
    }

    /**
     * Write statistics blobs to one sidecar file.
     *
     * @return metadata for the written blobs
     */
    public List<StatisticsBlobMetadata> write(List<StatisticsBlob> blobs) {
        checkNotNull(blobs, "blobs must not be null");
        checkArgument(!blobs.isEmpty(), "Statistics sidecar file must contain at least one blob.");

        Path path = pathFactory.newPath();
        List<StatisticsBlobMetadata> metadata = new ArrayList<>(blobs.size());
        try (PositionOutputStream out = fileIO.newOutputStream(path, false)) {
            for (StatisticsBlob blob : blobs) {
                long offset = out.getPos();
                byte[] payload = blob.payload();
                out.write(payload);
                metadata.add(blob.toMetadata(path.getName(), offset, payload.length));
            }

            byte[] footer =
                    JsonSerdeUtil.toFlatJson(new Footer(VERSION, metadata))
                            .getBytes(StandardCharsets.UTF_8);
            out.write(footer);
            out.write(StreamUtils.intToLittleEndian(footer.length));
            out.write(MAGIC_NUMBER_BYTES);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to write statistics sidecar file: " + path, e);
        }
        return metadata;
    }

    /** Read the metadata footer from a sidecar file. */
    public List<StatisticsBlobMetadata> readMetadata(String fileName) {
        Path path = pathFactory.toPath(fileName);
        try (SeekableInputStream in = fileIO.newInputStream(path)) {
            long fileLength = fileIO.getFileStatus(path).getLen();
            checkArgument(
                    fileLength >= MIN_FILE_LENGTH,
                    "Statistics sidecar file %s is too small.",
                    path);

            byte[] footerHandle = new byte[MIN_FILE_LENGTH];
            in.seek(fileLength - MIN_FILE_LENGTH);
            IOUtils.readFully(in, footerHandle);

            int footerLength = BytesUtils.getInt(footerHandle, 0);
            int magicNumber = BytesUtils.getInt(footerHandle, FOOTER_LENGTH_BYTES);
            checkArgument(
                    magicNumber == MAGIC_NUMBER,
                    "Statistics sidecar file %s has invalid magic number.",
                    path);
            checkArgument(
                    footerLength >= 0 && footerLength <= fileLength - MIN_FILE_LENGTH,
                    "Statistics sidecar file %s has invalid footer length.",
                    path);

            byte[] footerBytes = new byte[footerLength];
            in.seek(fileLength - MIN_FILE_LENGTH - footerLength);
            IOUtils.readFully(in, footerBytes);
            Footer footer =
                    JsonSerdeUtil.fromJson(
                            new String(footerBytes, StandardCharsets.UTF_8), Footer.class);
            checkArgument(
                    footer.version() == VERSION,
                    "Unsupported statistics sidecar file version %s.",
                    footer.version());
            return footer.blobs();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to read statistics sidecar file: " + path, e);
        }
    }

    /** Read a statistics blob payload described by the metadata. */
    public byte[] read(StatisticsBlobMetadata metadata) {
        checkNotNull(metadata, "metadata must not be null");
        checkArgument(
                metadata.offset().isPresent() && metadata.length().isPresent(),
                "Statistics blob metadata must contain offset and length.");

        long offset = metadata.offset().getAsLong();
        long length = metadata.length().getAsLong();
        checkArgument(length <= Integer.MAX_VALUE, "Statistics blob is too large to read.");
        checkArgument(offset >= 0 && length >= 0, "Invalid statistics blob range.");

        Path path = pathFactory.toPath(metadata.fileLocation());
        try (SeekableInputStream in = fileIO.newInputStream(path)) {
            long fileLength = fileIO.getFileStatus(path).getLen();
            checkArgument(
                    offset <= fileLength && length <= fileLength - offset,
                    "Statistics blob range exceeds sidecar file length: %s.",
                    metadata);
            byte[] payload = new byte[(int) length];
            in.seek(offset);
            IOUtils.readFully(in, payload);
            return payload;
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to read statistics blob: " + metadata, e);
        }
    }

    public void delete(String fileName) {
        fileIO.deleteQuietly(pathFactory.toPath(fileName));
    }

    public boolean exists(String fileName) {
        try {
            return fileIO.exists(pathFactory.toPath(fileName));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    private static class Footer {

        private static final String FIELD_VERSION = "version";
        private static final String FIELD_BLOBS = "blobs";

        @JsonProperty(FIELD_VERSION)
        private final int version;

        @JsonProperty(FIELD_BLOBS)
        private final List<StatisticsBlobMetadata> blobs;

        @JsonCreator
        private Footer(
                @JsonProperty(FIELD_VERSION) int version,
                @JsonProperty(FIELD_BLOBS) @Nullable List<StatisticsBlobMetadata> blobs) {
            this.version = version;
            this.blobs =
                    blobs == null
                            ? Collections.emptyList()
                            : Collections.unmodifiableList(new ArrayList<>(blobs));
        }

        private int version() {
            return version;
        }

        private List<StatisticsBlobMetadata> blobs() {
            return blobs;
        }
    }
}
