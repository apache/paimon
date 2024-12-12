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

package org.apache.paimon.utils;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FormatReaderContext;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.reader.RecordReader;

import javax.annotation.Nullable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.stream.Stream;

/** Utils for file reading and writing. */
public class FileUtils {

    /**
     * List versioned files for the directory.
     *
     * @return version stream
     */
    public static Stream<Long> listVersionedFiles(FileIO fileIO, Path dir, String prefix)
            throws IOException {
        return listOriginalVersionedFiles(fileIO, dir, prefix).map(Long::parseLong);
    }

    /**
     * List original versioned files for the directory.
     *
     * @return version stream
     */
    public static Stream<String> listOriginalVersionedFiles(FileIO fileIO, Path dir, String prefix)
            throws IOException {
        return listVersionedFileStatus(fileIO, dir, prefix)
                .map(FileStatus::getPath)
                .map(Path::getName)
                .map(name -> name.substring(prefix.length()));
    }

    /**
     * List versioned file status for the directory.
     *
     * @return file status stream
     */
    public static Stream<FileStatus> listVersionedFileStatus(FileIO fileIO, Path dir, String prefix)
            throws IOException {
        if (!fileIO.exists(dir)) {
            return Stream.empty();
        }

        FileStatus[] statuses = fileIO.listStatus(dir);

        if (statuses == null) {
            throw new RuntimeException(
                    String.format(
                            "The return value is null of the listStatus for the '%s' directory.",
                            dir));
        }

        return Arrays.stream(statuses)
                .filter(status -> status.getPath().getName().startsWith(prefix));
    }

    /**
     * List versioned directories for the directory.
     *
     * @return file status stream
     */
    public static Stream<FileStatus> listVersionedDirectories(
            FileIO fileIO, Path dir, String prefix) throws IOException {
        if (!fileIO.exists(dir)) {
            return Stream.empty();
        }

        FileStatus[] statuses = fileIO.listDirectories(dir);

        if (statuses == null) {
            throw new RuntimeException(
                    String.format(
                            "The return value is null of the listStatus for the '%s' directory.",
                            dir));
        }

        return Arrays.stream(statuses)
                .filter(status -> status.getPath().getName().startsWith(prefix));
    }

    public static void checkExists(FileIO fileIO, Path file) throws IOException {
        if (!fileIO.exists(file)) {
            throw new FileNotFoundException(
                    String.format(
                            "File '%s' not found, Possible causes: "
                                    + "1.snapshot expires too fast, you can configure 'snapshot.time-retained'"
                                    + " option with a larger value. "
                                    + "2.the file was compacted before it was consumed, you can improve the performance of consumption"
                                    + " (For example, increasing parallelism) or add a consumer id for safe consumption: https://paimon.apache.org/docs/master/flink/consumer-id/",
                            file));
        }
    }

    public static RecordReader<InternalRow> createFormatReader(
            FileIO fileIO, FormatReaderFactory format, Path file, @Nullable Long fileSize)
            throws IOException {
        try {
            if (fileSize == null) {
                fileSize = fileIO.getFileSize(file);
            }
            return format.createReader(new FormatReaderContext(fileIO, file, fileSize));
        } catch (Exception e) {
            checkExists(fileIO, file);
            throw e;
        }
    }
}
