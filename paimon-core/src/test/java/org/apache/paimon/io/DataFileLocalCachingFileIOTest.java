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

package org.apache.paimon.io;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.IOUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link DataFileLocalCachingFileIO}. */
public class DataFileLocalCachingFileIOTest {

    @TempDir java.nio.file.Path tempDir;

    private String bucketDir;
    private String cacheDir;
    private FileIO localFileIO;
    private FileIO targetFileIO;
    private DataFileLocalCachingFileIO cachingFileIO;

    @BeforeEach
    public void setUp() {
        bucketDir =
                tempDir.toString()
                        + Path.SEPARATOR
                        + FileStorePathFactory.BUCKET_PATH_PREFIX
                        + "0/";
        cacheDir = tempDir.toString() + Path.SEPARATOR + "cache/";
        localFileIO = LocalFileIO.create();
        targetFileIO = new LocalFileIO();
        cachingFileIO =
                new DataFileLocalCachingFileIO(
                        targetFileIO,
                        cacheDir,
                        new MemorySize(1024 * 1024), // 1MB cache
                        0.75);
    }

    @AfterEach
    public void clear() {
        localFileIO.deleteDirectoryQuietly(new Path(bucketDir));
        localFileIO.deleteDirectoryQuietly(new Path(cacheDir));
    }

    @Test
    public void testIsObjectStore() {
        assertThat(cachingFileIO.isObjectStore()).isFalse();
    }

    @Test
    public void testNewInputStreamWithDataFile() throws IOException {
        // Create a data file path (contains bucket path prefix)
        Path dataFilePath = new Path(bucketDir + "data-file-1.parquet");

        // Write some data to the target file
        String testData = "test data for caching";
        try (PositionOutputStream out = targetFileIO.newOutputStream(dataFilePath, false)) {
            out.write(testData.getBytes());
        }

        // Read through caching file IO
        byte[] result = new byte[testData.length()];
        try (SeekableInputStream in = cachingFileIO.newInputStream(dataFilePath)) {
            IOUtils.readFully(in, result);
        }

        assertThat(new String(result)).isEqualTo(testData);

        // Verify that the file is cached locally
        FileStatus[] tempDirs = localFileIO.listStatus(new Path(cacheDir));
        assertThat(tempDirs.length).isEqualTo(1);
        Path cachePath = tempDirs[0].getPath();

        FileStatus[] status = localFileIO.listStatus(cachePath);
        assertThat(status.length).isEqualTo(1);
        Path localFile = status[0].getPath();
        assertThat(localFile.toString()).contains("data-file-1.parquet");
    }

    @Test
    public void testNewInputStreamWithNonDataFile() throws IOException {
        // Create a non-data file path (doesn't contain bucket path prefix)
        Path nonDataFilePath = new Path(tempDir.toString(), "non-data-file.txt");

        // Write some data to the target file
        String testData = "test data for non-cached file";
        try (PositionOutputStream out = targetFileIO.newOutputStream(nonDataFilePath, false)) {
            out.write(testData.getBytes());
        }

        // Read through caching file IO
        byte[] result = new byte[testData.length()];
        try (SeekableInputStream in = cachingFileIO.newInputStream(nonDataFilePath)) {
            IOUtils.readFully(in, result);
        }

        assertThat(new String(result)).isEqualTo(testData);

        // The cache isn't initialized
        assertThat(localFileIO.exists(new Path(cacheDir))).isFalse();
    }

    @Test
    public void testNewOutputStreamWithDataFile() throws IOException {
        // Create a data file path (contains bucket path prefix)
        Path dataFilePath = new Path(bucketDir + "data-file-1.parquet");

        // Write through caching file IO
        String testData = "test output data";
        try (PositionOutputStream out = cachingFileIO.newOutputStream(dataFilePath, false)) {
            out.write(testData.getBytes());
        }

        // Test flush
        assertThat(localFileIO.exists(dataFilePath)).isFalse();
        cachingFileIO.flush();
        assertThat(localFileIO.exists(dataFilePath)).isTrue();

        // Read directly from target file IO to verify
        byte[] result = new byte[testData.length()];
        try (SeekableInputStream in = targetFileIO.newInputStream(dataFilePath)) {
            IOUtils.readFully(in, result);
        }

        assertThat(new String(result)).isEqualTo(testData);
    }

    @Test
    public void testNewOutputStreamWithNonDataFile() throws IOException {
        // Create a non-data file path (doesn't contain bucket path prefix)
        Path nonDataFilePath = new Path(tempDir.toString(), "non-data-output-file.txt");

        // Write through caching file IO
        String testData = "test output data for non-data file";
        try (PositionOutputStream out = cachingFileIO.newOutputStream(nonDataFilePath, false)) {
            out.write(testData.getBytes());
        }

        // For non-data files, data should be immediately written to target file IO
        // without needing to call flush
        assertThat(targetFileIO.exists(nonDataFilePath)).isTrue();

        // Read directly from target file IO to verify
        byte[] result = new byte[testData.length()];
        try (SeekableInputStream in = targetFileIO.newInputStream(nonDataFilePath)) {
            IOUtils.readFully(in, result);
        }

        assertThat(new String(result)).isEqualTo(testData);

        // The cache isn't initialized
        assertThat(localFileIO.exists(new Path(cacheDir))).isFalse();
    }

    @Test
    public void testGetFileStatusWithDataFile() throws IOException {
        // Create a data file path (contains bucket path prefix)
        Path dataFilePath = new Path(bucketDir + "data-file-1.parquet");

        // Test case 1: File exists only in target
        try (PositionOutputStream out = targetFileIO.newOutputStream(dataFilePath, false)) {
            out.write("test".getBytes());
        }

        FileStatus status1 = cachingFileIO.getFileStatus(dataFilePath);
        assertThat(status1).isNotNull();
        assertThat(status1.getPath().toString()).doesNotContain("cache");

        // Test case 2: File exists only in cache
        Path dataFilePath2 = new Path(bucketDir + "data-file-2.parquet");
        try (PositionOutputStream out = cachingFileIO.newOutputStream(dataFilePath2, false)) {
            out.write("test2".getBytes());
        }

        // At this point, the file should be in cache but not on remote
        // getFileStatus should flush the file to remote and return the remote file status
        FileStatus status2 = cachingFileIO.getFileStatus(dataFilePath2);
        assertThat(status2.getPath().toString()).doesNotContain("cache");

        // Verify the file now exists on remote
        assertThat(targetFileIO.exists(dataFilePath2)).isTrue();
    }

    @Test
    public void testExistsWithDataFile() throws IOException {
        // Create a data file path (contains bucket path prefix)
        Path dataFilePath = new Path(bucketDir + "data-file-1.parquet");

        // Test case 1: File exist in both cache and target
        try (PositionOutputStream out = cachingFileIO.newOutputStream(dataFilePath, false)) {
            out.write("test".getBytes());
        }
        cachingFileIO.flush();
        assertThat(targetFileIO.exists(dataFilePath)).isTrue();
        assertThat(cachingFileIO.exists(dataFilePath)).isTrue();

        cachingFileIO.delete(dataFilePath, false);
        // Test case 2: File exists only in cache
        try (PositionOutputStream out = cachingFileIO.newOutputStream(dataFilePath, false)) {
            out.write("test".getBytes());
        }
        assertThat(targetFileIO.exists(dataFilePath)).isFalse();
        assertThat(cachingFileIO.exists(dataFilePath)).isTrue();

        cachingFileIO.delete(dataFilePath, false);
        // Test case 3: File exists only in target
        try (PositionOutputStream out = targetFileIO.newOutputStream(dataFilePath, false)) {
            out.write("test".getBytes());
        }
        assertThat(targetFileIO.exists(dataFilePath)).isTrue();
        assertThat(cachingFileIO.exists(dataFilePath)).isTrue();
    }

    @Test
    public void testExistsWithNonDataFile() throws IOException {
        // Create a non-data file path (doesn't contain bucket path prefix)
        Path nonDataFilePath = new Path(tempDir.toString(), "non-data-existing-file");

        // Create the file
        try (PositionOutputStream out = cachingFileIO.newOutputStream(nonDataFilePath, false)) {
            out.write("test".getBytes());
        }

        // It should exist
        assertThat(cachingFileIO.exists(nonDataFilePath)).isTrue();
    }

    @Test
    public void testDeleteWithDataFile() throws IOException {
        // Create a data file path (contains bucket path prefix)
        Path dataFilePath = new Path(bucketDir + "data-file-1.parquet");

        // Test case 1: File exists only in cache
        try (PositionOutputStream out = cachingFileIO.newOutputStream(dataFilePath, false)) {
            out.write("test".getBytes());
        }
        assertThat(targetFileIO.exists(dataFilePath)).isFalse();
        assertThat(cachingFileIO.exists(dataFilePath)).isTrue();

        boolean deleted1 = cachingFileIO.delete(dataFilePath, false);
        assertThat(deleted1).isTrue();
        assertThat(cachingFileIO.exists(dataFilePath)).isFalse();

        // Test case 2: File exists only in target
        try (PositionOutputStream out = targetFileIO.newOutputStream(dataFilePath, false)) {
            out.write("test".getBytes());
        }
        assertThat(targetFileIO.exists(dataFilePath)).isTrue();
        assertThat(cachingFileIO.exists(dataFilePath)).isTrue();

        boolean deleted2 = cachingFileIO.delete(dataFilePath, false);
        assertThat(deleted2).isTrue();
        assertThat(cachingFileIO.exists(dataFilePath)).isFalse();
        assertThat(targetFileIO.exists(dataFilePath)).isFalse();

        // Test case 3: File exists in both cache and target
        try (PositionOutputStream out = cachingFileIO.newOutputStream(dataFilePath, false)) {
            out.write("test".getBytes());
        }
        cachingFileIO.flush();
        assertThat(targetFileIO.exists(dataFilePath)).isTrue();
        assertThat(cachingFileIO.exists(dataFilePath)).isTrue();

        boolean deleted3 = cachingFileIO.delete(dataFilePath, false);
        assertThat(deleted3).isTrue();
        assertThat(cachingFileIO.exists(dataFilePath)).isFalse();
        assertThat(targetFileIO.exists(dataFilePath)).isFalse();
    }
}
