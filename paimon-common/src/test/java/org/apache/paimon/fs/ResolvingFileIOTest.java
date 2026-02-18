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

package org.apache.paimon.fs;

import org.apache.paimon.catalog.CatalogHadoopContext;
import org.apache.paimon.fs.hadoop.HadoopFileIO;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/** Tests for {@link ResolvingFileIO}. */
public class ResolvingFileIOTest {

    private ResolvingFileIO resolvingFileIO;

    @BeforeEach
    public void setUp() {
        resolvingFileIO = new ResolvingFileIO();
        Options options = new Options();
        CatalogHadoopContext catalogContext = CatalogHadoopContext.create(options);
        resolvingFileIO.configure(catalogContext);
    }

    @Test
    public void testFileIONullSchemeReturnsFallbackFileIO() throws IOException {
        Path path = new Path("/path/to/file");
        FileIO result = resolvingFileIO.fileIO(path);
        assertNotNull(result);
        assertInstanceOf(LocalFileIO.class, result);
    }

    @Test
    public void testFileIOReturnsLocalFileIO() throws IOException {
        Path path = new Path("file:///path/to/file");
        FileIO result = resolvingFileIO.fileIO(path);
        assertNotNull(result);
        assertInstanceOf(LocalFileIO.class, result);
    }

    @Test
    public void testFileIOWithSchemeReturnsHdfsFileIO() throws IOException {
        Path path = new Path("hdfs:///path/to/file");
        FileIO result = resolvingFileIO.fileIO(path);
        assertNotNull(result);
        assertInstanceOf(HadoopFileIO.class, result);
    }

    @Test
    public void testFileIOConcurrentAccessInitializesFallbackFileIO() throws Exception {
        Path fileSchemePath = new Path("file:///path/to/file");
        ExecutorService executorService = Executors.newFixedThreadPool(2);
        Future<FileIO> future1 =
                executorService.submit(() -> resolvingFileIO.fileIO(fileSchemePath));
        Future<FileIO> future2 =
                executorService.submit(() -> resolvingFileIO.fileIO(fileSchemePath));

        FileIO result1 = future1.get();
        FileIO result2 = future2.get();

        assertNotNull(result1);
        assertNotNull(result2);
        assertEquals(result1, result2);
        assertInstanceOf(LocalFileIO.class, result1);

        Path noSchemePath = new Path("/path/to/file");
        future1 = executorService.submit(() -> resolvingFileIO.fileIO(noSchemePath));
        future2 = executorService.submit(() -> resolvingFileIO.fileIO(noSchemePath));

        result1 = future1.get();
        result2 = future2.get();

        assertNotNull(result1);
        assertNotNull(result2);
        assertEquals(result1, result2);
        assertInstanceOf(LocalFileIO.class, result1);

        Path hdfsSchemePath = new Path("hdfs:///path/to/file");
        future1 = executorService.submit(() -> resolvingFileIO.fileIO(hdfsSchemePath));
        future2 = executorService.submit(() -> resolvingFileIO.fileIO(hdfsSchemePath));

        result1 = future1.get();
        result2 = future2.get();

        assertNotNull(result1);
        assertNotNull(result2);
        assertEquals(result1, result2);
        assertInstanceOf(HadoopFileIO.class, result1);
    }

    @Test
    public void testFileIOMapStoresFileIOInstances() throws IOException {
        Path localPath = new Path("file:///path/to/local/file1");
        Path hdfsPath = new Path("hdfs:///path/to/hdfs/file1");

        // First call should create new instances
        FileIO localFileIO = resolvingFileIO.fileIO(localPath);
        FileIO hdfsFileIO = resolvingFileIO.fileIO(hdfsPath);

        assertNotNull(localFileIO);
        assertNotNull(hdfsFileIO);
        assertInstanceOf(LocalFileIO.class, localFileIO);
        assertInstanceOf(HadoopFileIO.class, hdfsFileIO);

        // Second call should return the same instances from fileIOMap
        FileIO localFileIOAgain = resolvingFileIO.fileIO(new Path("file:///path/to/local/file2"));
        FileIO hdfsFileIOAgain = resolvingFileIO.fileIO(new Path("hdfs:///path/to/local/file2"));

        assertNotNull(localFileIOAgain);
        assertNotNull(hdfsFileIOAgain);
        assertEquals(localFileIO, localFileIOAgain);
        assertEquals(hdfsFileIO, hdfsFileIOAgain);
    }
}
