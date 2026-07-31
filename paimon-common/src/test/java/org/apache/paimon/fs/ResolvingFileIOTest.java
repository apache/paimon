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

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.data.BlobDescriptor;
import org.apache.paimon.fs.hadoop.HadoopFileIO;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests for {@link ResolvingFileIO}. */
public class ResolvingFileIOTest {

    private ResolvingFileIO resolvingFileIO;

    @BeforeEach
    public void setUp() {
        resolvingFileIO = new ResolvingFileIO();
        Options options = new Options();
        CatalogContext catalogContext = CatalogContext.create(options);
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

    @Test
    public void testCloseReleasesEveryResolvedFileIO() throws Exception {
        List<FileIO> loaded = new ArrayList<>();
        configureWithFreshDelegates(loaded, false);

        // two authorities mean two entries in the delegate map
        FileIO first = resolvingFileIO.fileIO(new Path("oss://bucket-1/table"));
        FileIO second = resolvingFileIO.fileIO(new Path("oss://bucket-2/table"));
        assertNotEquals(first, second);

        resolvingFileIO.close();

        // both resolved delegates, and the throwaway probes FileIO.get made along the way
        for (FileIO fileIO : loaded) {
            verify(fileIO, times(1)).close();
        }
    }

    @Test
    public void testCloseKeepsGoingWhenADelegateFails() throws Exception {
        List<FileIO> loaded = new ArrayList<>();
        configureWithFreshDelegates(loaded, true);

        FileIO first = resolvingFileIO.fileIO(new Path("oss://bucket-1/table"));
        FileIO second = resolvingFileIO.fileIO(new Path("oss://bucket-2/table"));

        // the first failure must not keep the second entry from being closed
        assertThrows(IOException.class, () -> resolvingFileIO.close());

        verify(first, times(1)).close();
        verify(second, times(1)).close();
    }

    @Test
    public void testUseAfterCloseIsRejectedAndTheMapIsEmptied() throws Exception {
        List<FileIO> loaded = new ArrayList<>();
        configureWithFreshDelegates(loaded, false);

        FileIO delegate = resolvingFileIO.fileIO(new Path("oss://bucket-1/table"));
        resolvingFileIO.close();

        // the map really is drained, so a second close must not close the delegate again
        resolvingFileIO.close();
        verify(delegate, times(1)).close();

        // and resolving again must not silently rebuild a delegate nobody will release
        assertThrows(
                IOException.class, () -> resolvingFileIO.fileIO(new Path("oss://bucket-1/table")));
    }

    /**
     * Hands out a fresh delegate per load, the way a real loader does. FileIO.get loads one
     * throwaway instance to probe access and another one to return, so a single shared mock cannot
     * tell the two apart.
     */
    private void configureWithFreshDelegates(List<FileIO> loaded, boolean failOnClose)
            throws IOException {
        FileIOLoader loader = mock(FileIOLoader.class);
        when(loader.getScheme()).thenReturn("oss");
        when(loader.load(any()))
                .thenAnswer(
                        ignored -> {
                            FileIO delegate = mock(FileIO.class);
                            when(delegate.exists(any())).thenReturn(true);
                            if (failOnClose) {
                                doThrow(new IOException("cannot close")).when(delegate).close();
                            }
                            loaded.add(delegate);
                            return delegate;
                        });
        resolvingFileIO.configure(CatalogContext.create(new Options(), loader, null));
    }

    @Test
    public void testCreateBlobPresignedUrlResolvesDescriptorFileIO() throws IOException {
        FileIO delegate = mock(FileIO.class);
        when(delegate.exists(any())).thenReturn(true);
        FileIOLoader loader = mock(FileIOLoader.class);
        when(loader.load(any())).thenReturn(delegate);
        when(loader.getScheme()).thenReturn("oss");
        resolvingFileIO.configure(CatalogContext.create(new Options(), loader, null));

        Path tableRoot = new Path("oss://bucket/table");
        BlobDescriptor descriptor =
                new BlobDescriptor("oss://bucket/table/bucket-0/data.blob", 0, 1);
        Duration validity = Duration.ofMinutes(5);
        when(delegate.createBlobPresignedUrl(tableRoot, descriptor, validity))
                .thenReturn("https://example");

        assertEquals(
                "https://example",
                resolvingFileIO.createBlobPresignedUrl(tableRoot, descriptor, validity));
        verify(delegate).createBlobPresignedUrl(tableRoot, descriptor, validity);
    }

    @Test
    public void testTryToWriteAtomicReachesResolvedOverride() throws IOException {
        FileIO delegate = mock(FileIO.class);
        FileIOLoader loader = mock(FileIOLoader.class);
        when(loader.load(any())).thenReturn(delegate);
        when(loader.getScheme()).thenReturn("oss");
        resolvingFileIO.configure(CatalogContext.create(new Options(), loader, null));

        Path target = new Path("oss://bucket/table/snapshot/LATEST");
        when(delegate.tryToWriteAtomic(target, "content")).thenReturn(true);

        assertTrue(resolvingFileIO.tryToWriteAtomic(target, "content"));
        verify(delegate).tryToWriteAtomic(target, "content");
        // the interface default would have written a temp file and renamed it instead
        verify(delegate, never()).rename(any(), any());
    }
}
