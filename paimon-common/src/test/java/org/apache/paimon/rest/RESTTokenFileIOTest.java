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

package org.apache.paimon.rest;

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BlobDescriptor;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileIOLoader;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.RemoteIterator;
import org.apache.paimon.options.Options;
import org.apache.paimon.rest.responses.GetTableTokenResponse;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests for {@link RESTTokenFileIO}. */
class RESTTokenFileIOTest {

    @Test
    void testSetFileIOCacheMaximumSize() {
        long originalMaximumSize = RESTTokenFileIO.fileIOCacheMaximumSize();
        try {
            RESTTokenFileIO.setFileIOCacheMaximumSize(2000);
            assertThat(RESTTokenFileIO.fileIOCacheMaximumSize()).isEqualTo(2000);
            assertThatThrownBy(() -> RESTTokenFileIO.setFileIOCacheMaximumSize(0))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Maximum cache size must be positive.");
        } finally {
            RESTTokenFileIO.setFileIOCacheMaximumSize(originalMaximumSize);
        }
    }

    @Test
    void testCreateBlobPresignedUrlRequiresBoundRootAndDelegates() throws IOException {
        Path tableRoot = new Path("oss://bucket/table");
        BlobDescriptor descriptor =
                new BlobDescriptor("oss://bucket/table/bucket-0/data.blob", 0, 1);
        Duration validity = Duration.ofMinutes(5);
        FileIO delegate = mock(FileIO.class);
        when(delegate.exists(any())).thenReturn(true);
        when(delegate.createBlobPresignedUrl(tableRoot, descriptor, validity))
                .thenReturn("https://example");
        FileIOLoader loader = mock(FileIOLoader.class);
        when(loader.load(any())).thenReturn(delegate);
        when(loader.getScheme()).thenReturn("oss");
        RESTApi api = mock(RESTApi.class);
        Identifier identifier = Identifier.create("db", "table");
        when(api.loadTableToken(identifier))
                .thenReturn(new GetTableTokenResponse(Collections.emptyMap(), Long.MAX_VALUE));
        RESTTokenFileIO fileIO =
                new RESTTokenFileIO(
                        CatalogContext.create(new Options(), loader, null),
                        api,
                        identifier,
                        tableRoot);

        assertThat(fileIO.createBlobPresignedUrl(tableRoot, descriptor, validity))
                .isEqualTo("https://example");
        verify(delegate).createBlobPresignedUrl(tableRoot, descriptor, validity);

        assertThatThrownBy(
                        () ->
                                fileIO.createBlobPresignedUrl(
                                        new Path("oss://bucket/other"), descriptor, validity))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("bound table root");
    }

    @Test
    void testTryToWriteAtomicReachesInnerOverride() throws IOException {
        Path tableRoot = new Path("oss://bucket/table");
        FileIO delegate = mock(FileIO.class);
        FileIOLoader loader = mock(FileIOLoader.class);
        when(loader.load(any())).thenReturn(delegate);
        when(loader.getScheme()).thenReturn("oss");
        RESTApi api = mock(RESTApi.class);
        Identifier identifier = Identifier.create("db", "table");
        // a unique token, so the static token-keyed FileIO cache cannot serve another test's
        // delegate
        when(api.loadTableToken(identifier))
                .thenReturn(
                        new GetTableTokenResponse(
                                Collections.singletonMap("token", UUID.randomUUID().toString()),
                                Long.MAX_VALUE));
        RESTTokenFileIO fileIO =
                new RESTTokenFileIO(
                        CatalogContext.create(new Options(), loader, null),
                        api,
                        identifier,
                        tableRoot);

        Path target = new Path("oss://bucket/table/snapshot/LATEST");
        when(delegate.tryToWriteAtomic(target, "content")).thenReturn(true);

        assertThat(fileIO.tryToWriteAtomic(target, "content")).isTrue();
        verify(delegate).tryToWriteAtomic(target, "content");
        // the interface default would have written a temp file and renamed it instead
        verify(delegate, never()).rename(any(), any());
    }

    @Test
    void testListFilesIterativeReachesInnerOverride() throws IOException {
        Path tableRoot = new Path("oss://bucket/table");
        FileIO delegate = mock(FileIO.class);
        FileIOLoader loader = mock(FileIOLoader.class);
        when(loader.load(any())).thenReturn(delegate);
        when(loader.getScheme()).thenReturn("oss");
        RESTApi api = mock(RESTApi.class);
        Identifier identifier = Identifier.create("db", "table");
        // a unique token, so the static token-keyed FileIO cache cannot serve another test's
        // delegate
        when(api.loadTableToken(identifier))
                .thenReturn(
                        new GetTableTokenResponse(
                                Collections.singletonMap("token", UUID.randomUUID().toString()),
                                Long.MAX_VALUE));
        RESTTokenFileIO fileIO =
                new RESTTokenFileIO(
                        CatalogContext.create(new Options(), loader, null),
                        api,
                        identifier,
                        tableRoot);
        FileStatus status = mock(FileStatus.class);
        RemoteIterator<FileStatus> iterator =
                new RemoteIterator<FileStatus>() {
                    private boolean emitted;

                    @Override
                    public boolean hasNext() {
                        return !emitted;
                    }

                    @Override
                    public FileStatus next() {
                        emitted = true;
                        return status;
                    }
                };
        when(delegate.listFilesIterative(tableRoot, false)).thenReturn(iterator);

        RemoteIterator<FileStatus> actual = fileIO.listFilesIterative(tableRoot, false);

        assertThat(actual.hasNext()).isTrue();
        assertThat(actual.next()).isSameAs(status);
        assertThat(actual.hasNext()).isFalse();
        verify(delegate).listFilesIterative(tableRoot, false);
        // the interface default would construct its own iterator backed by listStatus
        verify(delegate, never()).listStatus(any());
    }
}
