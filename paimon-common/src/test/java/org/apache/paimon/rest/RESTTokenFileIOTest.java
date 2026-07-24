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
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;
import org.apache.paimon.rest.responses.GetTableTokenResponse;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests for {@link RESTTokenFileIO}. */
class RESTTokenFileIOTest {

    @Test
    void testCreateBlobPresignedUrlRequiresBoundRootAndDelegates() throws IOException {
        Path tableRoot = new Path("oss://bucket/table");
        BlobDescriptor descriptor =
                new BlobDescriptor("oss://bucket/table/bucket-0/data.blob", 0, 1);
        Duration validity = Duration.ofMinutes(5);
        FileIO delegate = mock(FileIO.class);
        when(delegate.exists(any())).thenReturn(true);
        when(delegate.createBlobPresignedUrl(tableRoot, descriptor, "png", validity))
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

        assertThat(fileIO.createBlobPresignedUrl(tableRoot, descriptor, "png", validity))
                .isEqualTo("https://example");
        verify(delegate).createBlobPresignedUrl(tableRoot, descriptor, "png", validity);

        assertThatThrownBy(
                        () ->
                                fileIO.createBlobPresignedUrl(
                                        new Path("oss://bucket/other"),
                                        descriptor,
                                        "png",
                                        validity))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("bound table root");
    }
}
