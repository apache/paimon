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
import org.apache.paimon.fs.cache.CachePolicyFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.rest.RESTTokenFileIO;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;

import static org.apache.paimon.options.CatalogOptions.RESOLVING_FILE_IO_ENABLED;
import static org.apache.paimon.rest.RESTCatalogOptions.IO_CACHE_POLICY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Tests for {@link BaseMultiPartUploadCommitter}. */
public class BaseMultiPartUploadCommitterTest {

    private static final Path TARGET = new Path("oss://bucket/table/data-0.parquet");

    private FileIO resolved;
    private ResolvingFileIO resolvingFileIO;

    @BeforeEach
    public void setUp() throws IOException {
        resolved = mock(FileIO.class);
        FileIOLoader loader = mock(FileIOLoader.class);
        when(loader.getScheme()).thenReturn("oss");
        when(loader.load(any())).thenReturn(resolved);
        resolvingFileIO = new ResolvingFileIO();
        resolvingFileIO.configure(CatalogContext.create(new Options(), loader, null));
    }

    @Test
    public void testCommitResolvesResolvingFileIO() throws IOException {
        RecordingCommitter committer = new RecordingCommitter();
        committer.commit(resolvingFileIO);
        // the subclasses cast this to their own concrete FileIO, so the resolver itself
        // reaching them would be a ClassCastException at commit time
        assertThat(committer.received).isSameAs(resolved);
    }

    @Test
    public void testDiscardStagingResolvesResolvingFileIO() throws IOException {
        RecordingCommitter committer = new RecordingCommitter();
        committer.discardStaging(resolvingFileIO);
        assertThat(committer.received).isSameAs(resolved);
    }

    @Test
    public void testConcreteFileIOIsPassedThroughUnchanged() throws IOException {
        RecordingCommitter committer = new RecordingCommitter();
        committer.commit(resolved);
        assertThat(committer.received).isSameAs(resolved);
    }

    @Test
    public void testCommitAndDiscardResolveRestPolicyAndResolverWrappers() throws IOException {
        Options options = new Options();
        options.set(IO_CACHE_POLICY, "write");
        options.set(RESOLVING_FILE_IO_ENABLED, true);
        FileIOLoader loader = mock(FileIOLoader.class);
        when(loader.load(any())).thenReturn(resolved);
        try (FileIO policy =
                CachePolicyFileIO.create(TARGET, CatalogContext.create(options, loader, null))) {
            RESTTokenFileIO rest = mock(RESTTokenFileIO.class);
            when(rest.fileIO()).thenReturn(policy);
            RecordingCommitter committer = new RecordingCommitter();
            committer.commit(rest);
            assertThat(committer.received).isSameAs(resolved);
            committer.discardStaging(rest);
            assertThat(committer.received).isSameAs(resolved);
        }
    }

    private static class RecordingCommitter extends BaseMultiPartUploadCommitter<String, String> {

        private FileIO received;

        private RecordingCommitter() {
            super(
                    "upload-id",
                    Collections.singletonList("part-1"),
                    "table/data-0.parquet",
                    1L,
                    TARGET);
        }

        @Override
        @SuppressWarnings("unchecked")
        protected MultiPartUploadStore<String, String> multiPartUploadStore(
                FileIO fileIO, Path targetPath) {
            this.received = fileIO;
            return mock(MultiPartUploadStore.class);
        }
    }
}
