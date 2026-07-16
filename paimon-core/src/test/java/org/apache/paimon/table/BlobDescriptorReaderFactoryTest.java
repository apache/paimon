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

package org.apache.paimon.table;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogLoader;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.IsolatedDirectoryFileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.options.Options;
import org.apache.paimon.utils.InstantiationUtil;
import org.apache.paimon.utils.UriReaderFactory;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests for {@link BlobDescriptorReaderFactory}. */
public class BlobDescriptorReaderFactoryTest {

    @TempDir java.nio.file.Path tempPath;

    @Test
    public void testUseCatalogContextByDefault() throws Exception {
        java.nio.file.Path tableDirectory = Files.createDirectory(tempPath.resolve("table"));
        java.nio.file.Path blobFile = tableDirectory.resolve("blob");
        Files.write(blobFile, new byte[] {1, 2});

        Options catalogOptions = new Options();
        catalogOptions.set(
                IsolatedDirectoryFileIO.ROOT_DIR, "isolated://" + tableDirectory.toString());
        CatalogEnvironment catalogEnvironment = mock(CatalogEnvironment.class);
        when(catalogEnvironment.catalogContext()).thenReturn(CatalogContext.create(catalogOptions));
        FileStoreTable table = mock(FileStoreTable.class);
        when(table.catalogEnvironment()).thenReturn(catalogEnvironment);
        when(table.coreOptions()).thenReturn(CoreOptions.fromMap(Collections.emptyMap()));

        UriReaderFactory readerFactory = BlobDescriptorReaderFactory.create(table);
        String blobUri = "isolated://" + blobFile;
        try (SeekableInputStream inputStream =
                readerFactory.create(blobUri).newInputStream(blobUri)) {
            assertThat(inputStream.read()).isEqualTo(1);
            assertThat(inputStream.read()).isEqualTo(2);
        }
        verify(table, never()).fileIO();
    }

    @Test
    public void testUseConfiguredSourceTableFileIO() throws Exception {
        java.nio.file.Path sourceDirectory = Files.createDirectory(tempPath.resolve("source"));
        java.nio.file.Path blobFile = sourceDirectory.resolve("blob");
        Files.write(blobFile, new byte[] {1, 2});

        FileIO sourceFileIO = spy(isolatedFileIO("isolated://" + sourceDirectory));
        FileStoreTable sourceTable = mock(FileStoreTable.class);
        when(sourceTable.fileIO()).thenReturn(sourceFileIO);

        Identifier sourceIdentifier = Identifier.fromString("db.source$branch_rt");
        Catalog catalog = mock(Catalog.class);
        when(catalog.getTable(sourceIdentifier)).thenReturn(sourceTable);
        CatalogLoader catalogLoader = mock(CatalogLoader.class);
        when(catalogLoader.load()).thenReturn(catalog);
        CatalogEnvironment catalogEnvironment = mock(CatalogEnvironment.class);
        when(catalogEnvironment.catalogLoader()).thenReturn(catalogLoader);

        FileStoreTable targetTable = mock(FileStoreTable.class);
        when(targetTable.catalogEnvironment()).thenReturn(catalogEnvironment);
        when(targetTable.coreOptions())
                .thenReturn(
                        CoreOptions.fromMap(
                                Collections.singletonMap(
                                        "blob-descriptor.source-table", "db.source$branch_rt")));

        String blobUri = "isolated://" + blobFile;
        UriReaderFactory contextOnlyFactory =
                new UriReaderFactory(CatalogContext.create(new Options()));
        assertThatThrownBy(() -> contextOnlyFactory.create(blobUri).newInputStream(blobUri))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining(IsolatedDirectoryFileIO.ROOT_DIR);

        UriReaderFactory readerFactory =
                InstantiationUtil.clone(BlobDescriptorReaderFactory.create(targetTable));
        try (SeekableInputStream inputStream =
                readerFactory.create(blobUri).newInputStream(blobUri)) {
            assertThat(inputStream.read()).isEqualTo(1);
            assertThat(inputStream.read()).isEqualTo(2);
        }

        verify(catalogLoader).load();
        verify(catalog).getTable(sourceIdentifier);
        verify(sourceFileIO).isObjectStore();
    }

    @Test
    public void testRejectSourceTableWithoutCatalogLoader() {
        CatalogEnvironment catalogEnvironment = mock(CatalogEnvironment.class);
        FileStoreTable targetTable = mock(FileStoreTable.class);
        when(targetTable.catalogEnvironment()).thenReturn(catalogEnvironment);
        when(targetTable.coreOptions())
                .thenReturn(
                        CoreOptions.fromMap(
                                Collections.singletonMap(
                                        "blob-descriptor.source-table", "db.source")));

        assertThatThrownBy(() -> BlobDescriptorReaderFactory.create(targetTable))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("not supported for tables without a catalog loader")
                .hasMessageContaining("external tables in REST catalogs");
    }

    @Test
    public void testUseConfiguredExternalStorageFileIO() throws Exception {
        java.nio.file.Path externalDirectory = Files.createDirectory(tempPath.resolve("external"));
        java.nio.file.Path targetDirectory = Files.createDirectory(tempPath.resolve("target"));
        java.nio.file.Path blobFile = externalDirectory.resolve("blob");
        Files.write(blobFile, new byte[] {1, 2});

        CatalogEnvironment catalogEnvironment = mock(CatalogEnvironment.class);
        when(catalogEnvironment.catalogContext()).thenReturn(CatalogContext.create(new Options()));
        FileStoreTable targetTable = mock(FileStoreTable.class);
        when(targetTable.fileIO()).thenReturn(isolatedFileIO(targetDirectory));
        when(targetTable.catalogEnvironment()).thenReturn(catalogEnvironment);
        when(targetTable.coreOptions())
                .thenReturn(
                        CoreOptions.fromMap(
                                Collections.singletonMap(
                                        "blob-descriptor.root-dir",
                                        "isolated://" + externalDirectory)));

        UriReaderFactory readerFactory = BlobDescriptorReaderFactory.create(targetTable);
        String blobUri = "isolated://" + blobFile;
        try (SeekableInputStream inputStream =
                readerFactory.create(blobUri).newInputStream(blobUri)) {
            assertThat(inputStream.read()).isEqualTo(1);
            assertThat(inputStream.read()).isEqualTo(2);
        }
    }

    private static FileIO isolatedFileIO(java.nio.file.Path root) {
        return isolatedFileIO(new Path(root.toUri()).toString());
    }

    private static FileIO isolatedFileIO(String root) {
        Options options = new Options();
        options.set(IsolatedDirectoryFileIO.ROOT_DIR, root);
        IsolatedDirectoryFileIO fileIO = new IsolatedDirectoryFileIO();
        fileIO.configure(CatalogContext.create(options));
        return fileIO;
    }
}
