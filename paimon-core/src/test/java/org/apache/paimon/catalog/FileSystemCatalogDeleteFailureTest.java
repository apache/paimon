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

package org.apache.paimon.catalog;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

/** Tests that filesystem catalog drops do not report success after failed deletions. */
class FileSystemCatalogDeleteFailureTest {

    private static final Identifier IDENTIFIER = Identifier.create("db", "t");

    @TempDir java.nio.file.Path tempDir;

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testFailedTableDirectoryDeletion(boolean ignoreIfNotExists) throws Exception {
        FileIO fileIO = spy(LocalFileIO.create());
        Path external = new Path(tempDir.resolve("external").toUri());
        fileIO.mkdirs(external);
        FileSystemCatalog catalog = createCatalog(fileIO, external);
        Path tablePath = catalog.getTableLocation(IDENTIFIER);
        doReturn(false).when(fileIO).delete(tablePath, true);

        assertThatThrownBy(() -> catalog.dropTable(IDENTIFIER, ignoreIfNotExists))
                .isInstanceOf(RuntimeException.class)
                .hasStackTraceContaining(tablePath.toString());
        assertThat(fileIO.exists(tablePath)).isTrue();
        assertThat(fileIO.exists(external)).isTrue();
        assertThat(catalog.getTable(IDENTIFIER)).isNotNull();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testFailedExternalDirectoryDeletion(boolean ignoreIfNotExists) throws Exception {
        FileIO fileIO = spy(LocalFileIO.create());
        Path external = new Path(tempDir.resolve("external").toUri());
        fileIO.mkdirs(external);
        FileSystemCatalog catalog = createCatalog(fileIO, external);
        doReturn(false).when(fileIO).delete(external, true);

        assertThatThrownBy(() -> catalog.dropTable(IDENTIFIER, ignoreIfNotExists))
                .isInstanceOf(RuntimeException.class)
                .hasStackTraceContaining(external.toString());
        assertThat(fileIO.exists(external)).isTrue();
    }

    @Test
    void testMissingExternalDirectoryDoesNotFailDrop() throws Exception {
        FileIO fileIO = LocalFileIO.create();
        Path external = new Path(tempDir.resolve("absent-external").toUri());
        FileSystemCatalog catalog = createCatalog(fileIO, external);
        catalog.dropTable(IDENTIFIER, false);
        assertThat(fileIO.exists(catalog.getTableLocation(IDENTIFIER))).isFalse();
        catalog.dropTable(IDENTIFIER, true);
    }

    @Test
    void testAlreadyRemovedDirectoryDoesNotFailDrop() throws Exception {
        LocalFileIO delegate = LocalFileIO.create();
        FileIO fileIO = spy(delegate);
        FileSystemCatalog catalog = createCatalog(fileIO, null);
        Path tablePath = catalog.getTableLocation(IDENTIFIER);
        doAnswer(
                        invocation -> {
                            delegate.delete(tablePath, true);
                            return false;
                        })
                .when(fileIO)
                .delete(tablePath, true);

        catalog.dropTable(IDENTIFIER, false);
        assertThat(fileIO.exists(tablePath)).isFalse();
    }

    @Test
    void testRealReadOnlySchemaDirectory() throws Exception {
        assumeTrue(Files.getFileStore(tempDir).supportsFileAttributeView("posix"));
        FileIO fileIO = LocalFileIO.create();
        FileSystemCatalog catalog = createCatalog(fileIO, null);
        Path tablePath = catalog.getTableLocation(IDENTIFIER);
        java.nio.file.Path schemaDirectory = Paths.get(new Path(tablePath, "schema").toUri());
        Set<PosixFilePermission> originalPermissions =
                Files.getPosixFilePermissions(schemaDirectory);
        try {
            Files.setPosixFilePermissions(
                    schemaDirectory, PosixFilePermissions.fromString("r-xr-xr-x"));
            // Root can still remove these files. Keep portable failure tests above for that case.
            assumeFalse(Files.isWritable(schemaDirectory));
            assertThat(catalog.getTable(IDENTIFIER)).isNotNull();
            Throwable failure = catchThrowable(() -> catalog.dropTable(IDENTIFIER, false));
            assertThat(catalog.getTable(IDENTIFIER)).isNotNull();
            assertThat(failure)
                    .isInstanceOf(RuntimeException.class)
                    .hasStackTraceContaining(tablePath.toString());
        } finally {
            Files.setPosixFilePermissions(schemaDirectory, originalPermissions);
        }
    }

    private FileSystemCatalog createCatalog(FileIO fileIO, Path external) throws Exception {
        FileSystemCatalog catalog =
                new FileSystemCatalog(fileIO, new Path(tempDir.resolve("warehouse").toUri()));
        catalog.createDatabase(IDENTIFIER.getDatabaseName(), false);
        Schema.Builder schema =
                Schema.newBuilder().column("id", DataTypes.INT()).option("bucket", "-1");
        if (external != null) {
            schema.option(CoreOptions.DATA_FILE_EXTERNAL_PATHS.key(), external.toString());
        }
        catalog.createTable(IDENTIFIER, schema.build(), false);
        return catalog;
    }
}
