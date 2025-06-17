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

package org.apache.paimon.iceberg;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Tests for {@link IcebergCommitCallback}. */
public class IcebergCommitCallbackTest {

    private FileStoreTable mockTable;
    private CoreOptions mockCoreOptions;
    private Options mockConfig;

    @BeforeEach
    void setUp() {
        mockTable = mock(FileStoreTable.class);
        when(mockTable.location()).thenReturn(new Path("file:/tmp/paimon/mydb.db/mytable"));

        mockCoreOptions = mock(CoreOptions.class);
        when(mockTable.coreOptions()).thenReturn(mockCoreOptions);

        mockConfig = mock(Options.class);
        when(mockCoreOptions.toConfiguration()).thenReturn(mockConfig);
    }

    @ParameterizedTest(name = "StorageType: {0}")
    @MethodSource("provideMetadataPathsWithStorageType")
    void testCatalogTableMetadataPathWithStorageType(
            IcebergOptions.StorageType storageType, String expectedPath) {
        when(mockConfig.get(IcebergOptions.METADATA_ICEBERG_STORAGE)).thenReturn(storageType);

        Path result = IcebergCommitCallback.catalogTableMetadataPath(mockTable);

        assertThat(result.toString()).isEqualTo(expectedPath);
    }

    @ParameterizedTest(name = "StorageType: {0}")
    @MethodSource("provideDatabasePathsWithStorageType")
    void testCatalogDatabasePathWithStorageType(
            IcebergOptions.StorageType storageType, String expectedPath) {
        when(mockConfig.get(IcebergOptions.METADATA_ICEBERG_STORAGE)).thenReturn(storageType);

        Path result = IcebergCommitCallback.catalogDatabasePath(mockTable);

        assertThat(result.toString()).isEqualTo(expectedPath);
    }

    @ParameterizedTest(name = "StorageType: {0}")
    @MethodSource("provideMetadataPathsWithStorageLocation")
    void testCatalogMetadataPathWithStorageLocation(
            IcebergOptions.StorageType storageType,
            IcebergOptions.StorageLocation storageLocation,
            String expectedPath) {
        when(mockConfig.get(IcebergOptions.METADATA_ICEBERG_STORAGE)).thenReturn(storageType);
        if (storageLocation != null) {
            when(mockConfig.getOptional(IcebergOptions.METADATA_ICEBERG_STORAGE_LOCATION))
                    .thenReturn(java.util.Optional.of(storageLocation));
        } else {
            when(mockConfig.getOptional(IcebergOptions.METADATA_ICEBERG_STORAGE_LOCATION))
                    .thenReturn(java.util.Optional.empty());
        }

        Path result = IcebergCommitCallback.catalogTableMetadataPath(mockTable);

        assertThat(result.toString()).isEqualTo(expectedPath);
    }

    @ParameterizedTest(name = "StorageType: {0}")
    @MethodSource("provideDatabasePathsWithStorageLocation")
    void testCatalogDatabasePathWithStorageLocation(
            IcebergOptions.StorageType storageType,
            IcebergOptions.StorageLocation storageLocation,
            String expectedPath) {
        when(mockConfig.get(IcebergOptions.METADATA_ICEBERG_STORAGE)).thenReturn(storageType);
        if (storageLocation != null) {
            when(mockConfig.getOptional(IcebergOptions.METADATA_ICEBERG_STORAGE_LOCATION))
                    .thenReturn(java.util.Optional.of(storageLocation));
        } else {
            when(mockConfig.getOptional(IcebergOptions.METADATA_ICEBERG_STORAGE_LOCATION))
                    .thenReturn(java.util.Optional.empty());
        }

        Path result = IcebergCommitCallback.catalogDatabasePath(mockTable);

        assertThat(result.toString()).isEqualTo(expectedPath);
    }

    private static Stream<Arguments> provideMetadataPathsWithStorageType() {
        return Stream.of(
                Arguments.of(
                        IcebergOptions.StorageType.TABLE_LOCATION,
                        "file:/tmp/paimon/mydb.db/mytable/metadata"),
                Arguments.of(
                        IcebergOptions.StorageType.HIVE_CATALOG,
                        "file:/tmp/paimon/iceberg/mydb/mytable/metadata"),
                Arguments.of(
                        IcebergOptions.StorageType.HADOOP_CATALOG,
                        "file:/tmp/paimon/iceberg/mydb/mytable/metadata"));
    }

    private static Stream<Arguments> provideMetadataPathsWithStorageLocation() {
        return Stream.of(
                // Explicitly set StorageLocation
                Arguments.of(
                        IcebergOptions.StorageType.TABLE_LOCATION,
                        IcebergOptions.StorageLocation.TABLE_LOCATION,
                        "file:/tmp/paimon/mydb.db/mytable/metadata"),
                Arguments.of(
                        IcebergOptions.StorageType.TABLE_LOCATION,
                        IcebergOptions.StorageLocation.CATALOG_STORAGE,
                        "file:/tmp/paimon/iceberg/mydb/mytable/metadata"),
                Arguments.of(
                        IcebergOptions.StorageType.HIVE_CATALOG,
                        IcebergOptions.StorageLocation.TABLE_LOCATION,
                        "file:/tmp/paimon/mydb.db/mytable/metadata"),
                Arguments.of(
                        IcebergOptions.StorageType.HIVE_CATALOG,
                        IcebergOptions.StorageLocation.CATALOG_STORAGE,
                        "file:/tmp/paimon/iceberg/mydb/mytable/metadata"),
                Arguments.of(
                        IcebergOptions.StorageType.HADOOP_CATALOG,
                        IcebergOptions.StorageLocation.TABLE_LOCATION,
                        "file:/tmp/paimon/mydb.db/mytable/metadata"),
                Arguments.of(
                        IcebergOptions.StorageType.HADOOP_CATALOG,
                        IcebergOptions.StorageLocation.CATALOG_STORAGE,
                        "file:/tmp/paimon/iceberg/mydb/mytable/metadata"),

                // Backward compatibility: StorageLocation is not provided.
                Arguments.of(
                        IcebergOptions.StorageType.TABLE_LOCATION,
                        null,
                        "file:/tmp/paimon/mydb.db/mytable/metadata"), // Defaults to TABLE_LOCATION
                Arguments.of(
                        IcebergOptions.StorageType.HIVE_CATALOG,
                        null,
                        "file:/tmp/paimon/iceberg/mydb/mytable/metadata"), // Defaults to
                // CATALOG_STORAGE
                Arguments.of(
                        IcebergOptions.StorageType.HADOOP_CATALOG,
                        null,
                        "file:/tmp/paimon/iceberg/mydb/mytable/metadata") // Defaults to
                // CATALOG_STORAGE
                );
    }

    private static Stream<Arguments> provideDatabasePathsWithStorageType() {
        return Stream.of(
                Arguments.of(IcebergOptions.StorageType.TABLE_LOCATION, "file:/tmp/paimon/mydb.db"),
                Arguments.of(
                        IcebergOptions.StorageType.HIVE_CATALOG, "file:/tmp/paimon/iceberg/mydb"),
                Arguments.of(
                        IcebergOptions.StorageType.HADOOP_CATALOG,
                        "file:/tmp/paimon/iceberg/mydb"));
    }

    private static Stream<Arguments> provideDatabasePathsWithStorageLocation() {
        return Stream.of(
                Arguments.of(
                        IcebergOptions.StorageType.TABLE_LOCATION,
                        IcebergOptions.StorageLocation.TABLE_LOCATION,
                        "file:/tmp/paimon/mydb.db"),
                Arguments.of(
                        IcebergOptions.StorageType.TABLE_LOCATION,
                        IcebergOptions.StorageLocation.CATALOG_STORAGE,
                        "file:/tmp/paimon/iceberg/mydb"),
                Arguments.of(
                        IcebergOptions.StorageType.HIVE_CATALOG,
                        IcebergOptions.StorageLocation.TABLE_LOCATION,
                        "file:/tmp/paimon/mydb.db"),
                Arguments.of(
                        IcebergOptions.StorageType.HIVE_CATALOG,
                        IcebergOptions.StorageLocation.CATALOG_STORAGE,
                        "file:/tmp/paimon/iceberg/mydb"),
                Arguments.of(
                        IcebergOptions.StorageType.HADOOP_CATALOG,
                        IcebergOptions.StorageLocation.TABLE_LOCATION,
                        "file:/tmp/paimon/mydb.db"),
                Arguments.of(
                        IcebergOptions.StorageType.HADOOP_CATALOG,
                        IcebergOptions.StorageLocation.CATALOG_STORAGE,
                        "file:/tmp/paimon/iceberg/mydb"),
                // Backward compatibility: StorageLocation is not provided.
                Arguments.of(
                        IcebergOptions.StorageType.TABLE_LOCATION,
                        null,
                        "file:/tmp/paimon/mydb.db"), // Defaults to TABLE_LOCATION
                Arguments.of(
                        IcebergOptions.StorageType.HIVE_CATALOG,
                        null,
                        "file:/tmp/paimon/iceberg/mydb"), // Defaults to CATALOG_STORAGE
                Arguments.of(
                        IcebergOptions.StorageType.HADOOP_CATALOG,
                        null,
                        "file:/tmp/paimon/iceberg/mydb") // Defaults to CATALOG_STORAGE
                );
    }
}
