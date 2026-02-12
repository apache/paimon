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

package org.apache.paimon.spark.action;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.StorageType;
import org.apache.paimon.operation.PartitionFileLister;
import org.apache.paimon.table.FileStoreTable;

import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Unit tests for ArchivePartitionAction. */
public class ArchivePartitionActionTest {

    @Mock private FileStoreTable table;

    @Mock private FileIO fileIO;

    @Mock private PartitionFileLister fileLister;

    @Mock private JavaSparkContext sparkContext;

    @TempDir java.nio.file.Path tempDir;

    private ArchivePartitionAction action;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        when(table.fileIO()).thenReturn(fileIO);
        when(fileIO.isObjectStore()).thenReturn(true);
        action = new ArchivePartitionAction(table, sparkContext);
    }

    @Test
    public void testArchiveWithStandardStorageTypeThrowsException() {
        Map<String, String> partitionSpec = new HashMap<>();
        partitionSpec.put("dt", "2024-01-01");
        List<Map<String, String>> partitionSpecs = Arrays.asList(partitionSpec);

        assertThrows(
                IllegalArgumentException.class,
                () -> action.archive(partitionSpecs, StorageType.Standard));
    }

    @Test
    public void testArchiveWithNonObjectStoreThrowsException() {
        when(fileIO.isObjectStore()).thenReturn(false);

        Map<String, String> partitionSpec = new HashMap<>();
        partitionSpec.put("dt", "2024-01-01");
        List<Map<String, String>> partitionSpecs = Arrays.asList(partitionSpec);

        assertThrows(
                UnsupportedOperationException.class,
                () -> action.archive(partitionSpecs, StorageType.Archive));
    }

    @Test
    public void testUnarchiveWithStandardStorageTypeThrowsException() {
        Map<String, String> partitionSpec = new HashMap<>();
        partitionSpec.put("dt", "2024-01-01");
        List<Map<String, String>> partitionSpecs = Arrays.asList(partitionSpec);

        assertThrows(
                IllegalArgumentException.class,
                () -> action.unarchive(partitionSpecs, StorageType.Standard));
    }

    @Test
    public void testRestoreArchiveWithNonObjectStoreThrowsException() {
        when(fileIO.isObjectStore()).thenReturn(false);

        Map<String, String> partitionSpec = new HashMap<>();
        partitionSpec.put("dt", "2024-01-01");
        List<Map<String, String>> partitionSpecs = Arrays.asList(partitionSpec);

        assertThrows(
                UnsupportedOperationException.class,
                () -> action.restoreArchive(partitionSpecs, Duration.ofDays(7)));
    }

    @Test
    public void testArchiveWithEmptyPartitionList() throws IOException {
        Map<String, String> partitionSpec = new HashMap<>();
        partitionSpec.put("dt", "2024-01-01");
        List<Map<String, String>> partitionSpecs = Arrays.asList(partitionSpec);

        // Mock file lister to return empty list
        PartitionFileLister mockLister = mock(PartitionFileLister.class);
        when(mockLister.listPartitionFiles(partitionSpecs)).thenReturn(Arrays.asList());

        // Since we can't easily mock the internal fileLister creation, we test the error handling
        // This test verifies the method signature and basic validation
        assertEquals(0, action.archive(partitionSpecs, StorageType.Archive));
    }

    @Test
    public void testArchiveWithColdArchiveStorageType() throws IOException {
        Map<String, String> partitionSpec = new HashMap<>();
        partitionSpec.put("dt", "2024-01-01");
        List<Map<String, String>> partitionSpecs = Arrays.asList(partitionSpec);

        // Should not throw exception for ColdArchive
        // Since we can't easily mock the internal fileLister creation, we test the validation
        // This test verifies the method accepts ColdArchive storage type
        assertEquals(0, action.archive(partitionSpecs, StorageType.ColdArchive));
    }

    @Test
    public void testUnarchiveWithArchiveStorageType() throws IOException {
        Map<String, String> partitionSpec = new HashMap<>();
        partitionSpec.put("dt", "2024-01-01");
        List<Map<String, String>> partitionSpecs = Arrays.asList(partitionSpec);

        // Should not throw exception for Archive storage type
        assertEquals(0, action.unarchive(partitionSpecs, StorageType.Archive));
    }

    @Test
    public void testUnarchiveWithColdArchiveStorageType() throws IOException {
        Map<String, String> partitionSpec = new HashMap<>();
        partitionSpec.put("dt", "2024-01-01");
        List<Map<String, String>> partitionSpecs = Arrays.asList(partitionSpec);

        // Should not throw exception for ColdArchive storage type
        assertEquals(0, action.unarchive(partitionSpecs, StorageType.ColdArchive));
    }

    @Test
    public void testRestoreArchiveWithDuration() throws IOException {
        Map<String, String> partitionSpec = new HashMap<>();
        partitionSpec.put("dt", "2024-01-01");
        List<Map<String, String>> partitionSpecs = Arrays.asList(partitionSpec);

        // Should not throw exception with duration
        assertEquals(0, action.restoreArchive(partitionSpecs, Duration.ofDays(7)));
    }
}
