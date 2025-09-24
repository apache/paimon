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

package org.apache.paimon.lookup.rocksdb;

import org.apache.paimon.options.Options;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.rocksdb.NativeLibraryLoader;
import org.rocksdb.RocksDB;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;

/** Test for {@link RocksDBStateFactory}. */
public class RocksDBStateFactoryTest {

    @TempDir Path tempDir;

    @Test
    void testRocksDBLibraryLoadSuccess() throws Exception {
        // Test that RocksDBStateFactory can be created successfully when RocksDB library loads
        // properly
        RocksDBStateFactory factory =
                new RocksDBStateFactory(tempDir.toString(), new Options(), null);

        // Verify that the factory is created and can be used
        assertThat(factory).isNotNull();
        assertThat(factory.db()).isNotNull();
        assertThat(factory.path()).isEqualTo(tempDir.toString());

        factory.close();
    }

    @Test
    void testEnsureRocksDBIsLoadedSuccess() throws Exception {
        // Reset the initialization flag first
        resetRocksDBInitializedFlag();

        // Test successful loading
        RocksDBStateFactory.ensureRocksDBIsLoaded();

        // Verify that the flag is set to true after successful loading
        assertThat(RocksDBStateFactory.rocksDbInitialized()).isTrue();
    }

    @Test
    void testEnsureRocksDBIsLoadedFailureAfterRetries() throws Exception {
        // Reset the initialization flag first
        resetRocksDBInitializedFlag();

        try (MockedStatic<RocksDB> mockedRocksDB = Mockito.mockStatic(RocksDB.class);
                MockedStatic<NativeLibraryLoader> mockedNativeLoader =
                        Mockito.mockStatic(NativeLibraryLoader.class)) {

            // Mock NativeLibraryLoader.getInstance()
            NativeLibraryLoader mockLoader = Mockito.mock(NativeLibraryLoader.class);
            mockedNativeLoader.when(NativeLibraryLoader::getInstance).thenReturn(mockLoader);

            // Make both NativeLibraryLoader.loadLibrary and RocksDB.loadLibrary fail
            Mockito.doThrow(new RuntimeException("Native library loading failed"))
                    .when(mockLoader)
                    .loadLibrary(anyString());
            mockedRocksDB
                    .when(RocksDB::loadLibrary)
                    .thenThrow(new RuntimeException("RocksDB library loading failed"));

            // Should throw IOException after all retry attempts fail
            assertThatThrownBy(RocksDBStateFactory::ensureRocksDBIsLoaded)
                    .isInstanceOf(IOException.class)
                    .hasMessage("Could not load the native RocksDB library");

            // Verify that the flag remains false after failure
            assertThat(RocksDBStateFactory.rocksDbInitialized()).isFalse();

            assertThatThrownBy(
                            () -> new RocksDBStateFactory(tempDir.toString(), new Options(), null))
                    .isInstanceOf(IOException.class)
                    .hasMessage("Could not load the native RocksDB library");
        }
    }

    @Test
    void testEnsureRocksDBIsLoadedAlreadyInitialized() throws Exception {
        // Set the initialization flag to true first
        setRocksDBInitializedFlag(true);

        try (MockedStatic<RocksDB> mockedRocksDB = Mockito.mockStatic(RocksDB.class)) {
            // Should not attempt to load library if already initialized
            RocksDBStateFactory.ensureRocksDBIsLoaded();

            // Verify that RocksDB.loadLibrary was never called
            mockedRocksDB.verify(Mockito.never(), () -> RocksDB.loadLibrary());
        }
    }

    @Test
    void testEnsureRocksDBIsLoadedWithDifferentExceptionTypes() throws Exception {
        // Reset the initialization flag first
        resetRocksDBInitializedFlag();

        try (MockedStatic<RocksDB> mockedRocksDB = Mockito.mockStatic(RocksDB.class);
                MockedStatic<NativeLibraryLoader> mockedNativeLoader =
                        Mockito.mockStatic(NativeLibraryLoader.class)) {

            // Mock NativeLibraryLoader.getInstance()
            NativeLibraryLoader mockLoader = Mockito.mock(NativeLibraryLoader.class);
            mockedNativeLoader.when(NativeLibraryLoader::getInstance).thenReturn(mockLoader);

            // Make NativeLibraryLoader.loadLibrary fail with UnsatisfiedLinkError
            Mockito.doThrow(new UnsatisfiedLinkError("Native library not found"))
                    .when(mockLoader)
                    .loadLibrary(anyString());

            // Make RocksDB.loadLibrary also fail
            mockedRocksDB
                    .when(RocksDB::loadLibrary)
                    .thenThrow(new UnsatisfiedLinkError("RocksDB library not found"));

            // Should throw IOException with the correct message
            assertThatThrownBy(RocksDBStateFactory::ensureRocksDBIsLoaded)
                    .isInstanceOf(IOException.class)
                    .hasMessage("Could not load the native RocksDB library")
                    .hasCauseInstanceOf(UnsatisfiedLinkError.class);
        }
    }

    @Test
    void testResetRocksDBLoadedFlag() throws Exception {
        // Test the resetRocksDBLoadedFlag method
        RocksDBStateFactory.resetRocksDBLoadedFlag();
    }

    // Helper methods for accessing private static fields
    private void setRocksDBInitializedFlag(boolean value) throws Exception {
        Field field = RocksDBStateFactory.class.getDeclaredField("rocksDbInitialized");
        field.setAccessible(true);
        field.setBoolean(null, value);
    }

    private void resetRocksDBInitializedFlag() throws Exception {
        setRocksDBInitializedFlag(false);
    }
}
