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

package org.apache.paimon.diskann;

/** Global DiskANN configuration and utilities. */
public final class DiskAnn {

    static {
        try {
            NativeLibraryLoader.load();
        } catch (DiskAnnException e) {
            // Library loading failed silently during class init.
            // Callers should check isLibraryLoaded() or call loadLibrary() explicitly.
        }
    }

    private DiskAnn() {}

    /**
     * Ensure the native library is loaded.
     *
     * <p>This method is called automatically when any DiskANN class is used. It can be called
     * explicitly to load the library early and catch any loading errors.
     *
     * @throws DiskAnnException if the native library cannot be loaded
     */
    public static void loadLibrary() throws DiskAnnException {
        NativeLibraryLoader.load();
    }

    /**
     * Check if the native library has been loaded.
     *
     * @return true if the library is loaded
     */
    public static boolean isLibraryLoaded() {
        return NativeLibraryLoader.isLoaded();
    }
}
