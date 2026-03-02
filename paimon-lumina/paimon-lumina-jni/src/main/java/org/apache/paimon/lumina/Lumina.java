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

package org.apache.paimon.lumina;

/** Global Lumina configuration and utilities. */
public final class Lumina {

    static {
        try {
            NativeLibraryLoader.load();
        } catch (LuminaException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    /**
     * Ensure the native library is loaded.
     *
     * @throws LuminaException if the native library cannot be loaded
     */
    public static void loadLibrary() throws LuminaException {
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
