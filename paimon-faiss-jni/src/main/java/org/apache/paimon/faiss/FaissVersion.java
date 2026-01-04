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

package org.apache.paimon.faiss;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Version information for Paimon Faiss.
 *
 * <p>This class provides version information for both the Java binding and the underlying Faiss
 * native library.
 */
public final class FaissVersion {

    /** The version of the Java binding. */
    private static final String JAVA_VERSION;

    /** The version of the native Faiss library. */
    private static final String NATIVE_VERSION;

    static {
        Properties props = new Properties();
        try (InputStream is =
                FaissVersion.class.getResourceAsStream("/paimon-faiss-version.properties")) {
            if (is != null) {
                props.load(is);
            }
        } catch (IOException e) {
            // Ignore, use defaults
        }

        JAVA_VERSION = props.getProperty("version", "unknown");
        NATIVE_VERSION = props.getProperty("faiss.version", "unknown");
    }

    private FaissVersion() {
        // Utility class
    }

    /**
     * Get the version of the Java binding.
     *
     * @return the Java binding version
     */
    public static String getJavaVersion() {
        return JAVA_VERSION;
    }

    /**
     * Get the version of the native Faiss library.
     *
     * @return the native Faiss version
     */
    public static String getNativeVersion() {
        return NATIVE_VERSION;
    }

    /**
     * Get the platform identifier for the current system.
     *
     * @return the platform identifier
     */
    public static String getPlatform() {
        return NativeLibraryLoader.getPlatformIdentifier();
    }

    /**
     * Main method to print version information.
     *
     * @param args command line arguments (ignored)
     */
    public static void main(String[] args) {
        System.out.println("Paimon Faiss JNI Version Information");
        System.out.println("================================");
        System.out.println("Java Binding Version: " + getJavaVersion());
        System.out.println("Native Faiss Version: " + getNativeVersion());
        System.out.println("Platform: " + getPlatform());
        System.out.println("Java Version: " + System.getProperty("java.version"));
        System.out.println(
                "OS: " + System.getProperty("os.name") + " " + System.getProperty("os.version"));
        System.out.println("Architecture: " + System.getProperty("os.arch"));
    }
}
