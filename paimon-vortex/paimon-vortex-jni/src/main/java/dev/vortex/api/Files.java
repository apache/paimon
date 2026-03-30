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

package dev.vortex.api;

import org.apache.paimon.shade.guava30.com.google.common.base.Preconditions;

import dev.vortex.jni.JNIFile;
import dev.vortex.jni.NativeFileMethods;

import java.net.URI;
import java.nio.file.Paths;
import java.util.Map;

/** Utility class for opening Vortex files. */
public final class Files {

    private Files() {}

    public static File open(String path) {
        return open(path, java.util.Collections.emptyMap());
    }

    public static File open(String path, Map<String, String> properties) {
        if (path.startsWith("/")) {
            return open(Paths.get(path).toUri(), properties);
        }
        return open(URI.create(path), properties);
    }

    public static File open(URI uri, Map<String, String> properties) {
        long ptr = NativeFileMethods.open(uri.toString(), properties);
        Preconditions.checkArgument(ptr > 0, "Failed to open file: %s", uri);
        return new JNIFile(ptr);
    }
}
