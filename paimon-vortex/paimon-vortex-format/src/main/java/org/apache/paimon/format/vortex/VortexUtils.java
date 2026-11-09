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

package org.apache.paimon.format.vortex;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.HadoopOptionsProvider;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;
import org.apache.paimon.rest.RESTTokenFileIO;
import org.apache.paimon.utils.Pair;

import javax.annotation.Nullable;

import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Decides how native Vortex code reaches a file.
 *
 * <p>Local files and OSS files go through a native storage client, which reads and writes without
 * calls back into the JVM. All other files go through the Paimon {@link FileIO}, so they use the
 * file system and the credentials that the table is configured with.
 */
public class VortexUtils {

    private VortexUtils() {}

    /**
     * Returns the URI and the storage options for a native storage client, or null when the file
     * must go through the {@link FileIO}.
     */
    @Nullable
    static Pair<String, Map<String, String>> nativeLocation(
            FileIO fileIO, Path path, boolean isRead) {
        URI uri = path.toUri();
        String scheme = uri.getScheme();

        if (scheme == null || "file".equals(scheme)) {
            return Pair.of(uri.toString(), Collections.<String, String>emptyMap());
        }

        if (!"oss".equals(scheme)) {
            return null;
        }

        if (fileIO instanceof RESTTokenFileIO) {
            try {
                fileIO = ((RESTTokenFileIO) fileIO).fileIO();
            } catch (IOException e) {
                throw new RuntimeException("Can't get fileIO from RESTTokenFileIO", e);
            }
        }

        if (!(fileIO instanceof HadoopOptionsProvider)) {
            return null;
        }

        Options originOptions =
                ((HadoopOptionsProvider) fileIO).hadoopOptions(path, isRead ? "read" : "write");
        Map<String, String> storageOptions = new HashMap<>();
        storageOptions.put(
                "endpoint",
                "https://" + uri.getHost() + "." + originOptions.get("fs.oss.endpoint"));
        storageOptions.put("access_key_id", originOptions.get("fs.oss.accessKeyId"));
        storageOptions.put("secret_access_key", originOptions.get("fs.oss.accessKeySecret"));
        storageOptions.put("virtual_hosted_style_request", "true");
        if (originOptions.containsKey("fs.oss.securityToken")) {
            storageOptions.put("session_token", originOptions.get("fs.oss.securityToken"));
        }
        return Pair.of(uri.toString().replace("oss://", "s3://"), storageOptions);
    }
}
