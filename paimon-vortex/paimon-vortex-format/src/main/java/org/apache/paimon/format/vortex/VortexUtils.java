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

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/** Utils for Vortex storage options. */
public class VortexUtils {

    public static Pair<Path, Map<String, String>> toVortexSpecifiedForReader(
            FileIO fileIO, Path path) {
        return toVortexSpecified(fileIO, path, true);
    }

    public static Pair<Path, Map<String, String>> toVortexSpecifiedForWriter(
            FileIO fileIO, Path path) {
        return toVortexSpecified(fileIO, path, false);
    }

    private static Pair<Path, Map<String, String>> toVortexSpecified(
            FileIO fileIO, Path path, boolean isRead) {
        URI uri = path.toUri();
        String schema = uri.getScheme();

        Options originOptions = hadoopOptions(fileIO, path, isRead);

        Path converted = path;
        Map<String, String> storageOptions = new HashMap<>();
        if ("oss".equals(schema)) {
            storageOptions.put(
                    "endpoint",
                    "https://" + uri.getHost() + "." + originOptions.get("fs.oss.endpoint"));
            storageOptions.put("access_key_id", originOptions.get("fs.oss.accessKeyId"));
            storageOptions.put("secret_access_key", originOptions.get("fs.oss.accessKeySecret"));
            storageOptions.put("virtual_hosted_style_request", "true");
            if (originOptions.containsKey("fs.oss.securityToken")) {
                storageOptions.put("session_token", originOptions.get("fs.oss.securityToken"));
            }
            converted = new Path(uri.toString().replace("oss://", "s3://"));
        }

        return Pair.of(converted, storageOptions);
    }

    private static Options hadoopOptions(FileIO fileIO, Path path, boolean isRead) {
        String opType = isRead ? "read" : "write";
        if (fileIO instanceof RESTTokenFileIO) {
            try (RESTTokenFileIO.Lease lease = ((RESTTokenFileIO) fileIO).acquire()) {
                return hadoopOptions(lease.fileIO(), path, opType);
            } catch (IOException e) {
                throw new RuntimeException("Can't get fileIO from RESTTokenFileIO", e);
            }
        }
        return hadoopOptions(fileIO, path, opType);
    }

    private static Options hadoopOptions(FileIO fileIO, Path path, String opType) {
        return fileIO instanceof HadoopOptionsProvider
                ? ((HadoopOptionsProvider) fileIO).hadoopOptions(path, opType)
                : new Options();
    }
}
