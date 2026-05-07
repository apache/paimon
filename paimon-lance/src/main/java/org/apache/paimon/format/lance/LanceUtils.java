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

package org.apache.paimon.format.lance;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.HadoopOptionsProvider;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;
import org.apache.paimon.rest.RESTTokenFileIO;
import org.apache.paimon.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/** Utils for lance all. */
public class LanceUtils {

    private static final Logger LOG = LoggerFactory.getLogger(LanceUtils.class);

    // OSS configuration keys
    public static final String FS_OSS_ENDPOINT = "fs.oss.endpoint";
    public static final String FS_OSS_ACCESS_KEY_ID = "fs.oss.accessKeyId";
    public static final String FS_OSS_ACCESS_KEY_SECRET = "fs.oss.accessKeySecret";
    public static final String FS_OSS_SECURITY_TOKEN = "fs.oss.securityToken";
    private static final String FS_PREFIX = "fs.";

    // Storage options keys for Lance
    public static final String STORAGE_OPTION_ENDPOINT = "endpoint";
    public static final String STORAGE_OPTION_ACCESS_KEY_ID = "access_key_id";
    public static final String STORAGE_OPTION_SECRET_ACCESS_KEY = "secret_access_key";
    public static final String STORAGE_OPTION_SESSION_TOKEN = "session_token";
    public static final String STORAGE_OPTION_VIRTUAL_HOSTED_STYLE = "virtual_hosted_style_request";
    public static final String STORAGE_OPTION_OSS_ACCESS_KEY_ID = "oss_access_key_id";
    public static final String STORAGE_OPTION_OSS_SECRET_ACCESS_KEY = "oss_secret_access_key";
    public static final String STORAGE_OPTION_OSS_SESSION_TOKEN = "oss_session_token";
    public static final String STORAGE_OPTION_OSS_ENDPOINT = "oss_endpoint";

    public static Pair<Path, Map<String, String>> toLanceSpecifiedForReader(
            FileIO fileIO, Path path) {
        return toLanceSpecified(fileIO, path, true);
    }

    public static Pair<Path, Map<String, String>> toLanceSpecifiedForWriter(
            FileIO fileIO, Path path) {
        return toLanceSpecified(fileIO, path, false);
    }

    private static Pair<Path, Map<String, String>> toLanceSpecified(
            FileIO fileIO, Path path, boolean isRead) {

        URI uri = path.toUri();
        String schema = uri.getScheme();
        Map<String, String> tokenOptions = null;

        if (fileIO instanceof RESTTokenFileIO) {
            RESTTokenFileIO restTokenFileIO = (RESTTokenFileIO) fileIO;
            try {
                fileIO = restTokenFileIO.fileIO();
            } catch (IOException e) {
                throw new RuntimeException("Can't get fileIO from RESTTokenFileIO", e);
            }
            tokenOptions = restTokenFileIO.validToken().token();
        }

        Options originOptions;
        if (fileIO instanceof HadoopOptionsProvider) {
            originOptions =
                    ((HadoopOptionsProvider) fileIO).hadoopOptions(path, isRead ? "read" : "write");
        } else {
            originOptions = new Options();
        }
        originOptions = new Options(originOptions.toMap());
        if (tokenOptions != null) {
            for (Map.Entry<String, String> entry : tokenOptions.entrySet()) {
                if (entry.getKey().startsWith(FS_PREFIX) && entry.getValue() != null) {
                    originOptions.set(entry.getKey(), entry.getValue());
                }
            }
        }

        Path converted = path;
        Map<String, String> storageOptions = new HashMap<>();

        if ("traceable".equals(schema)) {
            String uriString = uri.toString();
            if (uriString.startsWith("traceable:/")) {
                converted = new Path(uriString.replace("traceable:/", "file:/"));
            }
        } else if ("oss".equals(schema)) {
            logMissingOssOption(originOptions, FS_OSS_ENDPOINT, path);
            logMissingOssOption(originOptions, FS_OSS_ACCESS_KEY_ID, path);
            logMissingOssOption(originOptions, FS_OSS_ACCESS_KEY_SECRET, path);

            for (String key : originOptions.keySet()) {
                if (key.startsWith(FS_PREFIX)) {
                    storageOptions.put(key, originOptions.get(key));
                }
            }

            if (originOptions.get(FS_OSS_ENDPOINT) != null) {
                storageOptions.put(
                        STORAGE_OPTION_ENDPOINT,
                        "https://" + uri.getHost() + "." + originOptions.get(FS_OSS_ENDPOINT));
                storageOptions.put(STORAGE_OPTION_OSS_ENDPOINT, originOptions.get(FS_OSS_ENDPOINT));
            }
            if (originOptions.get(FS_OSS_ACCESS_KEY_ID) != null) {
                storageOptions.put(
                        STORAGE_OPTION_ACCESS_KEY_ID, originOptions.get(FS_OSS_ACCESS_KEY_ID));
                storageOptions.put(
                        STORAGE_OPTION_OSS_ACCESS_KEY_ID, originOptions.get(FS_OSS_ACCESS_KEY_ID));
            }
            if (originOptions.get(FS_OSS_ACCESS_KEY_SECRET) != null) {
                storageOptions.put(
                        STORAGE_OPTION_SECRET_ACCESS_KEY,
                        originOptions.get(FS_OSS_ACCESS_KEY_SECRET));
                storageOptions.put(
                        STORAGE_OPTION_OSS_SECRET_ACCESS_KEY,
                        originOptions.get(FS_OSS_ACCESS_KEY_SECRET));
            }
            storageOptions.put(STORAGE_OPTION_VIRTUAL_HOSTED_STYLE, "true");
            if (originOptions.get(FS_OSS_SECURITY_TOKEN) != null) {
                storageOptions.put(
                        STORAGE_OPTION_SESSION_TOKEN, originOptions.get(FS_OSS_SECURITY_TOKEN));
                storageOptions.put(
                        STORAGE_OPTION_OSS_SESSION_TOKEN, originOptions.get(FS_OSS_SECURITY_TOKEN));
            }
        }

        Iterator<Map.Entry<String, String>> iterator = storageOptions.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, String> entry = iterator.next();
            if (entry.getValue() == null) {
                LOG.warn("Removing null Lance storage option value for key '{}'.", entry.getKey());
                iterator.remove();
            }
        }
        return Pair.of(converted, storageOptions);
    }

    private static void logMissingOssOption(Options originOptions, String key, Path path) {
        if (!originOptions.containsKey(key) || originOptions.get(key) == null) {
            LOG.warn(
                    "Lance OSS storage option '{}' is missing for path '{}'. "
                            + "Lance native may fall back to environment variables or report "
                            + "a missing option error.",
                    key,
                    path);
        }
    }
}
