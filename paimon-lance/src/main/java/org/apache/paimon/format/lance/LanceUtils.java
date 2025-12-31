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
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PluginFileIO;
import org.apache.paimon.fs.hadoop.HadoopFileIO;
import org.apache.paimon.jindo.JindoFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.oss.OSSFileIO;
import org.apache.paimon.rest.RESTTokenFileIO;
import org.apache.paimon.utils.Pair;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/** Utils for lance all. */
public class LanceUtils {

    private static final Class<?> ossFileIOKlass;
    private static final Class<?> pluginFileIOKlass;
    private static final Class<?> jindoFileIOKlass;
    private static final Class<?> hadoopFileIOKlass;

    // OSS configuration keys
    public static final String FS_OSS_ENDPOINT = "fs.oss.endpoint";
    public static final String FS_OSS_ACCESS_KEY_ID = "fs.oss.accessKeyId";
    public static final String FS_OSS_ACCESS_KEY_SECRET = "fs.oss.accessKeySecret";
    public static final String FS_OSS_SECURITY_TOKEN = "fs.oss.securityToken";
    private static final String FS_OSS_PREFIX = "fs.oss.";

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

    static {
        Class<?> klass;
        try {
            klass = Class.forName("org.apache.paimon.oss.OSSFileIO");
        } catch (ClassNotFoundException | NoClassDefFoundError e) {
            klass = null;
        }
        ossFileIOKlass = klass;

        try {
            klass = Class.forName("org.apache.paimon.jindo.JindoFileIO");
        } catch (ClassNotFoundException | NoClassDefFoundError e) {
            klass = null;
        }
        jindoFileIOKlass = klass;

        try {
            klass = Class.forName("org.apache.paimon.fs.PluginFileIO");
        } catch (ClassNotFoundException | NoClassDefFoundError e) {
            klass = null;
        }
        pluginFileIOKlass = klass;

        try {
            klass = Class.forName("org.apache.paimon.fs.hadoop.HadoopFileIO");
        } catch (ClassNotFoundException | NoClassDefFoundError e) {
            klass = null;
        }
        hadoopFileIOKlass = klass;
    }

    public static Pair<Path, Map<String, String>> toLanceSpecified(FileIO fileIO, Path path) {

        URI uri = path.toUri();
        String schema = uri.getScheme();

        if (fileIO instanceof RESTTokenFileIO) {
            try {
                fileIO = ((RESTTokenFileIO) fileIO).fileIO();
            } catch (IOException e) {
                throw new RuntimeException("Can't get fileIO from RESTTokenFileIO", e);
            }
        }

        Options originOptions;
        if (ossFileIOKlass != null && ossFileIOKlass.isInstance(fileIO)) {
            originOptions = ((OSSFileIO) fileIO).hadoopOptions();
        } else if (jindoFileIOKlass != null && jindoFileIOKlass.isInstance(fileIO)) {
            originOptions = ((JindoFileIO) fileIO).hadoopOptions();
        } else if (pluginFileIOKlass != null && pluginFileIOKlass.isInstance(fileIO)) {
            originOptions = ((PluginFileIO) fileIO).options();
        } else if (hadoopFileIOKlass != null && hadoopFileIOKlass.isInstance(fileIO)) {
            originOptions = new Options(((HadoopFileIO) fileIO).hadoopConf());
        } else {
            originOptions = new Options();
        }

        Path converted = path;
        Map<String, String> storageOptions = new HashMap<>();

        if ("traceable".equals(schema)) {
            String uriString = uri.toString();
            if (uriString.startsWith("traceable:/")) {
                converted = new Path(uriString.replace("traceable:/", "file:/"));
            }
        } else if ("oss".equals(schema)) {
            assert originOptions.containsKey(FS_OSS_ENDPOINT);
            assert originOptions.containsKey(FS_OSS_ACCESS_KEY_ID);
            assert originOptions.containsKey(FS_OSS_ACCESS_KEY_SECRET);

            for (String key : originOptions.keySet()) {
                if (key.startsWith(FS_OSS_PREFIX)) {
                    storageOptions.put(key, originOptions.get(key));
                }
            }

            storageOptions.put(
                    STORAGE_OPTION_ENDPOINT,
                    "https://" + uri.getHost() + "." + originOptions.get(FS_OSS_ENDPOINT));
            storageOptions.put(
                    STORAGE_OPTION_ACCESS_KEY_ID, originOptions.get(FS_OSS_ACCESS_KEY_ID));
            storageOptions.put(
                    STORAGE_OPTION_OSS_ACCESS_KEY_ID, originOptions.get(FS_OSS_ACCESS_KEY_ID));
            storageOptions.put(
                    STORAGE_OPTION_SECRET_ACCESS_KEY, originOptions.get(FS_OSS_ACCESS_KEY_SECRET));
            storageOptions.put(
                    STORAGE_OPTION_OSS_SECRET_ACCESS_KEY,
                    originOptions.get(FS_OSS_ACCESS_KEY_SECRET));
            storageOptions.put(STORAGE_OPTION_VIRTUAL_HOSTED_STYLE, "true");
            if (originOptions.containsKey(FS_OSS_SECURITY_TOKEN)) {
                storageOptions.put(
                        STORAGE_OPTION_SESSION_TOKEN, originOptions.get(FS_OSS_SECURITY_TOKEN));
                storageOptions.put(
                        STORAGE_OPTION_OSS_SESSION_TOKEN, originOptions.get(FS_OSS_SECURITY_TOKEN));
            }
            if (originOptions.containsKey(FS_OSS_ENDPOINT)) {
                storageOptions.put(STORAGE_OPTION_OSS_ENDPOINT, originOptions.get(FS_OSS_ENDPOINT));
            }
        }

        return Pair.of(converted, storageOptions);
    }
}
